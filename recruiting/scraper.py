#!/usr/bin/env python3
"""
PM-intern openings poller  --  runs on a Render cron (or locally).

Tiered fetch, per the agreed architecture:
  Tier 1  Standard ATS JSON  (Greenhouse / Lever / Ashby)   <- covers most companies
  Tier 2  Big-company custom career APIs  (Google/Amazon/…)  <- add per-company as needed
  Tier 3  Aggregator trackers (GitHub raw README)            <- catches the long tail
          incl. abbybravo/PM-Recruiting-Hub, a PM/APM/TPM/Product-Ops-specific tracker
The same multi-keyword filter runs across every source.

Output:
  - public/openings.json   current open PM-intern roles (the hosted site reads this)
  - state.json             last run's role keys, for diffing "what's new"
  - ntfy push              a phone+laptop notification for NEW roles (set NTFY_TOPIC)

Stdlib only -> deploys on Render with no build step. Python 3.9+.
"""
import json, os, re, sys, time, urllib.request, urllib.parse, urllib.error, pathlib

# ---------------- filter ----------------
# TRACK selects which role family to keep. Set TRACK=swe for the SWE F2026 feed,
# TRACK=pm (default) for the PM/APM feed. Everything else in the pipeline is shared.
TRACK = os.environ.get("TRACK", "pm").lower()

_FILTERS = {
    "pm": {
        # Design choice: include EVERYTHING product-family — better to see a role and skip it than
        # miss one. Widest PM net; dedupe (in build_site) collapses identical rows so "include
        # everything" doesn't mean "see the same role twice."
        "include": ["product manag", "associate product manager", "apm",
                    "product intern", "pm intern", "product owner", "product strategy",
                    "product operations", "product analyst", "product development",
                    "product experience", "product design"],
        "exclude": ["senior", "staff", "principal", "director", "full-time",
                    "full time", "new grad", "newgrad", "mba", "phd", "returnship"],
    },
    "swe": {
        # broad SWE-intern net; the word-boundary "intern" gate prevents overreach
        "include": ["software engineer", "software engineering", "software developer",
                    "swe intern", "sde intern", "backend", "front end", "frontend",
                    "front-end", "full stack", "full-stack", "web developer",
                    "programmer", "developer intern", "engineering intern"],
        # drop hardware/mech/civil "engineer" interns, senior/full-time, non-code roles
        "exclude": ["senior", "staff", "principal", "manager", "director", "lead",
                    "full-time", "full time", "new grad", "newgrad", "mba", "phd",
                    "returnship", "mechanical", "electrical", "civil", "chemical",
                    "hardware", "firmware", "manufacturing", "industrial", "sales",
                    "field engineer", "systems engineer", "network engineer",
                    "quality engineer", "site reliability", "solutions engineer"],
    },
}
_F = _FILTERS.get(TRACK, _FILTERS["pm"])
INCLUDE, EXCLUDE = _F["include"], _F["exclude"]
_INTERN = re.compile(r"\bintern(ship)?s?\b")   # the WORD -- not "internal" / "international"

def is_pm_intern(title: str) -> bool:
    """Name kept for compatibility; matches the ACTIVE TRACK (pm or swe)."""
    t = (title or "").lower()
    if not _INTERN.search(t):                   # must actually be an internship
        return False
    if not any(k in t for k in INCLUDE):        # must match the track's role family
        return False
    if any(k in t for k in EXCLUDE):            # drop senior / full-time / wrong-discipline
        return False
    return True


# ---------------- posting dates ----------------
# Each source reports when a listing went up in its own format. Normalize to a unix
# timestamp ("postedAt") so the HQ can order listings by when they were released,
# instead of by when this scraper happened to first see them (firstSeen).
import datetime as _dt
def _iso_ts(v):
    try:
        return int(_dt.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()) if v else None
    except Exception:
        return None
def _ms_ts(v):
    try:
        return int(int(v) / 1000) if v else None
    except Exception:
        return None
def _age_ts(text, now=None):
    """'0d' / '5d' / '2mo' / 'Posted Today' / 'Posted Yesterday' / 'Posted 3 Days Ago' / 'Posted 30+ Days Ago' / 'Sep 29'."""
    import time as _t
    now = now or int(_t.time())
    t = re.sub(r"<[^>]+>", "", str(text or "")).strip().lower()
    if not t:
        return None
    if "today" in t or t in ("0d", "new"):
        return now
    if "yesterday" in t:
        return now - 86400
    m = re.search(r"(\d+)\+?\s*(d|day|days|mo|month|months|w|wk|week|weeks|h|hr|hour|hours)\b", t)
    if m:
        n, u = int(m.group(1)), m.group(2)
        mult = 3600 if u.startswith("h") else 86400 * 7 if u.startswith("w") else 86400 * 30 if u.startswith("mo") else 86400
        return now - n * mult
    m = re.match(r"^([a-z]{3})[a-z]*\.?\s+(\d{1,2})$", t)
    if m:
        try:
            d = _dt.datetime.strptime(f"{m.group(1)} {m.group(2)} {_dt.date.today().year}", "%b %d %Y")
            if d.date() > _dt.date.today():
                d = d.replace(year=d.year - 1)
            return int(d.timestamp())
        except Exception:
            return None
    return None

# ---------------- Tier 1: ATS board tokens ----------------
# Verify each token by opening the board URL in the comment. Extend freely.
# Company->ATS map is data-driven: recruiting/companies.json (built by the mapping workflow).
# Each entry: {company, ats: greenhouse|lever|ashby|workday|custom, slug, applyUrl, ...}
#   greenhouse/lever/ashby slug = board token   |   workday slug = "host|tenant|site"
# TRACK-aware defaults: pm -> companies.json / openings.json / state.json;
#                       swe -> swe-companies.json / swe-openings.json / swe-state.json
_PFX = "swe-" if TRACK == "swe" else ""
COMPANIES_FILE = pathlib.Path(os.environ.get("COMPANIES_FILE", f"recruiting/{_PFX}companies.json"))

def load_companies():
    try:
        return json.loads(COMPANIES_FILE.read_text())
    except Exception as e:
        print(f"[companies] {e}", file=sys.stderr)
        return []

def _get(url, headers=None, timeout=20):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "pm-poller/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def _json(url, **kw):
    return json.loads(_get(url, **kw))

# visible source-fetch failures (Workday HTTP 401/422 etc.) — these used to only go to stderr,
# which nobody reads on a cron; surfacing them in the output JSON makes silent tier breakage debuggable.
_ERRORS = []
def _err(source, company, msg):
    _ERRORS.append({"src": source, "company": company, "error": str(msg)})
    print(f"[{source}] {company}: {msg}", file=sys.stderr)

def url_alive(url, timeout=10):
    """Is a specific posting's deep link still resolving? Used as the real signal for
    dropping a role — NOT "did this run's fuzzy search happen to re-surface it" (Workday's
    search ranking can flip a live posting in/out of its own top-20 run to run; see the
    2026-07-16 Salesforce APM incident where the direct job link 200'd on both sides of a
    search miss)."""
    if not url or not url.startswith("http"):
        return False
    # Default to ALIVE unless the link DEFINITIVELY resolves dead (404/410). ATS boards
    # (Workday/Greenhouse/iCIMS/Oracle) routinely 403/429 a bot UA or time out — that's
    # "blocked/unknown," NOT "closed," and treating it as dead was false-closing live roles
    # (the most common failure). Use a browser UA to cut down on blocks; only a hard
    # 404/410 (or a matching not-found body) counts as gone.
    UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status not in (404, 410)
    except urllib.error.HTTPError as e:
        return e.code not in (404, 410)   # 403/429/500/etc = blocked/unknown -> keep alive
    except Exception:
        return True                        # timeout/DNS/reset = unknown -> keep alive, don't false-close

# ---------------- location awareness ----------------
# Location lives on the POSTING, not the company — a SF-HQ company still posts NYC roles.
# LOCATION_FILTER=nyc keeps only NYC postings; unset keeps all and just tags nyc=True/False.
_NYC = re.compile(r"\b(new york|nyc|manhattan|brooklyn|new york city|ny,? (us|usa|united states)|, ny\b)\b", re.I)
LOCATION_FILTER = os.environ.get("LOCATION_FILTER", "").strip().lower()  # "" = all, "nyc" = NYC only

def is_nyc(loc):
    return bool(_NYC.search(loc or ""))

# US-only by design, and postings from global
# companies (TikTok, Mondelez, Salesforce, etc.) routinely include non-US offices. Reject a
# posting only when the location POSITIVELY names a non-US country/city — an empty or
# ambiguous location gets the benefit of the doubt rather than being silently dropped.
_NON_US = re.compile(
    r"\b(spain|mexico|brazil|argentina|chile|colombia|peru|italy|netherlands|switzerland|"
    r"germany|france|india|ireland|japan|united kingdom|\buk\b|singapore|australia|canada|"
    r"china|poland|philippines|vietnam|indonesia|malaysia|thailand|south korea|\bkorea\b|"
    r"taiwan|hong kong|dubai|\buae\b|saudi|israel|portugal|sweden|norway|denmark|finland|"
    r"austria|belgium|czech|hungary|romania|egypt|nigeria|kenya|south africa|new zealand|"
    r"beijing|shanghai|shenzhen|london|dublin|toronto|vancouver|amsterdam|berlin|munich|"
    r"paris|madrid|milan|tokyo|seoul|sydney|melbourne|singapore city)\b", re.I)

def is_us(loc):
    return not bool(_NON_US.search(loc or ""))

# ---- term awareness (Summer/Fall/Winter/Spring) — the term is on the posting, not the company ----
_TERMS = [
    ("Summer 2027", re.compile(r"summer[\s'`-]*(20)?27", re.I)),
    ("Fall 2026",   re.compile(r"(fall|autumn)[\s'`-]*(20)?26", re.I)),
    ("Winter 2027", re.compile(r"winter[\s'`-]*(20)?27", re.I)),
    ("Spring 2027", re.compile(r"spring[\s'`-]*(20)?27", re.I)),
    ("Fall 2027",   re.compile(r"(fall|autumn)[\s'`-]*(20)?27", re.I)),
    ("Summer 2026", re.compile(r"summer[\s'`-]*(20)?26", re.I)),
]
TERM_FILTER = os.environ.get("TERM_FILTER", "").strip().lower()  # e.g. "fall 2026"; "" = all

def detect_term(text):
    for label, rx in _TERMS:
        if rx.search(text or ""):
            return label
    return ""   # unspecified — most intern reqs don't name the term in the title

def parse_hire_time(s):
    """jobright's `hireTime` field ("2027-Summer", "2026-Fall", sometimes just "2026" or
    "Fall") is a structured signal for the internship's actual start term — far better
    coverage (~80% of jobright postings) than regexing the title/location text, which is
    what every other source is stuck doing. Normalize to our "Season YYYY" canonical form;
    return "" rather than guess when there isn't enough info (year-only, season-only)."""
    s = (s or "").strip()
    m = re.match(r"^(\d{4})-(\w+)$", s)
    if m:
        season = m.group(2).capitalize()
        if season in ("Summer", "Fall", "Winter", "Spring"):
            return f"{season} {m.group(1)}"
    return ""

def _loc_ok(loc):
    if not is_us(loc):
        return False
    if LOCATION_FILTER == "nyc":
        return is_nyc(loc)
    return True

# ---------------- degree-level awareness ----------------
# Undergrad-only feed, not an MBA or
# other graduate-only candidate. Title-level "mba"/"phd" keywords are already excluded above, but
# jobright/simplify also expose the actual qualifications/requirements text, which sometimes
# reveals a grad-only req the title alone doesn't ("Product Manager Intern" titled generically,
# body says "currently pursuing an MBA degree" with no bachelor's/undergrad alternative).
_GRAD_ONLY_RE = re.compile(r"\bmba\b|\bphd\b|master'?s degree|graduate degree", re.I)
_UNDERGRAD_RE = re.compile(r"undergrad|bachelor'?s|\bbs\b", re.I)
def is_grad_only(text):
    t = text or ""
    return bool(_GRAD_ONLY_RE.search(t)) and not _UNDERGRAD_RE.search(t)

def from_greenhouse(company, token):
    out = []
    try:
        for j in _json(f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs").get("jobs", []):
            loc = (j.get("location") or {}).get("name", "")
            if is_pm_intern(j.get("title", "")) and _loc_ok(loc):
                out.append({"company": company, "title": j["title"], "location": loc, "nyc": is_nyc(loc),
                            "url": j.get("absolute_url", ""), "src": "greenhouse", "postedAt": _iso_ts(j.get("first_published") or j.get("updated_at"))})
    except Exception as e:
        _err("greenhouse", company, e)
    return out

def from_lever(company, token):
    out = []
    try:
        for j in _json(f"https://api.lever.co/v0/postings/{token}?mode=json"):
            loc = (j.get("categories") or {}).get("location", "")
            if is_pm_intern(j.get("text", "")) and _loc_ok(loc):
                out.append({"company": company, "title": j["text"], "location": loc, "nyc": is_nyc(loc),
                            "url": j.get("hostedUrl", ""), "src": "lever", "postedAt": _ms_ts(j.get("createdAt"))})
    except Exception as e:
        _err("lever", company, e)
    return out

def from_ashby(company, token):
    out = []
    try:
        for j in _json(f"https://api.ashbyhq.com/posting-api/job-board/{token}").get("jobs", []):
            loc = j.get("location", "") or j.get("locationName", "")
            if is_pm_intern(j.get("title", "")) and _loc_ok(loc):
                out.append({"company": company, "title": j["title"], "location": loc, "nyc": is_nyc(loc),
                            "url": j.get("jobUrl", ""), "src": "ashby", "postedAt": _iso_ts(j.get("publishedAt"))})
    except Exception as e:
        _err("ashby", company, e)
    return out

# ---------------- Tier 2: Workday (POST search; Salesforce/Adobe/enterprise) ----------------
def from_workday(company, cfg):
    host, tenant, site = cfg
    out, seen = [], set()
    for q in ("product manager intern", "associate product manager", "product management intern"):
        try:
            body = json.dumps({"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": q}).encode()
            req = urllib.request.Request(
                f"https://{host}/wday/cxs/{tenant}/{site}/jobs", data=body,
                headers={"Content-Type": "application/json", "Accept": "application/json",
                         "User-Agent": "pm-poller/1.0"})
            d = json.loads(urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "replace"))
            for j in d.get("jobPostings", []):
                title = j.get("title", "")
                loc = j.get("locationsText", "")
                if is_pm_intern(title) and title.lower() not in seen and _loc_ok(loc):
                    seen.add(title.lower())
                    out.append({"company": company, "title": title, "location": loc, "nyc": is_nyc(loc),
                                "url": f"https://{host}/en-US/{site}{j.get('externalPath','')}",
                                "src": "workday", "postedAt": _age_ts(j.get("postedOn"))})
        except Exception as e:
            _err("workday", company, f"{q}: {e}")
    return out

# ---------------- Tier 3: aggregator (GitHub raw README) ----------------
_TR_RE = re.compile(r"<tr>(.*?)</tr>", re.S | re.I)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)

def _html_rows_to_md(raw: str) -> str:
    """SimplifyJobs switched its README from markdown tables to HTML <table>s (one <td> per
    line), which the '|'-row parser below never saw — so Simplify contributed zero PM roles.
    Flatten each <tr> into a single '| c1 | c2 | ... |' line so the same parser handles both.
    The company cell is reduced to plain text so the first href left on the line is the apply
    link, not Simplify's company page."""
    lines = []
    for tr in _TR_RE.findall(raw):
        cells = [re.sub(r"\s+", " ", re.sub(r"</?br\s*/?>", "; ", c, flags=re.I)).replace("|", "/").strip()
                 for c in _TD_RE.findall(tr)]
        if len(cells) < 2:
            continue
        cells[0] = re.sub(r"<[^>]+>|[\U0001F000-\U0001FAFF☀-➿️]", "", cells[0]).strip()  # text only, no "🔥"
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)

def from_github_tracker():
    out = []
    for repo, branch in [("vanshb03/Summer2027-Internships", "dev"),
                         ("SimplifyJobs/Summer2027-Internships", "dev"),  # canonical S2027 feed, ~95 PM roles
                         ("speedyapply/2027-SWE-College-Jobs", "main"),   # S2027 SWE, location-tagged
                         ("speedyapply/2027-AI-College-Jobs", "main"),    # S2027 AI/ML — AI/ML focus
                         ("jobright-ai/2026-Product-Management-Internship", "master"),  # full PM feed (98+ rows)
                         ("abbybravo/PM-Recruiting-Hub", "main")]:        # PM/APM/TPM/Product Ops specific tracker
        try:
            raw = _get(f"https://raw.githubusercontent.com/{repo}/{branch}/README.md")
            raw += "\n" + _html_rows_to_md(raw)
            # jobright (and some others) list a company's extra roles on continuation rows whose
            # first cell is "↳" (meaning "same company as the row above"). Carry the last real
            # company forward across ALL table rows so those roles aren't silently dropped — this
            # was the parity gap where ~half of jobright's PM postings never made it into the feed.
            last_company = ""
            for line in raw.splitlines():
                if not line.startswith("|"):
                    continue
                cells = [c.strip() for c in line.strip("|").split("|")]
                if len(cells) < 2:
                    continue
                # trackers use: Company | Role | Location | Link | Age.
                # Some (speedyapply) use HTML <a> tags, others markdown [](); handle both.
                def _clean(s):
                    s = re.sub(r"<[^>]+>", "", s)                  # strip HTML tags
                    s = re.sub(r"\*\*|🔒|⛔|↳|\[|\]", "", s)        # md bold, lock/closed, cont., brackets
                    return s.strip()
                raw_co = cells[0].strip()
                is_cont = raw_co in ("↳", "") or set(raw_co) <= set("↳* ")
                if "---" in cells[0]:                              # header separator row
                    continue
                if not is_cont:                                   # a real company cell → remember it
                    _co = re.sub(r"\(.*?\)", "", _clean(cells[0])).strip()
                    if _co and _co.lower() != "company":
                        last_company = _co
                if is_pm_intern(line):
                    company = last_company
                    # markdown-link cells are **[Title](url)** — pull the bracketed link text so
                    # the URL doesn't leak into the title; fall back to _clean for HTML/plain cells
                    _mt = re.search(r"\[([^\]]+)\]\(", cells[1])
                    title = _clean(_mt.group(1)) if _mt else _clean(cells[1])
                    loc = _clean(cells[2]) if len(cells) > 2 else ""
                    if not company or not _loc_ok(loc):
                        continue
                    if "🔒" in line:                       # closed / no longer accepting apps
                        continue
                    # trackers wrap the link in HTML <a href> or markdown (url); PM-Recruiting-Hub
                    # instead puts a bare https://... straight in its own table cell, so fall back
                    # to matching a bare URL anywhere in the line if the wrapped forms don't hit.
                    # jobright rows link the COMPANY cell to the company homepage and the TITLE cell
                    # to the posting -- so the title cell's own link wins when it has one.
                    m = (re.search(r"\]\((https?://[^)\s]+)\)", cells[1])
                         or re.search(r'href="(https?://[^"]+)"', line)
                         or re.search(r"\((https?://[^)]+)\)", line)
                         or re.search(r"(https?://\S+?)\s*\|", line))
                    url = m.group(1) if m else ""
                    if not url.startswith("http"):          # a listing with no apply link isn't actionable
                        continue
                    posted = None
                    for _c in reversed(cells[2:]):
                        posted = _age_ts(_c)
                        if posted:
                            break
                    out.append({"company": company, "title": title, "location": loc, "nyc": is_nyc(loc),
                                "url": url, "src": "github", "postedAt": posted})
        except Exception as e:
            _err("github", repo, e)
    return out

# ---------------- Tier 4: jobright / intern-list.com ----------------
# Previously this source (jobright.ai, which intern-list.com is backed by) was ONLY used in
# test-completeness.js as an audit check ("does our feed already have what they list") — it was
# never actually ingested into the live feed, so it caught gaps but never filled them. Turns out
# it doesn't need Playwright at all: the page is server-rendered, with a clean job array sitting
# in the __NEXT_DATA__ blob. A plain GET is enough, and it's already caught real postings (e.g.
# Capital One's PM intern) that our OWN direct Workday integration for that company can't reach
# because Capital One's Workday endpoint is currently broken (see scraper errors).
_NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)

def from_jobright():
    out = []
    slug = "software_engineering" if TRACK == "swe" else "product_management"
    try:
        html = _get(f"https://jobright.ai/minisites-jobs/intern/us/{slug}?embed=true",
                    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                           "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"})
        m = _NEXT_DATA_RE.search(html)
        jobs = json.loads(m.group(1))["props"]["pageProps"]["initialJobs"] if m else []
        for j in jobs:
            if j.get("isNewGrad") or is_grad_only(j.get("qualifications", "")):
                continue
            title, loc = j.get("title", ""), j.get("location", "")
            if is_pm_intern(title) and _loc_ok(loc):
                out.append({"company": j.get("company", ""), "title": title, "location": loc,
                            "nyc": is_nyc(loc), "url": j.get("applyUrl", ""), "src": "jobright",
                            "term": parse_hire_time(j.get("hireTime", "")), "postedAt": _ms_ts(j.get("postedDate"))})
    except Exception as e:
        _err("jobright", "minisite", e)
    return out

# ---------------- Tier 4b: Simplify.jobs (also only ever a display-only tracker link before) ----------------
# Same story as jobright: simplify.jobs was listed in the Trackers tab for the user to click
# through, never actually ingested. Also server-renders its full job list into __NEXT_DATA__
# (385 historical hits for the PM/APM list; most are `active: false` — only the live ones count).
def from_simplify():
    out = []
    if TRACK != "pm":
        return out   # PM-only for now — simplify.jobs has no equivalent single-list SWE slug
                     # (their SWE lists are scoped, e.g. "FAANG-Software-Internships"); confirm
                     # the right one before wiring up SWE instead of guessing and 404ing every run
    path = "Associate-Product-Manager-Intern"
    try:
        html = _get(f"https://simplify.jobs/top-list/{path}",
                    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                           "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"})
        m = _NEXT_DATA_RE.search(html)
        hits = json.loads(m.group(1))["props"]["pageProps"]["initialJobHits"] if m else []
        for h in hits:
            if not h.get("active"):
                continue
            reqs = " ".join(h.get("requirements") or [])
            if is_grad_only(reqs):
                continue
            job = h.get("job") or {}
            title = job.get("title") or h.get("title", "")
            loc = ((h.get("locations") or [{}])[0]).get("value", "")
            if is_pm_intern(title) and _loc_ok(loc):
                out.append({"company": (job.get("company") or {}).get("name", ""), "title": title,
                            "location": loc, "nyc": is_nyc(loc), "url": h.get("url", ""), "src": "simplify",
                            "postedAt": _iso_ts(h.get("start_date"))})
    except Exception as e:
        _err("simplify", "top-list", e)
    return out

# ---------------- Tier 5: Product Haven community Slack ----------------
# A community workspace has no installable bot (you're a member, not an admin), so this reads
# the channels you're already in AS YOU, using your browser session's xoxc token + d cookie —
# the same "authenticate as the user" pattern the IG intel pull uses. Creds live in a gitignored
# recruiting/.slack-session.json and only ever go to Slack's own API. Missing/expired creds are
# a soft failure: the function logs and returns [] so the rest of the scrape is unaffected.
#
# .slack-session.json shape:
#   {"domain":"producthaven.slack.com","token":"xoxc-...","cookie_d":"xoxd-...",
#    "channels":["pm-role-postings","general"],"days":30}
SLACK_SESSION_FILE = pathlib.Path(os.environ.get(
    "SLACK_SESSION_FILE", str(pathlib.Path(__file__).resolve().parent / ".slack-session.json")))
_SLACK_URL_RE = re.compile(r"<(https?://[^>|]+)(?:\|([^>]+))?>|(https?://[^\s|>]+)")

def _slack_api(domain, method, token, cookie_d, params, timeout=25):
    """POST to Slack's web API authenticated as the user (xoxc token + d cookie)."""
    data = urllib.parse.urlencode({**params, "token": token}).encode()
    req = urllib.request.Request(
        f"https://{domain}/api/{method}", data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Cookie": f"d={cookie_d}",
                 "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        j = json.loads(r.read().decode("utf-8", "replace"))
    if not j.get("ok"):
        raise RuntimeError(j.get("error", "unknown slack error"))
    return j

def _slack_resolve_channels(domain, token, cookie_d, names):
    """Map channel names (pm-role-postings, general) -> IDs, paginating conversations.list."""
    want = {n.lstrip("#").lower() for n in names}
    found, cursor = {}, ""
    for _ in range(20):                                   # cap pages; big workspaces
        j = _slack_api(domain, "conversations.list", token, cookie_d,
                       {"types": "public_channel,private_channel", "limit": 1000,
                        "exclude_archived": "true", "cursor": cursor})
        for c in j.get("channels", []):
            nm = (c.get("name") or "").lower()
            if nm in want:
                found[nm] = c["id"]
        cursor = (j.get("response_metadata") or {}).get("next_cursor", "")
        if not cursor or len(found) == len(want):
            break
    return found

def _company_from_url(url):
    """Best-effort company name from a job-posting URL's ATS structure."""
    u = url.lower()
    try:
        if "greenhouse.io" in u:
            m = re.search(r"boards\.greenhouse\.io/([^/?#]+)", u) or re.search(r"://([^.]+)\.greenhouse\.io", u)
            if m: return m.group(1).replace("-", " ").title()
        if "lever.co" in u:
            m = re.search(r"lever\.co/([^/?#]+)", u)
            if m: return m.group(1).replace("-", " ").title()
        if "ashbyhq.com" in u:
            m = re.search(r"ashbyhq\.com/([^/?#]+)", u)
            if m: return m.group(1).replace("-", " ").title()
        if "myworkdayjobs.com" in u:
            m = re.search(r"://([^.]+)\.wd\d+\.myworkdayjobs\.com", u)
            if m: return m.group(1).replace("-", " ").title()
    except Exception:
        pass
    m = re.search(r"://(?:www\.)?([^./]+)\.", url)
    return m.group(1).title() if m else "Product Haven"

def from_slack_producthaven():
    if not SLACK_SESSION_FILE.exists():
        return []                                         # not configured yet — silent, expected
    try:
        cfg = json.loads(SLACK_SESSION_FILE.read_text())
    except Exception as e:
        _err("producthaven", "config", e); return []
    domain = cfg.get("domain", "").strip()
    token, cookie_d = cfg.get("token", "").strip(), cfg.get("cookie_d", "").strip()
    names = cfg.get("channels") or ["pm-role-postings", "general"]
    days = int(cfg.get("days", 30))
    if not (domain and token and cookie_d):
        _err("producthaven", "config", "missing domain/token/cookie_d"); return []
    out = []
    try:
        chans = _slack_resolve_channels(domain, token, cookie_d, names)
        oldest = str(int(time.time()) - days * 86400)
        for nm in names:
            cid = chans.get(nm.lstrip("#").lower())
            if not cid:
                _err("producthaven", nm, "channel not found / not a member"); continue
            j = _slack_api(domain, "conversations.history", token, cookie_d,
                           {"channel": cid, "limit": 200, "oldest": oldest})
            for msg in j.get("messages", []):
                text = msg.get("text", "") or ""
                urls = []                                 # (url, label)
                for m in _SLACK_URL_RE.finditer(text):
                    urls.append((m.group(1) or m.group(3), (m.group(2) or "").strip()))
                if not urls:
                    continue
                labels = " ".join(l for _, l in urls if l)
                # role families the tracker cares about: text OR any link label must read PM-intern
                if not (is_pm_intern(text) or is_pm_intern(labels)):
                    continue
                # prefer a real ATS apply link; else first url
                job = next((u for u, _ in urls if any(a in u.lower() for a in
                            ("greenhouse.io", "lever.co", "ashbyhq.com", "myworkdayjobs.com"))), urls[0][0])
                label = next((l for u, l in urls if u == job and l), "")
                # title: an intern-ish link label, else the message's first line, cleaned + trimmed
                title = label if is_pm_intern(label) else re.sub(r"\s+", " ",
                            re.sub(r"<[^>]+>", "", text.split("\n")[0])).strip()[:110] or label or "PM Intern role"
                loc = ""
                lm = re.search(r"(?:location|based in|located in)[:\s]+([A-Za-z .,\-/]+)", text, re.I)
                if lm: loc = lm.group(1).strip()[:80]
                if loc and not _loc_ok(loc):
                    continue
                out.append({"company": _company_from_url(job), "title": title,
                            "location": loc, "nyc": is_nyc(loc), "url": job,
                            "src": "producthaven", "term": detect_term(f"{title} {text}")})
    except Exception as e:
        _err("producthaven", "history", e)
    return out

# ---------------- notify ----------------
def notify(new, health_alert=None):
    topic = os.environ.get("NTFY_TOPIC", "")
    if not new and not health_alert:
        return
    if not topic:
        print("NTFY_TOPIC unset; skipping push", file=sys.stderr)
        return
    label = "SWE-intern" if TRACK == "swe" else "PM-intern"
    parts = []
    if new:
        names = ", ".join(dict.fromkeys(r["company"] for r in new))  # unique, ordered
        parts.append(f"{len(new)} new {label} role(s): {names[:160]}")
    if health_alert:
        parts.append("⚠ " + health_alert)
    msg = " | ".join(parts)
    try:
        urllib.request.urlopen(urllib.request.Request(
            f"https://ntfy.sh/{topic}", data=msg.encode("utf-8"),
            headers={"Title": f"{label} openings", "Tags": "briefcase",
                     "User-Agent": "pm-poller/1.0"}), timeout=15)
    except Exception as e:
        print(f"[ntfy] {e}", file=sys.stderr)

# ---------------- observability ----------------
# Today's Salesforce incident (a real, live posting silently vanished from search results for
# 2+ cron cycles with zero visible signal) plus the 18 broken ATS integrations found by
# accident (see _ERRORS) made the case: track run-over-run health somewhere durable, not just
# "whatever's in the current run's stderr, if anyone happens to read it."
HISTORY_FILE = pathlib.Path(os.environ.get("HISTORY_FILE", str(pathlib.Path(__file__).resolve().parent / "scrape-history.jsonl")))
HISTORY_KEEP = 200   # ~9 weeks at 3 runs/day/track before the file needs trimming

def _last_history_total():
    if not HISTORY_FILE.exists():
        return None
    try:
        for line in reversed(HISTORY_FILE.read_text().strip().splitlines()):
            rec = json.loads(line)
            if rec.get("track") == TRACK:
                return rec.get("total")
    except Exception:
        pass
    return None

def _append_history(rec):
    try:
        lines = []
        if HISTORY_FILE.exists():
            lines = HISTORY_FILE.read_text().strip().splitlines()
        lines.append(json.dumps(rec))
        HISTORY_FILE.write_text("\n".join(lines[-HISTORY_KEEP:]) + "\n")
    except Exception as e:
        print(f"[history] {e}", file=sys.stderr)

# ---------------- main ----------------
def key(r):
    return (r["company"].lower().strip(), r["title"].lower().strip())

def main():
    roles = []
    for c in load_companies():
        name, ats, slug = c.get("company", ""), (c.get("ats") or "").lower(), c.get("slug") or ""
        if not slug:
            continue
        if ats == "greenhouse":
            roles += from_greenhouse(name, slug)
        elif ats == "lever":
            roles += from_lever(name, slug)
        elif ats == "ashby":
            roles += from_ashby(name, slug)
        elif ats == "workday":
            parts = slug.split("|")
            if len(parts) == 3:
                roles += from_workday(name, tuple(parts))
        # ats == "custom": apply-link only for now (Tier-2 fetchers TODO)
    roles += from_github_tracker()
    roles += from_jobright()
    roles += from_simplify()
    roles += from_slack_producthaven()   # Product Haven community Slack (session-token; no-op if unconfigured)

    # tag term (Summer 2027 / Fall 2026 / Winter 2027 …): prefer a source-supplied term
    # (jobright's hireTime is structured and far more reliable) over regexing the title/
    # location text, which is what every other source is stuck doing and mostly comes up empty
    for r in roles:
        r["term"] = r.get("term") or detect_term(f"{r.get('title','')} {r.get('location','')}")
    if TERM_FILTER:
        roles = [r for r in roles if r.get("term", "").lower() == TERM_FILTER]

    # dedupe
    seen, uniq = {}, []
    for r in roles:
        k = key(r)
        if k in seen:
            # same role from two sources: keep the earliest posting date either one knows
            kept = seen[k]
            ds = [d for d in (kept.get("postedAt"), r.get("postedAt")) if d]
            if ds: kept["postedAt"] = min(ds)
            continue
        seen[k] = r; uniq.append(r)

    # --- carry-forward safety net ---
    # A role can vanish from THIS run's fresh scrape without actually having closed — Workday's
    # searchText ranking is not stable run-to-run (confirmed 2026-07-16: Salesforce's real, live
    # "Summer 2027 Intern - Associate Product Manager" req 200'd directly the whole time, but
    # dropped out of every searchText query's top-20 eight hours after it first matched). Instead
    # of trusting "search didn't return it" as a closure signal, check the posting's own URL —
    # drop it only once that resolves dead, with a small miss-count cap as a backstop in case a
    # generic/board-root URL always 200s regardless of the specific req's status.
    # High cap: as long as the posting's own URL isn't a hard 404/410 (see url_alive), keep
    # carrying a role that fell out of the fuzzy search — search ranking is noisy, closures are
    # not. 60 runs (~30 days at 2x/day) is just a backstop for board-root URLs that always 200.
    MAX_CARRY_MISSES = 60
    out = pathlib.Path(os.environ.get("OUTPUT_FILE", f"public/{_PFX}openings.json"))
    prev_roles = []
    if out.exists():
        try: prev_roles = json.loads(out.read_text()).get("roles", [])
        except Exception: prev_roles = []
    miss_file = pathlib.Path(os.environ.get("MISS_FILE", f"{_PFX}miss-counts.json"))
    misses = {}
    if miss_file.exists():
        try: misses = json.loads(miss_file.read_text())
        except Exception: misses = {}

    fresh_keys = {key(r) for r in uniq}
    for k in list(misses):                          # re-matched fresh this run -> reset its miss count
        company, title = k.split("|||", 1)
        if (company, title) in fresh_keys:
            del misses[k]
    carried = 0
    for pr in prev_roles:
        k = key(pr)
        if k in fresh_keys:
            continue
        k_str = f"{k[0]}|||{k[1]}"
        n = misses.get(k_str, 0) + 1
        if n <= MAX_CARRY_MISSES and url_alive(pr.get("url", "")):
            uniq.append(pr); fresh_keys.add(k)
            misses[k_str] = n
            carried += 1
        else:
            misses.pop(k_str, None)                  # link is dead, or missed too many runs -> drop for good
    miss_file.write_text(json.dumps(misses))

    # --- first-seen tracking: powers the "NEW" badge in Open Now (apply-velocity is the
    # documented #1 low-effort edge on rolling reqs — see market-edges.json) ---
    fs_file = pathlib.Path(os.environ.get("FIRST_SEEN_FILE", f"{_PFX}first-seen.json"))
    first_seen = {}
    if fs_file.exists():
        try: first_seen = json.loads(fs_file.read_text())
        except Exception: first_seen = {}
    now_ts = int(time.time())
    for r in uniq:
        k = key(r); k_str = f"{k[0]}|||{k[1]}"
        if k_str not in first_seen:
            first_seen[k_str] = now_ts
        r["firstSeen"] = first_seen[k_str]
    live = {f"{key(r)[0]}|||{key(r)[1]}" for r in uniq}
    fs_file.write_text(json.dumps({k: v for k, v in first_seen.items() if k in live}))

    # --- role lifecycle: real lastSeen + removedAt tracking ---
    # first-seen.json (above) only ever carries a bare firstSeen int and gets PRUNED to
    # currently-live keys every run (see the `if k in live` filter above) — that's fine for its
    # one job (powering the "NEW" badge) but means it actively throws away a role's record the
    # moment it drops out, which is exactly the history the brief needs kept. Rather than change
    # first-seen.json's long-standing prune-to-live behavior (and risk the NEW badge feature),
    # this keeps a SEPARATE, never-pruned store keyed the same way (company|||title) with a
    # richer per-role record: firstSeen/lastSeen/removedAt/active + the last known posting
    # details, so a closed role can still be rendered ("Closed {date}") after it vanishes.
    #
    # GROUND TRUTH: there is no historical close data before this ran for the first time — a
    # role that vanished on some earlier run has no real removedAt and never will. removedAt is
    # only ever stamped the run that ACTUALLY observes a previously-tracked role missing (once,
    # on the active->inactive transition) — never guessed, never backfilled.
    lc_file = pathlib.Path(os.environ.get("LIFECYCLE_FILE",
        str(pathlib.Path(__file__).resolve().parent / f"{_PFX}role-lifecycle.json")))
    lifecycle = {}
    if lc_file.exists():
        try: lifecycle = json.loads(lc_file.read_text())
        except Exception: lifecycle = {}
    for r in uniq:
        k = key(r); k_str = f"{k[0]}|||{k[1]}"
        entry = lifecycle.get(k_str, {})
        # first-seen.json (above) prunes a key the moment it drops out of `live`, so a role that
        # disappears and later reappears looks "new" to IT and gets handed a fresh now_ts — but
        # role-lifecycle.json was never pruned, so it may still hold the true original firstSeen.
        # Always keep the EARLIEST known timestamp, never let a later re-scrape overwrite it with
        # a newer one (that would be losing real history, not just failing to add it).
        true_first_seen = min(entry["firstSeen"], r.get("firstSeen", now_ts)) if entry.get("firstSeen") else r.get("firstSeen", now_ts)
        entry.update({"company": r.get("company", ""), "title": r.get("title", ""),
                      "location": r.get("location", ""), "url": r.get("url", ""),
                      "firstSeen": true_first_seen, "lastSeen": now_ts, "active": True})
        entry.pop("removedAt", None)             # reappeared after a scrape miss -> not "closed"
        lifecycle[k_str] = entry
        r["firstSeen"] = true_first_seen          # correct the role's own output field too (NEW-badge + Opened chip)
        r["lastSeen"] = now_ts
        r["active"] = True
    newly_closed = 0
    for k_str, entry in lifecycle.items():
        if k_str not in live and entry.get("active", True):
            entry["removedAt"] = now_ts           # stamped exactly once, at the transition
            entry["active"] = False
            newly_closed += 1
    lc_file.write_text(json.dumps(lifecycle, indent=2, sort_keys=True))

    # surface recently-closed roles in the site feed (dimmed "Closed {date}" chip in the UI)
    # for a bounded grace window, instead of a role just silently vanishing with zero trace on
    # the live site the way every removal did before this — but never resurrected as "active"
    # and never given a removedAt earlier than the run that actually caught it missing.
    CLOSED_GRACE_SECONDS = 14 * 86400
    closed_roles = []
    for k_str, entry in lifecycle.items():
        if entry.get("active", True):
            continue
        removed_at = entry.get("removedAt")
        if not removed_at or now_ts - removed_at > CLOSED_GRACE_SECONDS:
            continue
        closed_roles.append({"company": entry.get("company", ""), "title": entry.get("title", ""),
                             "location": entry.get("location", ""), "url": entry.get("url", ""),
                             "src": "closed", "nyc": False, "term": "",
                             "firstSeen": entry.get("firstSeen"), "lastSeen": entry.get("lastSeen"),
                             "removedAt": removed_at, "active": False})

    # diff vs last run
    state = pathlib.Path(os.environ.get("STATE_FILE", f"{_PFX}state.json"))
    prev = set()
    if state.exists():
        try: prev = set(tuple(x) for x in json.loads(state.read_text()))
        except Exception: prev = set()
    new = [r for r in uniq if key(r) not in prev]

    # persist state + site feed
    state.write_text(json.dumps([list(key(r)) for r in uniq]))
    out.parent.mkdir(parents=True, exist_ok=True)
    nyc = [r for r in uniq if r.get("nyc")]
    from collections import Counter
    terms = Counter(r.get("term") or "unspecified" for r in uniq)
    by_src = Counter(r.get("src", "?") for r in uniq)
    err_by_src = Counter(e.get("src", "?") for e in _ERRORS)
    out.write_text(json.dumps({"updated": int(time.time()), "count": len(uniq), "new": len(new),
                               "nyc": len(nyc), "carried": carried, "terms": dict(terms),
                               "closed": len(closed_roles), "newlyClosed": newly_closed,
                               "errors": _ERRORS, "roles": uniq + closed_roles}, indent=2))

    # observability: append this run to the durable history log, and flag (log + push) any
    # run-over-run collapse instead of letting it pass silently like Salesforce did today
    prev_total = _last_history_total()
    drop_pct = round((prev_total - len(uniq)) / prev_total * 100) if prev_total else None
    health_alert = None
    if prev_total is not None and drop_pct is not None and drop_pct >= 30 and len(uniq) < prev_total:
        health_alert = f"{TRACK.upper()} feed dropped {drop_pct}% run-over-run ({prev_total} -> {len(uniq)}) — check scraper health"
        print(f"[health] {health_alert}", file=sys.stderr)
    _append_history({"ts": int(time.time()), "track": TRACK, "total": len(uniq), "carried": carried,
                      "new": len(new), "errors": len(_ERRORS), "by_src": dict(by_src),
                      "err_by_src": dict(err_by_src)})

    print(f"total={len(uniq)} new={len(new)} nyc={len(nyc)} carried={carried} errors={len(_ERRORS)} "
          f"closed={len(closed_roles)} newlyClosed={newly_closed} "
          f"(loc={LOCATION_FILTER or 'all'} term={TERM_FILTER or 'all'})")
    print("by term:", dict(terms))
    notify(new, health_alert)

if __name__ == "__main__":
    main()
