#!/usr/bin/env node
/*
 * Playwright career-page scraper — the coverage fix.
 *
 * The ATS-API scraper (scraper.py) only reaches companies on standard job boards
 * (Greenhouse/Lever/Ashby/Workday). It's BLIND to ~100 companies on custom career
 * sites (Google, Meta, Amazon, Apple, Microsoft, Uber, Stripe…) and to dead tokens.
 * This renders each such company's careers SEARCH page in a real headless browser and
 * scrapes the job cards the JS renders — catching what the APIs miss.
 *
 *   TRACK=pm  node recruiting/pw-scraper.js      -> public/pw-openings.json
 *   TRACK=swe node recruiting/pw-scraper.js      -> public/pw-swe-openings.json
 */
const { chromium } = require("playwright");
const fs = require("fs");
const path = require("path");

const TRACK = (process.env.TRACK || "pm").toLowerCase();
const R = __dirname;
const REPO = path.resolve(R, "..");
const companies = JSON.parse(fs.readFileSync(path.join(R, TRACK === "swe" ? "swe-companies.json" : "companies.json"), "utf8"));

const INTERN = /\bintern(ship)?s?\b/i;
const INC = TRACK === "swe"
  ? [/software engineer/i, /software developer/i, /software engineering/i, /\bswe\b/i, /\bsde\b/i, /backend|front[- ]?end|full[- ]?stack/i, /developer intern/i]
  : [/product manag/i, /associate product manager/i, /\bapm\b/i, /product intern/i, /\bpm intern/i];
const EXC = [/senior|staff|principal|lead\b/i, /director|\bvp\b|head of/i, /manager,/i, /full[- ]time/i, /new ?grad/i, /\bmba\b|\bphd\b/i, /mechanical|electrical|hardware|firmware|civil/i];
const NYC = /new york|nyc|manhattan|brooklyn|,\s*ny\b/i;
// UI/noise lines that aren't job titles (result counts, filters, search echoes)
const JUNK = /\bresults?\b|\bshowing\b|\bfilter(s|ed| by)?\b|\bsort by\b|\bsearch\b|^\d[\d,]*\s|category|refine|no jobs|clear all|view all|^\d+$/i;
const isMatch = (t) => !!t && INTERN.test(t) && INC.some((r) => r.test(t)) && !EXC.some((r) => r.test(t)) && !JUNK.test(t);

// scrape the companies the API scraper can't reach: custom sites + any with no usable token
const targets = companies.filter((c) => ((c.ats || "").toLowerCase() === "custom" || !c.slug) && c.applyUrl);

async function scrapeOne(context, c) {
  const page = await context.newPage();
  const out = [];
  try {
    await page.goto(c.applyUrl, { waitUntil: "domcontentloaded", timeout: 35000 });
    await page.waitForTimeout(4500);                       // let the SPA render job cards
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
    await page.waitForTimeout(1500);
    // titles on most SPAs live in non-anchor nodes -> read rendered TEXT lines,
    // and separately map anchor text/aria-label -> href so we can resolve a real posting URL.
    const { lines, anchors } = await page.evaluate(() => {
      const lines = (document.body.innerText || "").split("\n").map((s) => s.trim().replace(/\s+/g, " ")).filter((s) => s.length >= 8 && s.length <= 130);
      const anchors = [];
      document.querySelectorAll("a[href]").forEach((a) => {
        const t = (a.innerText || a.getAttribute("aria-label") || a.textContent || "").trim().replace(/\s+/g, " ");
        if (t && !a.href.startsWith("javascript")) anchors.push({ t: t.toLowerCase(), href: a.href });
      });
      return { lines, anchors };
    });
    const seen = new Set();
    for (const line of lines) {
      if (!isMatch(line)) continue;
      if (/^(search|filter|results|product management|software engineering)\b.*\bintern/i.test(line) && line.split(" ").length <= 3) continue; // query echo
      const key = line.toLowerCase();
      if (seen.has(key)) continue; seen.add(key);
      // resolve URL: anchor whose text overlaps the title, else the search page itself
      let url = c.applyUrl;
      const hit = anchors.find((a) => a.t && (a.t.includes(key) || key.includes(a.t)) && a.t.length > 8);
      if (hit) url = hit.href;
      out.push({ company: c.company, title: line, url, location: "", nyc: NYC.test(line), src: "careerpage" });
    }
  } catch (e) {
    process.stderr.write(`[pw] ${c.company}: ${e.message.split("\n")[0]}\n`);
  } finally {
    await page.close();
  }
  return out;
}

// bounded concurrency
async function run() {
  const browser = await chromium.launch({ headless: true, args: ["--disable-http2", "--disable-blink-features=AutomationControlled"] });
  const context = await browser.newContext({ locale: "en-US", userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36" });
  const results = [];
  const CONC = 4;
  let i = 0;
  async function worker() {
    while (i < targets.length) {
      const c = targets[i++];
      const roles = await scrapeOne(context, c);
      if (roles.length) process.stdout.write(`  ${c.company}: ${roles.length} role(s)\n`);
      results.push(...roles);
    }
  }
  await Promise.all(Array.from({ length: CONC }, worker));
  await browser.close();

  const nyc = results.filter((r) => r.nyc).length;
  const outPath = path.join(REPO, "public", TRACK === "swe" ? "pw-swe-openings.json" : "pw-openings.json");
  fs.writeFileSync(outPath, JSON.stringify({ updated: Math.floor(Date.now() / 1000), count: results.length, nyc, roles: results }, null, 2));
  process.stdout.write(`\n[${TRACK}] career-page scrape: ${results.length} roles across ${targets.length} custom sites | ${nyc} NYC -> ${path.relative(REPO, outPath)}\n`);
}
run().catch((e) => { console.error(e); process.exit(1); });
