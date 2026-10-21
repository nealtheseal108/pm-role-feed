# pm-role-feed

Scheduled scraper for US PM/APM and SWE internship postings. It reads public job boards
(Greenhouse, Lever, Ashby, Workday and others), company career pages, and public internship
trackers on GitHub, then commits the results:

- `public/openings.json`, `public/pw-openings.json`: PM/APM internship roles
- `public/swe-openings.json`, `public/pw-swe-openings.json`: SWE internship roles

Runs every 8 hours on GitHub Actions (`.github/workflows/refresh.yml`).
