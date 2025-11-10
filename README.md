# Remote.co Jobs Scraper

This Apify actor scrapes remote job listings from Remote.co using Crawlee's CheerioCrawler and gotScraping.

## Features

- Scrapes Remote.co search results and job detail pages (no browser required).
- Prefers structured data (JSON-LD) where available, falls back to HTML parsing.
- Handles pagination until the requested number of results is reached.
- Optional detail scraping mode to fetch full job descriptions.
- Saves results to an Apify dataset using a consistent schema.

## Input

The actor accepts the following input fields (all optional unless noted):

- `keyword` (string) — Job title or skill to search for. If omitted, the actor fetches general listings.
- `location` (string) — Location filter (Remote.co listings are typically remote/global but this is kept for consistency).
- `category` (string) — Job category to filter (if available on Remote.co).
- `startUrl` / `url` / `startUrls` — Specific Remote.co search URL(s) to start from. If provided, these override keyword/location.
- `results_wanted` (integer) — Maximum number of job listings to collect. Default: 100.
- `max_pages` (integer) — Safety cap on number of listing pages to visit.
- `collectDetails` (boolean) — If true, the actor will visit each job detail page to extract full description. Default: true.
- `cookies` / `cookiesJson` — Optional cookies to include in requests.
- `proxyConfiguration` — Proxy settings (use Apify Proxy for best results).

## Output

Each item saved to the dataset follows this structure:

````markdown
# Caterer.com Jobs Scraper

This Apify actor scrapes hospitality and catering job listings from Caterer.com using Crawlee's CheerioCrawler and gotScraping.

## Features

- Scrapes Caterer.com search results and job detail pages (no browser required).
- Prefers structured data (JSON-LD) where available, falls back to HTML parsing.
- Handles pagination until the requested number of results is reached.
- Optional detail scraping mode to fetch full job descriptions.
- Saves results to an Apify dataset using a consistent schema.

## Input

The actor accepts the following input fields (all optional unless noted):

- `keyword` (string) — Job title or skill to search for (e.g., 'Head Chef', 'Restaurant Manager', 'Sous Chef'). If omitted, the actor fetches general listings.
- `location` (string) — Location filter for job search (e.g., 'London', 'Manchester', 'Birmingham').
- `category` (string) — Job category to filter (if available on Caterer.com).
- `startUrl` / `url` / `startUrls` — Specific Caterer.com search URL(s) to start from. If provided, these override keyword/location.
- `results_wanted` (integer) — Maximum number of job listings to collect. Default: 100.
- `max_pages` (integer) — Safety cap on number of listing pages to visit.
- `collectDetails` (boolean) — If true, the actor will visit each job detail page to extract full description. Default: true.
- `cookies` / `cookiesJson` — Optional cookies to include in requests.
- `proxyConfiguration` — Proxy settings (use Apify Proxy for best results).

## Output

Each item saved to the dataset follows this structure:

```
{
	"title": "...",
	"company": "...",
	"category": "...",
	"location": "...",
	"date_posted": "...",
	"description_html": "<p>...</p>",
	"description_text": "Plain text version of description",
	"url": "..."
}
```

## Notes

- The actor uses CheerioCrawler with gotScraping; no additional local packages are required beyond those in package.json.
- On Apify platform, provide `proxyConfiguration` and reasonable `results_wanted` to avoid rate limits.
- If Caterer.com changes their markup, selectors in `src/main.js` may need small updates.
````

## Notes

- The actor uses CheerioCrawler with gotScraping; no additional local packages are required beyond those in package.json.
- On Apify platform, provide `proxyConfiguration` and reasonable `results_wanted` to avoid rate limits.
- If Remote.co changes their markup, selectors in `src/main.js` may need small updates.