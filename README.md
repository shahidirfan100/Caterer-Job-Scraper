# Caterer.com Jobs Scraper

An efficient web scraper for extracting hospitality and catering job listings from Caterer.com. This tool helps job seekers and recruiters find the latest opportunities in the hospitality industry, including chef positions, restaurant management roles, and catering jobs across various locations.

## Features

- **Comprehensive Job Scraping**: Extracts detailed job listings including titles, companies, locations, salaries, job types, and posting dates
- **Flexible Search Options**: Search by keywords, locations, and job categories to find specific roles
- **Detailed Descriptions**: Optionally fetches full job descriptions from individual job pages
- **Pagination Handling**: Automatically navigates through search result pages to collect the desired number of listings
- **Structured Data Output**: Saves results in a clean, consistent JSON format suitable for further processing or integration
- **Rate Limit Management**: Built-in delays and proxy support to respect website policies
- **Error Handling**: Robust error handling with fallback mechanisms for reliable data extraction

## Input Configuration

The scraper accepts the following input parameters to customize your job search:

### Required Parameters
None - all parameters are optional for maximum flexibility

### Optional Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `keyword` | string | Job title, skill, or keyword to search for | "Head Chef", "Restaurant Manager", "Sous Chef" |
| `location` | string | Geographic location for job search | "London", "Manchester", "Birmingham" |
| `category` | string | Job category filter (when available on Caterer.com) | "Chef", "Management", "Catering" |
| `startUrl` / `url` / `startUrls` | string/array | Custom Caterer.com URL(s) to start scraping from | "https://www.caterer.com/jobs/search/chef" |
| `results_wanted` | integer | Maximum number of job listings to collect (default: 100) | 50 |
| `max_pages` | integer | Maximum number of search pages to visit | 10 |
| `collectDetails` | boolean | Whether to fetch full job descriptions from detail pages (default: true) | false |
| `postedWithin` | string | Filter jobs by posting recency: "any", "24h", "7d", "30d" | "7d" |
| `cookies` / `cookiesJson` | string/object | Custom cookies for requests | |
| `proxyConfiguration` | object | Proxy settings for enhanced scraping reliability | |

## Output Schema

Each scraped job listing is saved as a JSON object with the following structure:

```json
{
  "title": "Head Chef",
  "company": "The Restaurant Group Ltd",
  "location": "London",
  "salary": "£35,000 - £40,000 per annum",
  "job_type": "Full-time",
  "date_posted": "2023-11-10T00:00:00.000Z",
  "description_html": "<p>We are seeking an experienced Head Chef...</p>",
  "description_text": "We are seeking an experienced Head Chef to join our team...",
  "url": "https://www.caterer.com/job/head-chef-london"
}
```

### Field Descriptions

- **title**: Job position title
- **company**: Hiring company or organization name
- **location**: Job location or region
- **salary**: Salary information when available
- **job_type**: Employment type (Full-time, Part-time, etc.)
- **date_posted**: ISO 8601 formatted posting date
- **description_html**: Full job description in HTML format
- **description_text**: Plain text version of the job description
- **url**: Direct link to the job listing on Caterer.com

## Usage Examples

### Basic Job Search
Search for chef positions without any filters:

```json
{
  "keyword": "chef",
  "results_wanted": 50
}
```

### Location-Specific Search
Find restaurant manager jobs in London:

```json
{
  "keyword": "restaurant manager",
  "location": "London",
  "results_wanted": 25
}
```

### Recent Jobs Only
Get jobs posted within the last 7 days:

```json
{
  "keyword": "hospitality",
  "postedWithin": "7d",
  "collectDetails": true
}
```

### Custom Start URL
Scrape from a specific Caterer.com search page:

```json
{
  "startUrls": ["https://www.caterer.com/jobs/search/head-chef"],
  "results_wanted": 100,
  "max_pages": 5
}
```

## Configuration Tips

### Optimizing Performance
- Set `results_wanted` to a reasonable number to avoid excessive scraping
- Use `max_pages` to limit the number of pages visited
- Enable `collectDetails` only if you need full job descriptions (increases scraping time)

### Handling Large Searches
- For broad searches, consider using location filters to narrow results
- Use `postedWithin` to focus on recent opportunities
- Monitor your proxy usage when running large-scale scrapes

### Proxy Configuration
For best results on the Apify platform, configure a proxy:

```json
{
  "proxyConfiguration": {
    "useApifyProxy": true
  }
}
```

## SEO Keywords

caterer jobs scraper, hospitality jobs, catering employment, chef positions, restaurant jobs, hospitality recruitment, job scraping tool, Caterer.com scraper, UK hospitality jobs, catering industry jobs

## Notes

- This scraper is designed to respect Caterer.com's terms of service and implements appropriate delays between requests
- Results are saved to an Apify dataset for easy export and integration
- The scraper handles various job listing formats and includes fallback parsing methods
- For large-scale scraping needs, consider using Apify's proxy services for optimal performance