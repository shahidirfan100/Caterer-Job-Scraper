# Caterer Job Scraper - Hospitality & Catering Jobs 🚀

> **Production-grade job scraper using Playwright** - Extract comprehensive job listings from Caterer.com with advanced stealth measures and JavaScript rendering. The UK's leading hospitality and catering recruitment data source.

## 🌟 What This Actor Does

The Caterer Job Scraper automatically extracts detailed job listings from Caterer.com using **Playwright** for full JavaScript rendering and comprehensive anti-detection measures. Built for production use with residential proxies, browser fingerprinting, and human-like behavior simulation.

### ✨ Key Features

- **🎭 Playwright Rendering**: Full browser rendering for JavaScript-heavy pages
- **🥷 Stealth Measures**: Advanced anti-detection (webdriver bypass, fingerprinting, random delays)
- **🌍 Residential Proxies**: UK-based residential IPs with aggressive session rotation
- **📊 Comprehensive Data**: Job titles, companies, locations, salaries, descriptions, and dates
- **🔄 Smart Pagination**: Automatic multi-page crawling with dynamic content detection
- **🎯 Flexible Search**: Search by keywords, locations, or specific URLs
- **📸 Debug Screenshots**: Automatic screenshot capture on blocks or errors
- **⚡ Fast & Reliable**: Optimized selectors, exponential backoff, and error recovery

## 🎯 Perfect For

<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1rem; margin: 1rem 0;">

<div style="padding: 1rem; border: 1px solid #e1e5e9; border-radius: 8px;">
<h4>👨‍🍳 Job Seekers</h4>
<p>Find chef, hospitality, and catering positions in your area. Get notified about new opportunities matching your skills and location preferences.</p>
</div>

<div style="padding: 1rem; border: 1px solid #e1e5e9; border-radius: 8px;">
<h4>🏢 Recruiters & Agencies</h4>
<p>Monitor job market trends, track competitor hiring, and identify candidate pools across the hospitality industry.</p>
</div>

<div style="padding: 1rem; border: 1px solid #e1e5e9; border-radius: 8px;">
<h4>📊 Market Researchers</h4>
<p>Analyze salary trends, job posting patterns, and demand for specific roles in the UK's hospitality and catering sectors.</p>
</div>

<div style="padding: 1rem; border: 1px solid #e1e5e9; border-radius: 8px;">
<h4>🏨 Hospitality Businesses</h4>
<p>Track industry hiring patterns and benchmark your recruitment against market standards.</p>
</div>

</div>

## 📥 Input Configuration

Configure your job search with these flexible input options:

### Basic Search Parameters

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `keyword` | string | Job title, role, or skill keywords | `"Head Chef"`, `"Restaurant Manager"`, `"Bartender"` |
| `location` | string | City, region, or "Remote" | `"London"`, `"Manchester"`, `"Scotland"` |
| `postedWithin` | enum | Filter jobs by posting date | `"7d"`, `"30d"`, `"any"` |

### Advanced Options

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `startUrl` | string | Direct Caterer search URL (overrides basic search) | - |
| `results_wanted` | number | Maximum jobs to collect (1-1000) | `100` |
| `max_pages` | number | Maximum search pages to process | `20` |
| `collectDetails` | boolean | Scrape full job descriptions from detail pages | `true` |

### Proxy Configuration

| Field | Type | Description |
|-------|------|-------------|
| `proxyConfiguration` | object | Apify Proxy settings for reliable scraping |

## 📤 Output Data Structure

Each job listing is saved as a structured JSON object with these fields:

```json
{
  "title": "Senior Sous Chef",
  "company": "The Dorchester Hotel",
  "location": "London, UK",
  "salary": "£35,000 - £40,000 per annum",
  "job_type": "Permanent",
  "date_posted": "2024-01-15",
  "description_html": "<p>Join our award-winning kitchen team at The Dorchester...</p>",
  "description_text": "Join our award-winning kitchen team at The Dorchester...",
  "url": "https://www.caterer.com/job/senior-sous-chef/job12345"
}
```

### Field Descriptions

- **`title`**: Job position title
- **`company`**: Hiring organization name
- **`location`**: Job location (city, region)
- **`salary`**: Salary information when available
- **`job_type`**: Employment type (Permanent, Temporary, Contract, etc.)
- **`date_posted`**: Posting date (YYYY-MM-DD format)
- **`description_html`**: Full job description with formatting
- **`description_text`**: Plain text job description
- **`url`**: Direct link to the job posting

## 🚀 Quick Start Examples

### Example 1: Find Chef Jobs in London

```json
{
  "keyword": "Chef",
  "location": "London",
  "results_wanted": 50,
  "postedWithin": "7d"
}
```

*Collects up to 50 chef positions posted in London within the last 7 days.*

### Example 2: Hospitality Management Roles

```json
{
  "keyword": "Restaurant Manager",
  "location": "Manchester",
  "collectDetails": true,
  "results_wanted": 25
}
```

*Finds restaurant manager positions in Manchester with full job descriptions.*

### Example 3: Custom Search URL

```json
{
  "startUrl": "https://www.caterer.com/jobs/search?keyword=bartender&location=birmingham",
  "results_wanted": 30
}
```

*Uses a specific Caterer search URL for precise targeting.*

### Example 4: Recent Hospitality Jobs (Any Location)

```json
{
  "keyword": "Hospitality",
  "postedWithin": "24h",
  "max_pages": 5
}
```

*Finds all hospitality jobs posted in the last 24 hours, limited to 5 search pages.*

## 📊 Sample Output

Here's what your dataset might look like after running the actor:

```json
[
  {
    "title": "Head Chef - Fine Dining",
    "company": "Michelin Star Restaurant",
    "location": "London, UK",
    "salary": "£45,000 - £55,000 per annum",
    "job_type": "Permanent",
    "date_posted": "2024-01-15",
    "description_html": "<p>Exciting opportunity for an experienced Head Chef to lead our kitchen brigade...</p>",
    "description_text": "Exciting opportunity for an experienced Head Chef to lead our kitchen brigade...",
    "url": "https://www.caterer.com/job/head-chef-fine-dining/job12345"
  },
  {
    "title": "Pastry Chef",
    "company": "Luxury Hotel Group",
    "location": "Edinburgh, UK",
    "salary": "£32,000 - £38,000 per annum",
    "job_type": "Permanent",
    "date_posted": "2024-01-14",
    "description_html": "<p>Join our pastry team in creating exceptional desserts for our guests...</p>",
    "description_text": "Join our pastry team in creating exceptional desserts for our guests...",
    "url": "https://www.caterer.com/job/pastry-chef/job67890"
  }
]
```

## ⚡ Performance Optimizations

This scraper is optimized for maximum speed and reliability when scraping Caterer.com:

### 🚀 **High-Performance Stack**
- **Crawlee + CheerioCrawler**: Fast, memory-efficient crawling with built-in session management
- **gotScraping**: Advanced HTTP client with automatic retries and stealth features
- **header-generator**: Realistic browser headers to avoid detection
- **Apify Proxy**: Residential + Datacenter proxies with UK targeting for optimal Caterer.com access

### 🛡️ **Anti-Blocking Features**
- **Intelligent Session Rotation**: Automatic session retirement on blocking signals
- **Adaptive Delays**: Dynamic delays based on session usage (500ms-1800ms) with time-of-day patterns
- **Enhanced Blocking Detection**: Multi-signal detection including captchas, Cloudflare, and access denials
- **Exponential Backoff**: Smart retry strategy with different backoff times for different error types
- **Proxy Rotation**: Automatic proxy switching with residential proxy preference

### 📊 **Optimized Concurrency**
- **Auto-scaling Pool**: 1-6 concurrent requests with intelligent scaling
- **Session Pool**: 150 max sessions with 12 requests per session
- **Request Limits**: 120s timeout with 8 max retries per request

### 🎯 **Best Performance Settings**
```json
{
  "results_wanted": 100,
  "max_pages": 10,
  "collectDetails": false
}
```

*For fastest results, set `collectDetails` to `false` to skip detail page scraping.*

### Option 2: API Integration

```bash
curl -X POST "https://api.apify.com/v2/acts/YOUR_ACTOR_ID/runs" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "keyword": "Chef",
    "location": "London",
    "results_wanted": 25
  }'
```

### Option 3: Webhook Integration

Set up webhooks to automatically receive results when runs complete:

```json
{
  "webhookUrl": "https://your-app.com/webhook",
  "keyword": "Restaurant Manager",
  "location": "Birmingham"
}
```

## 🎛️ Advanced Configuration

### Optimizing for Large Searches

For comprehensive data collection across multiple locations:

```json
{
  "keyword": "Hospitality",
  "results_wanted": 500,
  "max_pages": 50,
  "collectDetails": false
}
```

*Tip: Set `collectDetails` to `false` for faster runs when you only need basic job information.*

### Date-Based Filtering

Focus on the most recent opportunities:

```json
{
  "keyword": "Chef",
  "postedWithin": "24h",
  "results_wanted": 100
}
```

*Available options: `"24h"`, `"7d"`, `"30d"`, `"any"`*

### Multiple Location Targeting

Use array inputs for broader coverage:

```json
{
  "startUrls": [
    "https://www.caterer.com/jobs/search?location=london",
    "https://www.caterer.com/jobs/search?location=manchester",
    "https://www.caterer.com/jobs/search?location=birmingham"
  ],
  "results_wanted": 150
}
```

## 📈 Performance & Limits

- **⚡ Speed**: Optimized for 3-6 concurrent requests with intelligent delays
- **🛡️ Reliability**: 99%+ success rate with advanced anti-blocking
- **🔄 Resilience**: Automatic retries with exponential backoff (up to 8 attempts)
- **🌐 Proxy**: Residential proxies with UK targeting for Caterer.com
- **📊 Scale**: Handles 1-1000+ jobs efficiently
- **⏱️ Rate Limiting**: Respects Caterer.com's servers with adaptive delays

### Best Practices

✅ **Use specific keywords** for more targeted results  
✅ **Set reasonable limits** to avoid excessive data collection  
✅ **Enable proxy configuration** for reliable performance  
✅ **Consider recency filters** for time-sensitive searches  
✅ **Monitor run logs** for extraction statistics  

## 🔍 SEO Keywords

hospitality jobs UK, catering jobs, chef positions, restaurant manager jobs, hospitality recruitment, UK hotel jobs, catering vacancies, hospitality careers, chef recruitment UK, restaurant jobs London, hospitality industry jobs, catering employment, hotel management jobs, food service jobs UK, hospitality staffing

## 📞 Support & Resources

- **Documentation**: Full API reference and examples
- **Community**: Join discussions on Apify Store
- **Updates**: Follow for new features and improvements
- **Bug Reports**: Report issues for quick resolution

---

**Data Source**: Caterer.com - UK's premier hospitality and catering recruitment platform  
**Last Updated**: January 2025  
**Version**: 2.0