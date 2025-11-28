// Caterer Job Scraper - Production-grade implementation using got-scraping for stealth
// Stack: Apify + got-scraping + cheerio + header-generator (bypasses anti-bot better than CheerioCrawler)
import { Actor, log } from 'apify';
import { Dataset } from 'crawlee';
import { gotScraping } from 'got-scraping';
import { load as cheerioLoad } from 'cheerio';
import fs from 'fs/promises';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Time constants
const RELATIVE_UNIT_MS = {
    minute: 60 * 1000,
    hour: 60 * 60 * 1000,
    day: 24 * 60 * 60 * 1000,
    week: 7 * 24 * 60 * 60 * 1000,
    month: 30 * 24 * 60 * 60 * 1000,
    year: 365 * 24 * 60 * 60 * 1000,
};

const RECENCY_WINDOWS = {
    '24h': RELATIVE_UNIT_MS.day,
    '7d': 7 * RELATIVE_UNIT_MS.day,
    '30d': 30 * RELATIVE_UNIT_MS.day,
};

let recencyWindowMs = null;
let postedWithinLabel = 'any';

// Utility functions
const normalizePostedWithin = (value) => {
    const allowed = new Set(['any', '24h', '7d', '30d']);
    if (!value || typeof value !== 'string') return 'any';
    const trimmed = value.trim().toLowerCase();
    return allowed.has(trimmed) ? trimmed : 'any';
};

const parsePostedDate = (value) => {
    if (!value && value !== 0) return null;
    if (value instanceof Date) return value;
    if (typeof value === 'number' && Number.isFinite(value)) return new Date(value);
    
    const text = String(value).trim();
    if (!text) return null;
    
    const parsed = Date.parse(text);
    if (!Number.isNaN(parsed)) return new Date(parsed);
    
    const lower = text.toLowerCase();
    if (lower.includes('just') || lower.includes('today') || lower.includes('new') || lower.includes('feature')) {
        return new Date();
    }
    if (lower.includes('yesterday')) {
        return new Date(Date.now() - RELATIVE_UNIT_MS.day);
    }
    
    const relMatch = lower.match(/(\d+)\s*(minute|hour|day|week|month|year)s?\s*ago/);
    if (relMatch) {
        const count = Number(relMatch[1]);
        const unit = relMatch[2];
        const multiplier = RELATIVE_UNIT_MS[unit];
        if (multiplier) return new Date(Date.now() - count * multiplier);
    }
    return null;
};

const normalizePostedDateValue = (value) => {
    const parsed = parsePostedDate(value);
    return parsed ? parsed.toISOString() : (value || null);
};

const shouldKeepByRecency = (dateValue) => {
    if (!recencyWindowMs) return true;
    const parsed = parsePostedDate(dateValue);
    if (!parsed) return true;
    return (Date.now() - parsed.getTime()) <= recencyWindowMs;
};

const normalizeSalaryText = (text) => {
    if (!text) return null;
    const cleaned = String(text)
        .replace(/\u00a3/g, '£')
        .replace(/\ufffd/g, '£')
        .replace(/\s+/g, ' ')
        .trim();
    return cleaned || null;
};

// Header Generator Setup
let HeaderGenerator;
try {
    const hg = await import('header-generator');
    HeaderGenerator = hg.default || hg.HeaderGenerator;
} catch (error) {
    log.warning('header-generator not available:', error?.message || String(error));
    HeaderGenerator = null;
}

let headerGeneratorInstance;
const initHeaderGenerator = () => {
    if (!HeaderGenerator || headerGeneratorInstance) return;
    try {
        headerGeneratorInstance = new HeaderGenerator({
            browsers: [
                { name: 'chrome', minVersion: 120, maxVersion: 131 },
                { name: 'firefox', minVersion: 115, maxVersion: 122 },
            ],
            operatingSystems: ['windows', 'macos'],
            devices: ['desktop'],
            locales: ['en-GB', 'en-US'],
        });
        log.info('HeaderGenerator initialized successfully');
    } catch (error) {
        log.warning('HeaderGenerator init failed:', error.message);
        headerGeneratorInstance = null;
    }
};

// Generate realistic browser headers
const getStealthHeaders = (referer = 'https://www.google.co.uk/') => {
    let headers;
    
    if (headerGeneratorInstance) {
        try {
            headers = headerGeneratorInstance.getHeaders();
        } catch (e) {
            log.debug('HeaderGenerator failed, using fallback');
        }
    }
    
    // Fallback or enhance headers
    const chromeVersion = 120 + Math.floor(Math.random() * 11); // 120-130
    return {
        'User-Agent': headers?.['user-agent'] || `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${chromeVersion}.0.0.0 Safari/537.36`,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Referer': referer,
        'Sec-Ch-Ua': `"Not_A Brand";v="8", "Chromium";v="${chromeVersion}", "Google Chrome";v="${chromeVersion}"`,
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        ...headers,
    };
};

// Human-like delay with randomization
const humanDelay = async (min = 2000, max = 5000) => {
    const delay = min + Math.random() * (max - min);
    // Add micro-variations to seem more human
    const microDelay = Math.random() * 500;
    await sleep(delay + microDelay);
};

// Error handlers
process.on('unhandledRejection', (reason) => {
    log.error('Unhandled Rejection:', reason);
});
process.on('uncaughtException', (err) => {
    log.error('Uncaught Exception:', err.stack || err);
});

async function main() {
    try {
        await Actor.init();
        log.info('Actor initialized');

        // Get input with fallback
        let input;
        try {
            input = await Actor.getInput();
        } catch (error) {
            log.warning('Failed to get input, trying fallback:', error.message);
            try {
                const raw = await fs.readFile(new URL('../INPUT.json', import.meta.url));
                input = JSON.parse(String(raw));
            } catch {
                input = {};
            }
        }
        input = input || {};

        // Parse input with safe defaults
        const safeInt = (v, def) => (Number.isFinite(+v) && +v > 0 ? +v : def);
        const safeBool = (v, def) => (typeof v === 'boolean' ? v : def);
        const safeStr = (v, def) => (typeof v === 'string' ? v : def);

        const keyword = safeStr(input.keyword, '');
        const location = safeStr(input.location, '');
        const RESULTS_WANTED = safeInt(input.results_wanted, 100);
        const MAX_PAGES = safeInt(input.max_pages, 20);
        const collectDetails = safeBool(input.collectDetails, true);
        const startUrl = safeStr(input.startUrl, '');
        const debugSaveHtml = safeBool(input.debugSaveHtml, false);

        postedWithinLabel = normalizePostedWithin(input.postedWithin);
        recencyWindowMs = RECENCY_WINDOWS[postedWithinLabel] || null;

        log.info('Configuration:', {
            keyword,
            location,
            results_wanted: RESULTS_WANTED,
            max_pages: MAX_PAGES,
            collectDetails,
            postedWithin: postedWithinLabel,
            debugSaveHtml,
        });

        initHeaderGenerator();

        // Proxy configuration - CRITICAL for avoiding blocks
        let proxyConfiguration = null;
        let getNewProxyUrl = null;
        let proxySessionId = null;
        let useProxy = true;
        
        try {
            const proxyConfig = input.proxyConfiguration || {
                useApifyProxy: true,
                apifyProxyGroups: ['RESIDENTIAL'],
            };
            proxyConfiguration = await Actor.createProxyConfiguration(proxyConfig);
            getNewProxyUrl = async () => {
                try {
                    return await proxyConfiguration?.newUrl();
                } catch (err) {
                    log.debug('Failed to get proxy URL:', err.message);
                    return null;
                }
            };
            const testUrl = await getNewProxyUrl();
            log.info('Proxy configured:', { 
                hasProxy: !!testUrl, 
                configProvided: !!input.proxyConfiguration,
                groups: proxyConfig.apifyProxyGroups 
            });
        } catch (e) {
            log.warning('Proxy setup failed, continuing without proxy:', e.message);
            getNewProxyUrl = async () => null;
            useProxy = false;
        }

        const rotateProxySession = () => {
            proxySessionId = `sess-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
            return proxySessionId;
        };

        const getProxyUrlForRequest = async (rotate = false) => {
            if (!getNewProxyUrl) return null;
            if (rotate || !proxySessionId) rotateProxySession();
            try {
                return await proxyConfiguration?.newUrl({ sessionId: proxySessionId });
            } catch {
                return await getNewProxyUrl();
            }
        };

        // Stats tracking
        const stats = {
            pagesProcessed: 0,
            jobsFound: 0,
            jobsSaved: 0,
            requestsFailed: 0,
            blockedResponses: 0,
            detailPagesProcessed: 0,
        };

        // Tracking sets
        const pushedUrls = new Set();
        let saved = 0;

        // Build start URL
        const buildSearchUrl = (kw, loc, page = 1) => {
            const u = new URL('https://www.caterer.com/jobs/search');
            if (kw) u.searchParams.set('keywords', kw.trim());
            if (loc) u.searchParams.set('location', loc.trim());
            if (page > 1) u.searchParams.set('page', String(page));
            return u.href;
        };

        const refererPool = [
            'https://www.google.co.uk/',
            'https://www.bing.com/search?q=uk+jobs',
            'https://www.yahoo.com/',
            'https://duckduckgo.com/?q=uk+jobs',
        ];

        // Make stealth request using got-scraping with rotating proxy
        const makeRequest = async (url, referer = 'https://www.google.co.uk/', retries = 6, tryWithoutProxy = true) => {
            for (let attempt = 1; attempt <= retries; attempt++) {
                try {
                    const headers = getStealthHeaders(referer);
                    
                    // Try without proxy on attempts 3+ if tryWithoutProxy is true
                    // Or if useProxy is already disabled
                    const shouldUseProxy = useProxy && attempt < 3;
                    const proxyUrl = shouldUseProxy ? await getProxyUrlForRequest(attempt > 1) : null;
                    
                    const options = {
                        url,
                        headers,
                        timeout: { 
                            request: 120000, // 2 minutes
                            connect: 30000   // 30 seconds to connect
                        },
                        retry: { limit: 0 },
                        throwHttpErrors: false,
                        http2: false, // Disable HTTP/2 for better compatibility
                        // got-scraping auto-generates browser-like TLS fingerprint
                        headerGeneratorOptions: {
                            browsers: ['chrome'],
                            operatingSystems: ['windows'],
                            devices: ['desktop'],
                            locales: ['en-GB'],
                        },
                    };

                    if (proxyUrl) {
                        options.proxyUrl = proxyUrl;
                    }

                    log.info(`Request attempt ${attempt}/${retries}:`, { 
                        url: url.slice(0, 80), 
                        hasProxy: !!proxyUrl,
                        strategy: proxyUrl ? 'with-proxy' : 'direct'
                    });
                    
                    const response = await gotScraping(options);
                    
                    log.info(`Response received:`, {
                        statusCode: response.statusCode,
                        contentLength: response.body?.length || 0
                    });
                    
                    // Check for blocking status codes
                    if ([403, 429, 503, 502, 504].includes(response.statusCode)) {
                        stats.blockedResponses++;
                        log.warning(`Blocked (${response.statusCode}) on attempt ${attempt}:`, { url: url.slice(0, 60) });
                        
                        if (attempt < retries) {
                            rotateProxySession();
                            referer = refererPool[Math.floor(Math.random() * refererPool.length)];
                            const backoff = Math.min(45000, 8000 * Math.pow(1.5, attempt - 1));
                            log.info(`Backing off ${Math.round(backoff / 1000)}s before retry with new proxy...`);
                            await sleep(backoff + Math.random() * 5000);
                            continue;
                        }
                        return null;
                    }

                    // Check for captcha/block content
                    const body = response.body.toLowerCase();
                    const blockSignals = ['captcha', 'blocked', 'access denied', 'cloudflare', 'security check', 'please verify', 'bot detected'];
                    
                    if (blockSignals.some(sig => body.includes(sig))) {
                        stats.blockedResponses++;
                        log.warning(`Content blocking detected on attempt ${attempt}`);
                        
                        if (attempt < retries) {
                            rotateProxySession();
                            referer = refererPool[Math.floor(Math.random() * refererPool.length)];
                            const backoff = Math.min(60000, 12000 * Math.pow(1.5, attempt - 1));
                            await sleep(backoff + Math.random() * 8000);
                            continue;
                        }
                        return null;
                    }

                    // Success!
                    log.debug(`Request successful (${response.statusCode})`);
                    return response.body;

                } catch (error) {
                    stats.requestsFailed++;
                    const isTimeout = /timeout|timed out/i.test(error.message);
                    const isConnection = /ECONNREFUSED|ENOTFOUND|ECONNRESET|socket hang up/i.test(error.message);
                    const isProxyError = /proxy/i.test(error.message);
                    
                    log.warning(`Request error (attempt ${attempt}/${retries}):`, { 
                        error: error.message?.slice(0, 150),
                        url: url.slice(0, 60),
                        isTimeout,
                        isConnection,
                        isProxyError
                    });
                    
                    // If proxy errors, disable proxy for next attempts
                    if (isProxyError && useProxy) {
                        log.warning('Proxy error detected, will try without proxy on next attempts');
                        useProxy = false;
                    }
                    
                    if (attempt < retries) {
                        const backoff = isTimeout ? 10000 : 5000 * attempt + Math.random() * 3000;
                        log.info(`Waiting ${Math.round(backoff/1000)}s before retry...`);
                        await sleep(backoff);
                    }
                }
            }
            log.error('All request attempts failed for:', url.slice(0, 80));
            return null;
        };

        
        // Extract jobs from list page
        const extractJobsFromList = ($, pageUrl) => {
            const jobs = [];
            const seenUrls = new Set();

            const pushJob = (job) => {
                if (!job?.url) return;
                if (seenUrls.has(job.url)) return;
                seenUrls.add(job.url);
                jobs.push(job);
            };

            // Log page structure for debugging
            log.info('Extracting jobs from page:', {
                totalLinks: $('a').length,
                jobLinks: $('a[href*="/job/"]').length,
                h2JobLinks: $('h2 a[href*="/job/"]').length,
                dataJobItems: $('[data-at="job-item"], [data-testid="job-card"]').length,
            });

            // Prefer explicit job cards (Caterer currently uses data-at="job-item")
            $('[data-at="job-item"], [data-testid="job-card"]').each((_, el) => {
                try {
                    const $card = $(el);
                    const $titleLink = $card.find('[data-at="job-title"], h2 a[href*="/job/"]').first();
                    const href = $titleLink.attr('href');
                    if (!href || !/\/job\/[^/]+\/.*job\d+/i.test(href)) return;

                    const jobUrl = new URL(href, pageUrl).href;
                    const title = ($titleLink.text() || '').trim();
                    if (!title || title.length < 3) return;

                    const containerText = $card.text().replace(/\s+/g, ' ').trim();

                    let company = null;
                    const companyText = $card.find('[data-at="company-name"], [data-qa="company"], [class*="company"], [class*="employer"]').first().text();
                    if (companyText) company = companyText.trim() || null;

                    let location = null;
                    const locationText = $card.find('[data-at="job-location"], [data-qa="location"], [class*="location"]').first().text();
                    if (locationText) location = locationText.trim() || null;

                    let salary = null;
                    const salaryText = $card.find('[data-at="salary"], [data-qa="salary"], [class*="salary"], .wage').first().text();
                    if (salaryText) salary = normalizeSalaryText(salaryText);

                    if (!salary) {
                        const salaryMatch = containerText.match(/[\u00a3][\d,]+(?:\.\d{2})?(?:\s*[-–]\s*[\u00a3][\d,]+(?:\.\d{2})?)?(?:\s*(?:per|p\/|\/)\s*(?:hour|annum|year|day|week|month))?/i);
                        if (salaryMatch) salary = normalizeSalaryText(salaryMatch[0]);
                    }

                    let jobType = null;
                    const typePatterns = ['full time', 'full-time', 'part time', 'part-time', 'permanent', 'contract', 'temporary', 'freelance'];
                    const lowerText = containerText.toLowerCase();
                    for (const pattern of typePatterns) {
                        if (lowerText.includes(pattern)) {
                            jobType = pattern.split(/[\s-]+/).map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
                            break;
                        }
                    }

                    let datePosted = null;
                    const dateMatch = containerText.match(/(\d+)\s*(minute|hour|day|week|month)s?\s*ago/i);
                    if (dateMatch) {
                        datePosted = normalizePostedDateValue(dateMatch[0]);
                    } else if (lowerText.includes('today') || lowerText.includes('just')) {
                        datePosted = new Date().toISOString();
                    }

                    pushJob({
                        title,
                        company,
                        location,
                        salary,
                        job_type: jobType,
                        date_posted: datePosted,
                        url: jobUrl,
                        description_html: null,
                        description_text: null,
                    });
                } catch (err) {
                    log.debug('Error parsing job card:', err.message);
                }
            });

            // Primary selector: job links in h2 tags (Caterer.com structure)
            $('h2 a[href*="/job/"]').each((_, el) => {
                try {
                    const $link = $(el);
                    const href = $link.attr('href');

                    if (!href || !/\/job\/[^\/]+\/.*job\d+/i.test(href)) return;

                    const jobUrl = new URL(href, pageUrl).href;
                    const title = $link.text().trim();
                    if (!title || title.length < 3) return;

                    const $container = $link.closest('article, section, [class*="job"], [class*="vacancy"], li, div').first();
                    const containerText = $container.text().replace(/\s+/g, ' ').trim();

                    let company = null;
                    const companyLink = $container.find('a[href*="/jobs/"]').not($link).first();
                    if (companyLink.length) {
                        company = companyLink.text().trim();
                    }
                    if (!company) {
                        const companyMatch = containerText.match(/(?:at|by|with)\s+([A-Z][A-Za-z\s&]+?)(?:\s*[-–]|$)/);
                        if (companyMatch) company = companyMatch[1].trim();
                    }

                    let location = null;
                    const postcodeMatch = containerText.match(/[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}/i);
                    if (postcodeMatch) {
                        location = postcodeMatch[0].toUpperCase();
                    } else {
                        const cityMatch = containerText.match(/(?:in\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:,\s*[A-Z][a-z]+)?)/);
                        if (cityMatch) location = cityMatch[1].trim();
                    }

                    let salary = null;
                    const salaryMatch = containerText.match(/[\u00a3][\d,]+(?:\.\d{2})?(?:\s*[-–]\s*[\u00a3][\d,]+(?:\.\d{2})?)?(?:\s*(?:per|p\/|\/)\s*(?:hour|annum|year|day|week|month))?/i);
                    if (salaryMatch) salary = normalizeSalaryText(salaryMatch[0]);

                    let jobType = null;
                    const typePatterns = ['full time', 'full-time', 'part time', 'part-time', 'permanent', 'contract', 'temporary', 'freelance'];
                    const lowerText = containerText.toLowerCase();
                    for (const pattern of typePatterns) {
                        if (lowerText.includes(pattern)) {
                            jobType = pattern.split(/[\s-]+/).map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
                            break;
                        }
                    }

                    let datePosted = null;
                    const dateMatch = containerText.match(/(\d+)\s*(hour|day|week|month)s?\s*ago/i);
                    if (dateMatch) {
                        datePosted = normalizePostedDateValue(dateMatch[0]);
                    } else if (lowerText.includes('today') || lowerText.includes('just')) {
                        datePosted = new Date().toISOString();
                    }

                    pushJob({
                        title,
                        company,
                        location,
                        salary,
                        job_type: jobType,
                        date_posted: datePosted,
                        url: jobUrl,
                        description_html: null,
                        description_text: null,
                    });

                } catch (err) {
                    log.debug('Error parsing job element:', err.message);
                }
            });

            if (jobs.length === 0) {
                // Secondary selector: look for all job links (fallback)
                log.info('Primary selectors found 0 jobs, trying secondary selector...');
                $('a[href*="/job/"]').each((_, el) => {
                    try {
                        const $link = $(el);
                        const href = $link.attr('href');
                        if (!href || !/\/job\/[^\/]+\/.*job\d+/i.test(href)) return;

                        const jobUrl = new URL(href, pageUrl).href;
                        if (seenUrls.has(jobUrl)) return;

                        const title = $link.text().trim() || $link.attr('title') || '';
                        if (title.length < 3 || title.length > 200) return;

                        pushJob({
                            title,
                            company: null,
                            location: null,
                            salary: null,
                            job_type: null,
                            date_posted: null,
                            url: jobUrl,
                            description_html: null,
                            description_text: null,
                        });
                    } catch (err) {
                        log.debug('Error in secondary extraction:', err.message);
                    }
                });
            }

            return jobs;
        };

// Extract job details from detail page
        const extractJobDetails = ($, url) => {
            const details = {};
            
            // Try JSON-LD first (most reliable)
            $('script[type="application/ld+json"]').each((_, script) => {
                try {
                    const data = JSON.parse($(script).html() || '');
                    const items = Array.isArray(data) ? data : [data];
                    for (const item of items) {
                        if (item['@type'] === 'JobPosting') {
                            details.title = item.title || item.name;
                            details.company = item.hiringOrganization?.name;
                            details.date_posted = normalizePostedDateValue(item.datePosted);
                            details.description_html = item.description;
                            if (item.jobLocation?.address) {
                                const addr = item.jobLocation.address;
                                details.location = addr.addressLocality || addr.addressRegion || addr.streetAddress;
                            }
                            details.job_type = item.employmentType;
                            if (item.baseSalary) {
                                const sal = item.baseSalary;
                                if (sal.value) {
                                    const min = sal.value.minValue || sal.value;
                                    const max = sal.value.maxValue;
                                    const unit = sal.value.unitText || 'YEAR';
                                    details.salary = max ? `£${min} - £${max} per ${unit.toLowerCase()}` : `£${min} per ${unit.toLowerCase()}`;
                                }
                            }
                            break;
                        }
                    }
                } catch { /* ignore JSON parse errors */ }
            });
            
            // Fallback to HTML parsing
            if (!details.title) {
                details.title = $('h1').first().text().trim() || null;
            }
            
            if (!details.company) {
                const companySelectors = [
                    '[class*="company"]',
                    '[class*="employer"]', 
                    '[class*="recruiter"]',
                    '[data-qa="company"]',
                    '.company-name'
                ];
                for (const sel of companySelectors) {
                    const el = $(sel).first();
                    if (el.length && el.text().trim().length > 1) {
                        details.company = el.text().trim();
                        break;
                    }
                }
            }
            
            if (!details.description_html) {
                const descSelectors = [
                    '.job-description',
                    '[class*="description"]',
                    '[data-qa="job-description"]',
                    'article',
                    '.content'
                ];
                for (const sel of descSelectors) {
                    const el = $(sel).first();
                    if (el.length && el.text().length > 100) {
                        details.description_html = el.html();
                        details.description_text = el.text().replace(/\s+/g, ' ').trim();
                        break;
                    }
                }
            }
            
            if (!details.location) {
                const locSelectors = ['[class*="location"]', '[data-qa="location"]', '.location'];
                for (const sel of locSelectors) {
                    const el = $(sel).first();
                    if (el.length && el.text().trim().length > 1) {
                        details.location = el.text().trim();
                        break;
                    }
                }
            }
            
            if (!details.salary) {
                const salarySelectors = ['[class*="salary"]', '[data-qa="salary"]', '.salary', '.wage'];
                for (const sel of salarySelectors) {
                    const el = $(sel).first();
                    if (el.length) {
                        details.salary = el.text().trim();
                        break;
                    }
                }
            }
            
            // Extract job type from page text if not found
            if (!details.job_type) {
                const bodyText = $('body').text().toLowerCase();
                const types = [
                    { pattern: 'full time', display: 'Full Time' },
                    { pattern: 'full-time', display: 'Full Time' },
                    { pattern: 'part time', display: 'Part Time' },
                    { pattern: 'part-time', display: 'Part Time' },
                    { pattern: 'permanent', display: 'Permanent' },
                    { pattern: 'contract', display: 'Contract' },
                    { pattern: 'temporary', display: 'Temporary' },
                ];
                for (const t of types) {
                    if (bodyText.includes(t.pattern)) {
                        details.job_type = t.display;
                        break;
                    }
                }
            }
            
            return details;
        };

        // Find next page URL
        const findNextPage = ($, currentUrl) => {
            // Look for "Next" link
            const nextSelectors = [
                'a:contains("Next")',
                'a:contains("next")',
                'a[rel="next"]',
                '[class*="next"] a',
                'a[aria-label*="next"]',
            ];
            
            for (const sel of nextSelectors) {
                const nextLink = $(sel).first().attr('href');
                if (nextLink && nextLink !== '#') {
                    return new URL(nextLink, currentUrl).href;
                }
            }
            
            // Try incrementing page number
            const url = new URL(currentUrl);
            const currentPage = parseInt(url.searchParams.get('page') || '1');
            const nextPageNum = currentPage + 1;
            
            // Check if next page link exists
            const nextPageLink = $(`a[href*="page=${nextPageNum}"]`).first().attr('href');
            if (nextPageLink) {
                return new URL(nextPageLink, currentUrl).href;
            }
            
            // Check pagination numbers
            const pageLinks = $('a[href*="page="]');
            let hasNextPage = false;
            pageLinks.each((_, el) => {
                const href = $(el).attr('href');
                const match = href?.match(/page=(\d+)/);
                if (match && parseInt(match[1]) === nextPageNum) {
                    hasNextPage = true;
                }
            });
            
            if (hasNextPage) {
                url.searchParams.set('page', String(nextPageNum));
                return url.href;
            }
            
            return null;
        };

        // Save job to dataset
        const saveJob = async (job, source) => {
            if (!job || !job.url) return false;
            if (saved >= RESULTS_WANTED) return false;
            if (pushedUrls.has(job.url)) return false;
            
            if (!shouldKeepByRecency(job.date_posted)) {
                log.debug('Skipped due to recency filter:', { url: job.url });
                return false;
            }
            
            await Dataset.pushData(job);
            pushedUrls.add(job.url);
            saved++;
            stats.jobsSaved++;
            
            log.info(`✓ Saved job ${saved}/${RESULTS_WANTED} (${source}):`, { 
                title: job.title?.slice(0, 50),
                company: job.company?.slice(0, 30)
            });
            
            return true;
        };

        // ============ MAIN SCRAPING LOGIC ============
        const initialUrl = startUrl || buildSearchUrl(keyword, location);
        let currentUrl = initialUrl;
        let pageNum = 1;

        log.info('🚀 Starting Caterer.com scraper:', { 
            startUrl: initialUrl,
            target: RESULTS_WANTED,
            maxPages: MAX_PAGES,
            useProxy
        });

        // Warm up with homepage (optional - won't fail if unsuccessful)
        log.info('🔄 Warming up...');
        const testHtml = await makeRequest('https://www.caterer.com/', 'https://www.google.co.uk/', 2, true);
        if (testHtml) {
            log.info('✓ Homepage loaded', { length: testHtml.length });
        } else {
            log.warning('⚠ Homepage request failed, will proceed anyway');
        }
        await humanDelay(2000, 4000);

        // Process list pages
        while (saved < RESULTS_WANTED && pageNum <= MAX_PAGES) {
            log.info(`📄 Processing page ${pageNum}/${MAX_PAGES}:`, { 
                url: currentUrl.slice(0, 80), 
                saved, 
                target: RESULTS_WANTED 
            });
            
            // Human-like delay between pages (longer for subsequent pages)
            if (pageNum > 1) {
                await humanDelay(5000, 10000);
            }
            
            const referer = pageNum === 1 ? 'https://www.caterer.com/' : initialUrl;
            let html = await makeRequest(currentUrl, referer, 6, true);
            
            if (!html) {
                log.warning(`❌ Failed to fetch page ${pageNum}, will retry after delay`);
                // Try once more with even longer delay and definitely no proxy
                await humanDelay(20000, 30000);
                useProxy = false; // Force disable proxy for retry
                html = await makeRequest(currentUrl, referer, 6, true);
                if (!html) {
                    // If this is the first page and we can't get it, try alternative URL
                    if (pageNum === 1) {
                        log.warning('Trying alternative approach: direct jobs page');
                        const altUrl = 'https://www.caterer.com/jobs';
                        await humanDelay(10000, 15000);
                        html = await makeRequest(altUrl, 'https://www.google.co.uk/', 6, true);
                        if (html) {
                            currentUrl = altUrl;
                        }
                    }
                    
                    if (!html) {
                        log.error('Unable to fetch page after all attempts');
                        if (pageNum === 1) {
                            log.error('Could not fetch first page - stopping');
                            break;
                        } else {
                            log.warning('Skipping to next page');
                            pageNum++;
                            continue;
                        }
                    }
                }
            }
            
            // Check if we got actual content
            if (html.length < 5000) {
                log.warning('Response too short, might be blocked:', { length: html.length });
                log.debug('Response preview:', html.slice(0, 500));
            }
            
            const $ = cheerioLoad(html || '');
            stats.pagesProcessed++;
            
            // Debug: Log page info
            const pageTitle = $('title').text();
            log.info('Page loaded:', {
                title: pageTitle.slice(0, 80),
                htmlLength: (html || '').length,
                hasJobsText: (html || '').includes('/job/')
            });
            
            // Extract jobs from list
            const jobs = extractJobsFromList($, currentUrl);
            stats.jobsFound += jobs.length;

            if (debugSaveHtml && pageNum === 1 && (jobs.length === 0 || html.length < 5000)) {
                await Actor.setValue('DEBUG_LIST_PAGE', { url: currentUrl, html: html.slice(0, 200000) });
            }
            
            log.info(`📋 Found ${jobs.length} jobs on page ${pageNum}`);
            
            if (jobs.length === 0) {
                // Check if we got a valid page
                const title = $('title').text();
                const hasJobContent = $('[class*="job"], [class*="vacancy"]').length > 0;
                
                log.warning('No jobs extracted:', {
                    pageTitle: title.slice(0, 60),
                    hasJobContent,
                    bodyLength: $.html().length
                });
                
                if (!hasJobContent && pageNum > 1) {
                    log.info('Likely reached end of results');
                    break;
                }
            }
            
            // Process each job
            for (const job of jobs) {
                if (saved >= RESULTS_WANTED) break;
                
                if (collectDetails && job.url) {
                    // Fetch detail page for richer data
                    await humanDelay(2500, 5000);
                    
                    const detailHtml = await makeRequest(job.url, currentUrl, 4, true);
                    
                    if (detailHtml) {
                        const $detail = cheerioLoad(detailHtml);
                        const details = extractJobDetails($detail, job.url);
                        stats.detailPagesProcessed++;
                        
                        // Merge details with list data (detail page takes precedence)
                        const enrichedJob = {
                            title: details.title || job.title,
                            company: details.company || job.company,
                            location: details.location || job.location,
                            salary: details.salary || job.salary,
                            job_type: details.job_type || job.job_type,
                            date_posted: details.date_posted || job.date_posted,
                            description_html: details.description_html || job.description_html,
                            description_text: details.description_text || job.description_text,
                            url: job.url,
                        };
                        
                        await saveJob(enrichedJob, 'DETAIL');
                    } else {
                        // Couldn't get details, save with list data
                        await saveJob(job, 'LIST_ONLY');
                    }
                } else {
                    await saveJob(job, 'LIST');
                }
            }
            
            // Find next page
            const nextUrl = findNextPage($, currentUrl);
            
            if (!nextUrl) {
                log.info('📍 No more pages found');
                break;
            }
            
            if (saved >= RESULTS_WANTED) {
                log.info('🎯 Target reached!');
                break;
            }
            
            currentUrl = nextUrl;
            pageNum++;
        }

        // Final stats
        stats.finalSaved = saved;
        
        log.info('✅ Scraping completed!', {
            jobsSaved: saved,
            pagesProcessed: stats.pagesProcessed,
            detailPages: stats.detailPagesProcessed,
            blockedResponses: stats.blockedResponses,
            requestsFailed: stats.requestsFailed,
            jobsFound: stats.jobsFound,
        });
        
        await Actor.setValue('RUN_STATS', stats);
        
        if (saved === 0) {
            log.warning('⚠ No jobs extracted. Possible causes: site blocking, selector changes, or no matching jobs.');
        }
        
    } catch (error) {
        log.error('💥 Fatal error:', {
            name: error?.name,
            message: error?.message,
            stack: error?.stack?.slice(0, 500),
        });
        throw error;
    } finally {
        await Actor.exit();
    }
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});
