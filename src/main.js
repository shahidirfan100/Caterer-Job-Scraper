// Caterer.com jobs scraper - Production-ready implementation
// Stack: Apify + Crawlee + CheerioCrawler + gotScraping + header-generator
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import fs from 'fs/promises';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
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
const normalizePostedWithin = (value) => {
    const allowed = new Set(['any', '24h', '7d', '30d']);
    if (!value || typeof value !== 'string') return 'any';
    const trimmed = value.trim().toLowerCase();
    return allowed.has(trimmed) ? trimmed : 'any';
};
const parsePostedDate = (value) => {
    if (!value && value !== 0) return null;
    if (value instanceof Date) return value;
    if (typeof value === 'number' && Number.isFinite(value)) {
        return new Date(value);
    }
    const text = String(value).trim();
    if (!text) return null;
    const parsed = Date.parse(text);
    if (!Number.isNaN(parsed)) {
        return new Date(parsed);
    }
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
        if (multiplier) {
            return new Date(Date.now() - count * multiplier);
        }
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

// Try to import header-generator, fallback if not available
let HeaderGenerator;
try {
    const hg = await import('header-generator');
    HeaderGenerator = hg.default;
} catch (error) {
    log.warning('header-generator not available, using fallback headers:', error.message);
    HeaderGenerator = null;
}
let headerGeneratorInstance;
const initHeaderGenerator = () => {
    if (!HeaderGenerator || headerGeneratorInstance) return;
    try {
        headerGeneratorInstance = new HeaderGenerator({
            browsers: ['chrome', 'firefox'],
            operatingSystems: ['windows', 'macos', 'linux'],
            devices: ['desktop'],
        });
        log.info('HeaderGenerator initialized');
    } catch (error) {
        log.warning('HeaderGenerator initialization failed, will use fallback headers:', error.message);
        headerGeneratorInstance = null;
    }
};

// Single-entrypoint main
// Install useful global handlers to capture unexpected errors during runtime
process.on('unhandledRejection', (reason, p) => {
    log && log.error && log.error('Unhandled Rejection at:', { reason, promise: p });
    console.error('Unhandled Rejection at:', reason);
});
process.on('uncaughtException', (err) => {
    log && log.error && log.error('Uncaught Exception thrown:', err.stack || err);
    console.error('Uncaught Exception thrown:', err.stack || err);
});

async function main() {
    try {
        // Initialize Actor inside main so initialization errors are captured by main() try/catch
        try {
            await Actor.init();
            log.info('Actor.init() succeeded');
        } catch (initErr) {
            // Provide more actionable diagnostics when Actor.init() fails with ArgumentError
            log.error('Actor.init() failed:', { name: initErr.name, message: initErr.message, stack: initErr.stack, validationErrors: initErr.validationErrors });
            // If this appears to be an input validation problem, log the raw environment input if available
            try {
                if (process.env.APIFY_INPUT) {
                    log.warning('APIFY_INPUT env var present; logging its type and truncated content');
                    const raw = String(process.env.APIFY_INPUT);
                    log.warning('APIFY_INPUT (truncated 1k):', raw.slice(0, 1024));
                }
            } catch (envErr) { /* ignore env logging errors */ }
            // Re-throw so outer catch still handles termination, but with richer logs
            throw initErr;
        }
        let input;
        try {
            input = await Actor.getInput();
            log.info('Actor.getInput() succeeded');
        } catch (error) {
            log.error('Error in Actor.getInput():', error);
            log.error('Error details:', { name: error.name, message: error.message, validationErrors: error.validationErrors });
            if (error && error.name === 'ArgumentError') {
                // Try to fall back to a local INPUT.json if present (useful for local runs or malformed platform input)
                try {
                    const raw = await fs.readFile(new URL('../INPUT.json', import.meta.url));
                    input = JSON.parse(String(raw));
                    log.warning('Loaded fallback INPUT.json from repository root');
                } catch (fsErr) {
                    log.warning('Could not read fallback INPUT.json, using empty input instead:', fsErr.message || fsErr);
                    input = {};
                }
            } else {
                throw error;
            }
        }
        input = input || {};
        log.info('Raw input received:', input);
        // Defensive defaults and type-casting for all fields
        const safeInt = (v, def) => (Number.isFinite(+v) && +v > 0 ? +v : def);
        const safeBool = (v, def) => (typeof v === 'boolean' ? v : def);
        const safeStr = (v, def) => (typeof v === 'string' ? v : def);
        const safeObj = (v, def) => (v && typeof v === 'object' && !Array.isArray(v) ? v : def);

        const keyword = safeStr(input.keyword, '');
        const location = safeStr(input.location, '');
        const category = safeStr(input.category, '');
        const results_wanted = safeInt(input.results_wanted, 100);
        const max_pages = safeInt(input.max_pages, 20);
        const collectDetails = safeBool(input.collectDetails, true);
        const startUrls = Array.isArray(input.startUrls) ? input.startUrls : undefined;
        const proxyConfiguration = safeObj(input.proxyConfiguration, undefined);
        const postedWithinInput = safeStr(input.postedWithin, 'any');

        // Defensive input validation and logging
        if (typeof input !== 'object' || Array.isArray(input)) {
            log.error('Input must be a JSON object. Received:', input);
            throw new Error('INPUT_ERROR: Input must be a JSON object.');
        }

        const RESULTS_WANTED = Number.isFinite(+results_wanted) ? Math.max(1, +results_wanted) : Number.MAX_SAFE_INTEGER;
        const MAX_PAGES = Number.isFinite(+max_pages) ? Math.max(1, +max_pages) : Math.max(20, Math.ceil(RESULTS_WANTED / 5));
        postedWithinLabel = normalizePostedWithin(postedWithinInput);
        recencyWindowMs = RECENCY_WINDOWS[postedWithinLabel] || null;
        
        log.info('Starting Caterer.com Job Scraper', { 
            keyword, 
            location, 
            category, 
            results_wanted: RESULTS_WANTED, 
            max_pages: MAX_PAGES,
            collect_details: collectDetails,
            posted_within: postedWithinLabel,
        });

        initHeaderGenerator();
        let fallbackHeaderHits = 0;
        
        // Multiple realistic user agent strings for rotation
        const userAgents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
        ];
        
        // Dynamic header generation for anti-bot evasion with consistent fingerprints
        const getHeaders = () => {
            if (headerGeneratorInstance) {
                try {
                    const headers = headerGeneratorInstance.getHeaders();
                    // Ensure no bot-identifying headers
                    delete headers['DNT'];
                    delete headers['dnt'];
                    return headers;
                } catch (error) {
                    log.warning('HeaderGenerator getHeaders failed, using fallback headers:', error.message);
                }
            }
            fallbackHeaderHits += 1;
            
            // Rotate user agent on fallback
            const ua = userAgents[fallbackHeaderHits % userAgents.length];
            const isChrome = ua.includes('Chrome') && !ua.includes('Firefox');
            
            const baseHeaders = {
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
            };
            
            // Add Chrome-specific client hints only for Chrome UA
            if (isChrome) {
                baseHeaders['sec-ch-ua'] = '"Chromium";v="131", "Google Chrome";v="131", "Not_A Brand";v="24"';
                baseHeaders['sec-ch-ua-mobile'] = '?0';
                baseHeaders['sec-ch-ua-platform'] = ua.includes('Mac') ? '"macOS"' : '"Windows"';
            }
            
            return baseHeaders;
        };

        const toAbs = (href, base = 'https://www.caterer.com') => {
            try { return new URL(href, base).href; } catch { return null; }
        };
        const normalizeJobUrl = (href, base) => {
            const abs = toAbs(href, base);
            if (!abs) return null;
            try {
                const urlObj = new URL(abs);
                urlObj.hash = '';
                const removable = [];
                urlObj.searchParams.forEach((_, key) => {
                    if (/^(utm_|wt\.|icid|tracking)/i.test(key)) removable.push(key);
                });
                removable.forEach((key) => urlObj.searchParams.delete(key));
                return urlObj.href;
            } catch {
                return abs;
            }
        };

        const cleanText = (html) => {
            if (!html) return '';
            const $ = cheerioLoad(html);
            $('script, style, noscript, iframe').remove();
            return $.root().text().replace(/\s+/g, ' ').trim();
        };
        const normalizeText = (text) => (text || '').replace(/\s+/g, ' ').trim();
        const extractJobTypeFromPage = ($) => {
            if (!$) return null;
            const keywords = ['job type', 'employment type', 'contract type'];
            const candidateSelectors = [
                'li',
                '.job-summary__item',
                '.job-details__item',
                '.job-info li',
                '.job-card__meta li',
                '.job-meta li',
                '[class*="summary"] li',
            ];
            for (const selector of candidateSelectors) {
                const items = $(selector);
                if (!items.length) continue;
                for (let i = 0; i < items.length; i++) {
                    const el = items[i];
                    const $el = $(el);
                    const rawText = normalizeText($el.text());
                    if (!rawText) continue;
                    const lower = rawText.toLowerCase();
                    if (!keywords.some((keyword) => lower.includes(keyword))) continue;
                    const spans = $el.find('span');
                    if (spans.length > 1) {
                        const candidate = normalizeText($(spans[spans.length - 1]).text());
                        if (candidate && !keywords.some((keyword) => candidate.toLowerCase().includes(keyword))) {
                            return candidate;
                        }
                    }
                    const cleaned = keywords.reduce((acc, keyword) => acc.replace(new RegExp(keyword, 'ig'), ''), rawText)
                        .replace(/[:\-]/g, '')
                        .trim();
                    if (cleaned) return cleaned;
                }
            }
            const schemaNode = $('[itemprop="employmentType"], meta[itemprop="employmentType"]').first();
            if (schemaNode.length) {
                const value = schemaNode.attr('content') || schemaNode.text();
                if (value) return normalizeText(value);
            }
            const dataTestId = $('[data-testid*="employment-type"], [data-testid*="job-type"]').first().text();
            if (dataTestId) return normalizeText(dataTestId);
            return null;
        };

        const buildStartUrl = (kw, loc, cat) => {
            if (cat) {
                let url = `https://www.caterer.com/jobs/${encodeURIComponent(cat.toLowerCase())}`;
                if (loc) url += `?location=${encodeURIComponent(loc)}`;
                return url;
            } else {
                // Caterer.com uses /jobs/search for search results
                const u = new URL('https://www.caterer.com/jobs/search');
                if (kw) u.searchParams.set('keywords', String(kw).trim());
                if (loc) u.searchParams.set('location', String(loc).trim());
                return u.href;
            }
        };

        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) initial.push(...startUrls);
        if (!initial.length) initial.push(buildStartUrl(keyword, location, category));

        // Defensive proxyConfiguration handling
        let proxyConf = undefined;
        try {
            const useDefaultProxy = !proxyConfiguration || Object.keys(proxyConfiguration).length === 0;
            const proxyOptions = useDefaultProxy
                ? { useApifyProxy: true, apifyProxyGroups: ['DATACENTER'] }
                : proxyConfiguration;
            proxyConf = await Actor.createProxyConfiguration(proxyOptions);
            log.info('Proxy configuration ready', { defaultProxy: useDefaultProxy });
        } catch (e) {
            log.warning('Failed to create proxy configuration, continuing without proxy (may reduce success rate):', e.message);
            proxyConf = undefined;
        }

        let saved = 0;
        const pushedUrls = new Set();
        const pendingListings = new Map();
        const queuedDetailUrls = new Set();
        const stats = {
            listPagesProcessed: 0,
            detailPagesProcessed: 0,
            blockedResponses: 0,
            duplicateJobsSkipped: 0,
            detailPagesEnqueued: 0,
            listPagesEnqueued: initial.length,
            pendingListingFlushes: 0,
            recencyFiltered: 0,
        };
        const passesRecency = (dateValue, url, logger = log) => {
            if (shouldKeepByRecency(dateValue)) return true;
            stats.recencyFiltered += 1;
            logger.info('Skipping job due to postedWithin filter', { url, date: dateValue, postedWithin: postedWithinLabel });
            return false;
        };

        const pushJob = async (job, sourceLabel) => {
            if (!job || !job.url) return false;
            if (saved >= RESULTS_WANTED) return false;
            if (pushedUrls.has(job.url)) {
                stats.duplicateJobsSkipped += 1;
                return false;
            }
            if (!passesRecency(job.date_posted, job.url)) return false;
            await Dataset.pushData(job);
            pushedUrls.add(job.url);
            saved += 1;
            log.info(`✓ Saved (${sourceLabel}) Total: ${saved}/${RESULTS_WANTED}`, { url: job.url });
            return true;
        };

        function extractFromJsonLd($) {
            const scripts = $('script[type="application/ld+json"]');
            for (let i = 0; i < scripts.length; i++) {
                try {
                    const parsed = JSON.parse($(scripts[i]).html() || '');
                    const arr = Array.isArray(parsed) ? parsed : [parsed];
                    for (const e of arr) {
                        if (!e) continue;
                        const t = e['@type'] || e.type;
                        if (t === 'JobPosting' || (Array.isArray(t) && t.includes('JobPosting'))) {
                            return {
                                title: e.title || e.name || null,
                                company: e.hiringOrganization?.name || null,
                                date_posted: normalizePostedDateValue(e.datePosted || e.datepublished || e.date),
                                description_html: e.description || null,
                                location: (e.jobLocation && e.jobLocation.address && (e.jobLocation.address.addressLocality || e.jobLocation.address.addressRegion)) || null,
                            };
                        }
                    }
                } catch (e) { /* ignore parsing errors */ }
            }
            return null;
        }

        function findJobLinks($, base) {
            const links = new Set();
            // Caterer.com job URLs follow pattern: /job/{title-slug}/{company-slug}-job{id}
            // Example: https://www.caterer.com/job/bar-staff/search-job106117278
            $('a[href]').each((_, a) => {
                const href = $(a).attr('href');
                if (!href) return;
                
                // Match actual job posting URLs, not location/category links
                // Job URLs: /job/{slug}/{company}-job{number}
                // Exclude: /jobs/search/in-{location}, /jobs/{category}, /jobs?{params}
                if (/^\/job\/[^\/]+\/[^\/]+-job\d+$|^\/job\/[a-z0-9\-]+\/[a-z0-9\-]+-job\d+$/i.test(href)) {
                    const abs = normalizeJobUrl(href, base);
                    if (abs && !abs.includes('#')) {
                        links.add(abs);
                    }
                }
            });
            return [...links];
        }

        function findNextPage($, base) {
            // Caterer.com uses ?page=N pagination (seen in fetched HTML: ?page=2, ?page=3, etc.)
            const nextLink = $('a[href*="page="]').filter((_, el) => {
                const text = $(el).text().trim().toLowerCase();
                return text === 'next' || text.includes('next');
            }).first().attr('href');
            
            if (nextLink) return toAbs(nextLink, base);
            
            // Fallback: try to find current page and calculate next
            const currentUrl = new URL(base);
            const currentPage = parseInt(currentUrl.searchParams.get('page') || '1');
            
            // Check if a link to next page number exists
            const nextPageNum = currentPage + 1;
            const nextPageLink = $(`a[href*="page=${nextPageNum}"]`).first().attr('href');
            if (nextPageLink) return toAbs(nextPageLink, base);
            
            return null;
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 6,
            useSessionPool: true,
            minConcurrency: 2,
            maxConcurrency: 5,
            autoscaledPoolOptions: {
                desiredConcurrency: 3,
                maxConcurrency: 5,
                minConcurrency: 2,
            },
            requestHandlerTimeoutSecs: 120,
            navigationTimeoutSecs: 90,
            sessionPoolOptions: {
                maxPoolSize: 80,
                sessionOptions: {
                    maxUsageCount: 6,
                    maxAgeSecs: 400,
                },
            },
            persistCookiesPerSession: false,
            
            // Force HTTP/1.1 to avoid HTTP/2 stream errors
            additionalMimeTypes: ['application/json', 'text/plain'],
            suggestResponseEncoding: 'utf-8',
            
            // Enhanced got-scraping options for better connection handling
            requestOptions: {
                http2: false,
                timeout: {
                    request: 90000,
                    connect: 20000,
                    secureConnect: 20000,
                },
                retry: {
                    limit: 0, // Let Crawlee handle retries
                },
            },
            
            // Stealth headers and throttling handled in hooks
            preNavigationHooks: [
                async ({ request, session, log: crawlerLog }) => {
                    // Optimized delays: faster for detail pages, slower for list/pagination
                    const label = request.userData?.label || 'LIST';
                    const isListPage = label === 'LIST';
                    
                    // List pages need more caution, detail pages can be faster
                    const baseDelay = isListPage 
                        ? 800 + Math.random() * 1200  // 0.8-2s for list pages
                        : 400 + Math.random() * 600;   // 0.4-1s for detail pages
                    
                    await sleep(baseDelay);
                    
                    const headers = getHeaders();
                    const referer = request.userData?.referrer || 'https://www.caterer.com/';
                    const fetchSite = referer.includes('caterer.com') ? 'same-origin' : 'same-site';
                    
                    // Enhanced headers with better fingerprinting consistency
                    request.headers = {
                        ...headers,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Referer': referer,
                        'Sec-Fetch-Site': fetchSite,
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-User': '?1',
                        'Sec-Fetch-Dest': 'document',
                        'Upgrade-Insecure-Requests': '1',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive',
                    };
                    
                    // Remove DNT and bot-identifying headers
                    delete request.headers['DNT'];
                    delete request.headers['dnt'];
                    
                    // Force HTTP/1.1 to avoid HTTP/2 stream errors
                    if (request.options) {
                        request.options.http2 = false;
                    }
                    
                    // Retire sessions more aggressively to rotate fingerprints
                    if (session && session.usageCount >= 4) {
                        session.retire();
                        crawlerLog.debug('Retiring session proactively after 4 uses');
                    }
                },
            ],

            async errorHandler({ request, error, session, log: crawlerLog }) {
                const retries = request.retryCount ?? 0;
                const message = error?.message || '';
                const statusCode = error?.statusCode || error?.status;
                
                // Retire session on any error
                if (session) {
                    session.retire();
                }
                
                // Identify error types
                const isHttp2Error = /nghttp2|http2|stream closed/i.test(message);
                const isBlockedError = [403, 429].includes(Number(statusCode)) || /blocked|denied/i.test(message);
                const isNetworkError = /ECONNRESET|ETIMEDOUT|ENOTFOUND|socket hang up/i.test(message);
                
                // Optimized exponential backoff - faster initial retries
                let baseWait = Math.min(30000, (2 ** Math.min(retries, 6)) * 600);
                
                // Apply multipliers based on error type
                if (isBlockedError) {
                    baseWait *= 2;
                    crawlerLog.warning('Detected blocking (403/429), applying aggressive backoff', {
                        url: request.url,
                        statusCode,
                        waitMs: baseWait,
                        retryCount: retries,
                    });
                } else if (isHttp2Error) {
                    baseWait *= 1.5;
                    crawlerLog.warning('HTTP/2 error detected, backing off and forcing HTTP/1.1', {
                        url: request.url,
                        message,
                        waitMs: baseWait,
                        retryCount: retries,
                    });
                    // Force HTTP/1.1 for retry
                    if (request.options) {
                        request.options.http2 = false;
                    }
                } else if (isNetworkError) {
                    baseWait *= 1.2;
                    crawlerLog.warning('Network error, applying moderate backoff', {
                        url: request.url,
                        message,
                        waitMs: baseWait,
                        retryCount: retries,
                    });
                }
                
                // Add random jitter (±20%)
                const jitter = baseWait * (0.8 + Math.random() * 0.4);
                const waitMs = Math.min(45000, jitter);
                
                crawlerLog.info(`Waiting ${Math.round(waitMs)}ms before retry ${retries + 1}/6`);
                await sleep(waitMs);
            },
            async failedRequestHandler({ request, error }, { session }) {
                const message = error?.message || '';
                const statusCode = error?.statusCode || error?.status;
                
                // Log the failure with full context
                log.warning(`Request permanently failed after ${request.retryCount} retries (gracefully continuing)`, { 
                    url: request.url,
                    error: message,
                    statusCode,
                    label: request.userData?.label,
                });
                
                // Retire session
                if (session) session.retire();
                
                // Don't throw - let the scraper continue with other URLs
                // This prevents the entire run from failing due to a few problematic URLs
            },
            async requestHandler({ request, $, enqueueLinks, log: crawlerLog, response, session }) {
                const label = request.userData?.label || 'LIST';
                
                // Optimized reading delay based on page type
                const readingDelay = label === 'DETAIL' 
                    ? 300 + Math.random() * 500  // 0.3-0.8s for detail pages
                    : 400 + Math.random() * 800; // 0.4-1.2s for list pages
                await sleep(readingDelay);
                
                if (saved >= RESULTS_WANTED) {
                    crawlerLog.info('Results limit reached, skipping further processing');
                    return;
                }
                const pageNo = request.userData?.pageNo || 1;
                const statusCode = response?.statusCode ?? response?.status;
                
                // Enhanced blocking detection
                if (statusCode && [403, 429, 503].includes(Number(statusCode))) {
                    stats.blockedResponses += 1;
                    crawlerLog.warning(`Blocked with status ${statusCode} on ${request.url}, retiring session and skipping`);
                    if (session) {
                        session.retire();
                    }
                    // Moderate delay before continuing
                    await sleep(3000 + Math.random() * 3000);
                    return;
                }
                
                const pageTitle = typeof $ === 'function' ? $('title').first().text().toLowerCase() : '';
                const bodyText = typeof $ === 'function' ? $('body').first().text().toLowerCase() : '';
                
                // More comprehensive blocking detection
                if ($ && (
                    pageTitle.includes('access denied') || 
                    pageTitle.includes('temporarily blocked') ||
                    pageTitle.includes('captcha') ||
                    bodyText.includes('please verify you are human') ||
                    bodyText.includes('unusual traffic')
                )) {
                    stats.blockedResponses += 1;
                    crawlerLog.warning(`Detected access denial content on ${request.url}, skipping page`);
                    if (session) {
                        session.retire();
                    }
                    await sleep(3000 + Math.random() * 3000);
                    return;
                }

                if (label === 'LIST') {
                    stats.listPagesProcessed += 1;
                    crawlerLog.info(`Processing LIST page ${pageNo}: ${request.url}`);
                    
                    // Extract jobs directly from listing page
                    const jobs = [];
                    
                    // Find all job links in h2 tags - more reliable selector
                    const jobElements = $('a[href]').filter((_, el) => {
                        const href = $(el).attr('href');
                        return href && /\/job\/[^\/]+\/[^\/]+-job\d+/i.test(href);
                    });
                    crawlerLog.info(`Found ${jobElements.length} potential job links`);
                    
                    jobElements.each((idx, el) => {
                        try {
                            const $link = $(el);
                            const href = $link.attr('href');
                            
                            // Validate it's a proper job URL
                            if (!href || !/\/job\/[^\/]+\/[^\/]+-job\d+/i.test(href)) {
                                return;
                            }
                            
                            const jobUrl = normalizeJobUrl(href, request.url);
                            if (!jobUrl) return;
                            const title = $link.text().trim() || $jobContainer.find('h2, h3, .job-title, [class*="title"]').first().text().trim() || $link.attr('title') || 'Job Posting';
                            
                            // Try to find the parent container for this job to extract other details
                            const $jobContainer = $link.closest('article, section, .vacancy, [class*="job-item"], div[role="article"]').first();
                            
                            // Extract company - usually appears after the title in the listing
                            let company = null;
                            const companyLink = $jobContainer.find('a[href*="/jobs/"]').not($link).first();
                            if (companyLink.length) {
                                company = companyLink.text().trim();
                            } else {
                                // Fallback: look for company text patterns
                                const containerText = $jobContainer.text();
                                const companyMatch = containerText.match(/(?:by|company|employer):\s*([^\n]+)/i);
                                if (companyMatch) company = companyMatch[1].trim();
                            }
                            
                            // Extract location - typically in the job container
                            const locationText = $jobContainer.text();
                            let location = null;
                            
                            // UK postcode pattern or city patterns
                            const locationMatch = locationText.match(/([A-Z]{1,2}\d{1,2}\s?[A-Z]{2}|[A-Z][a-z]+(?:\s[A-Z][a-z]+)*(?:,\s*[A-Z]{2,})?)/);
                            if (locationMatch) {
                                location = locationMatch[0].trim();
                            }
                            
                            // Extract salary - look for £ signs with better pattern
                            let salary = null;
                            const salaryMatch = locationText.match(
                                /(?:£[\d,]+(?:\.\d{2})?(?:\s*-\s*£[\d,]+(?:\.\d{2})?)?|Up to £[\d,]+(?:\.\d{2})?)\s*per\s*(?:hour|annum|day|year|week)/i
                            );
                            if (salaryMatch) {
                                salary = salaryMatch[0].trim();
                            }
                            
                            // Extract date posted with improved pattern
                            let datePosted = null;
                            const dateMatch = locationText.match(
                                /(?:posted\s)?(?:(\d+)\s*(?:hours?|days?|weeks?|months?)\s*ago|NEW|FEATURED|\d+\s*ago)/i
                            );
                            if (dateMatch) {
                                datePosted = normalizePostedDateValue(dateMatch[0]);
                            }
                            
                            if (title && jobUrl) {
                                if (!passesRecency(datePosted, jobUrl, crawlerLog)) {
                                    return;
                                }
                                jobs.push({
                                    title,
                                    company,
                                    location,
                                    salary,
                                    job_type: null,
                                    date_posted: datePosted,
                                    description_html: null,
                                    description_text: null,
                                    url: jobUrl,
                                });
                            }
                        } catch (err) {
                            crawlerLog.warning(`Error parsing job element: ${err.message}`);
                        }
                    });
                    
                    crawlerLog.info(`Extracted ${jobs.length} valid jobs from ${request.url}`);
                    
                    if (jobs.length > 0) {
                        const remaining = Math.max(0, RESULTS_WANTED - saved);
                        for (const job of jobs) {
                            if (!job || !job.url) continue;
                            if (collectDetails) {
                                if (!pendingListings.has(job.url)) {
                                    pendingListings.set(job.url, job);
                                }
                            } else if (saved < RESULTS_WANTED) {
                                await pushJob(job, 'LIST');
                                if (saved >= RESULTS_WANTED) break;
                            }
                        }
                        crawlerLog.info(`Buffered ${collectDetails ? 'listing stubs' : 'jobs pushed'} from LIST page`, { buffered: jobs.length, pendingListings: pendingListings.size });
                    }

                    // Optionally enqueue detail pages if requested
                    if (collectDetails && saved < RESULTS_WANTED) {
                        const detailCandidates = new Set();
                        for (const job of jobs) {
                            if (job?.url) detailCandidates.add(job.url);
                        }
                        for (const link of findJobLinks($, request.url)) {
                            detailCandidates.add(link);
                        }
                        const remainingDetails = Math.max(0, RESULTS_WANTED - saved);
                        const toEnqueue = [];
                        for (const url of detailCandidates) {
                            if (!url || pushedUrls.has(url) || queuedDetailUrls.has(url)) continue;
                            toEnqueue.push(url);
                            queuedDetailUrls.add(url);
                            if (toEnqueue.length >= remainingDetails) break;
                        }
                        if (toEnqueue.length > 0) {
                            crawlerLog.info(`Enqueueing ${toEnqueue.length} detail pages`);
                            await enqueueLinks({ 
                                urls: toEnqueue, 
                                userData: { label: 'DETAIL', referrer: request.url }
                            });
                            stats.detailPagesEnqueued += toEnqueue.length;
                        }
                    }

                    // Handle pagination with optimized rate limiting
                    if (saved < RESULTS_WANTED && pageNo < MAX_PAGES) {
                        const next = findNextPage($, request.url);
                        if (next) {
                            crawlerLog.info(`Enqueueing next page (${pageNo + 1}): ${next}`);
                            await enqueueLinks({ 
                                urls: [next], 
                                userData: { label: 'LIST', pageNo: pageNo + 1, referrer: request.url }
                            });
                            stats.listPagesEnqueued += 1;
                        } else {
                            crawlerLog.info('No next page found - pagination complete');
                        }
                    }
                    
                    // Moderate delay after list page - pagination will have pre-navigation delay
                    await sleep(800 + Math.random() * 1200);
                    return;
                }

                if (label === 'DETAIL') {
                    stats.detailPagesProcessed += 1;
                    if (saved >= RESULTS_WANTED) {
                        crawlerLog.info('Results limit reached, skipping detail page');
                        await sleep((Math.random() * 0.3 + 0.2) * 1000);
                        return;
                    }
                    
                    try {
                        crawlerLog.info(`Processing DETAIL page: ${request.url}`);
                        
                        const listingStub = pendingListings.get(request.url);
                        if (listingStub) pendingListings.delete(request.url);
                        
                        // Try JSON-LD first
                        const json = extractFromJsonLd($);
                        const data = json || {};
                        
                        // Enhanced selectors for detail pages
                        if (!data.title) {
                            data.title = $('h1, [class*="job-title"], .job-heading').first().text().trim() || null;
                        }
                        
                        if (!data.company) {
                            data.company = $('[class*="recruiter"], [class*="employer"], .company, .job-company').first().text().trim() || null;
                        }
                        
                        if (!data.description_html) {
                            const descSelectors = ['.job-description', '[class*="description"]', '.job-details', 'article', '.content'];
                            for (const sel of descSelectors) {
                                const desc = $(sel).first();
                                if (desc.length && desc.text().length > 50) {
                                    data.description_html = desc.html();
                                    break;
                                }
                            }
                        }
                        
                        if (data.description_html) {
                            data.description_text = cleanText(data.description_html);
                        }
                        
                        if (!data.location) {
                            data.location = $('[class*="location"]').first().text().trim() || null;
                        }
                        
                        if (data.date_posted) {
                            data.date_posted = normalizePostedDateValue(data.date_posted);
                        }
                        if (!data.date_posted) {
                            const timeNode = $('time[datetime]').first();
                            if (timeNode.length) {
                                data.date_posted = normalizePostedDateValue(timeNode.attr('datetime') || timeNode.text());
                            }
                        }
                        if (!data.date_posted) {
                            const postedNode = $('[class*="posted"], [class*="date"]').filter((_, el) => {
                                const text = $(el).text().toLowerCase();
                                return text.includes('posted') || text.includes('hour') || text.includes('day');
                            }).first();
                            if (postedNode.length) {
                                data.date_posted = normalizePostedDateValue(postedNode.text());
                            }
                        }
                        
                        const salary = $('[class*="salary"], .wage').first().text().trim() || null;
                        const jobTypeFromJson = data.employmentType || data.jobType || data.job_type || null;
                        const jobType =
                            jobTypeFromJson ||
                            extractJobTypeFromPage($) ||
                            listingStub?.job_type ||
                            $('[class*="job-type"], .employment-type').first().text().trim() ||
                            null;

                        const merged = {
                            ...(listingStub || {}),
                            title: data.title || listingStub?.title || null,
                            company: data.company || listingStub?.company || null,
                            location: data.location || listingStub?.location || null,
                            salary: salary || listingStub?.salary || null,
                            job_type: jobType || listingStub?.job_type || null,
                            date_posted: data.date_posted || listingStub?.date_posted || null,
                            description_html: data.description_html || listingStub?.description_html || null,
                            description_text: data.description_text || listingStub?.description_text || null,
                            url: request.url,
                        };
                        merged.date_posted = normalizePostedDateValue(merged.date_posted);

                        if (!merged.description_text && merged.description_html) {
                            merged.description_text = cleanText(merged.description_html);
                        }

                        if (merged.title) {
                            await pushJob(merged, 'DETAIL');
                        } else {
                            crawlerLog.warning(`Detail page missing title, skipping push: ${request.url}`);
                        }
                    } catch (err) {
                        crawlerLog.error(`DETAIL extraction failed: ${err.message}`);
                    }
                    
                    // Shorter delay after detail page - they're less scrutinized than list pages
                    await sleep(600 + Math.random() * 900);
                }
            }
        });

        log.info(`Starting crawler with ${initial.length} initial URL(s):`, initial);
        try {
            await crawler.run(initial.map(u => ({ url: u, userData: { label: 'LIST', pageNo: 1, referrer: 'https://www.google.com/' } })));
        } catch (crawlerError) {
            log.warning('Crawler encountered errors but continuing with post-processing:', crawlerError.message);
        }

        if (collectDetails && pendingListings.size && saved < RESULTS_WANTED) {
            log.info('Flushing pending listings without detail pages', { pending: pendingListings.size });
            for (const job of pendingListings.values()) {
                if (saved >= RESULTS_WANTED) break;
                await pushJob(job, 'LIST_FALLBACK');
                stats.pendingListingFlushes += 1;
            }
        }

        log.info(`✓ Finished successfully. Saved ${saved} job listings`);
        stats.totalSaved = saved;
        stats.pendingListings = pendingListings.size;
        stats.detailQueueSize = queuedDetailUrls.size;
        stats.fallbackHeaderHits = fallbackHeaderHits;
        stats.postedWithin = postedWithinLabel;
        stats.recencyWindowHours = recencyWindowMs ? Math.round(recencyWindowMs / (60 * 60 * 1000)) : null;
        stats.timestamp = new Date().toISOString();
        
        // Calculate success rate
        const totalRequests = stats.listPagesProcessed + stats.detailPagesProcessed;
        const successfulRequests = totalRequests - stats.blockedResponses;
        stats.successRate = totalRequests > 0 ? ((successfulRequests / totalRequests) * 100).toFixed(2) + '%' : 'N/A';
        
        await Actor.setValue('RUN_STATS', stats);
        
        // Enhanced result summary
        log.info('=== Scraper Run Summary ===', {
            totalSaved: saved,
            targetResults: RESULTS_WANTED,
            listPagesProcessed: stats.listPagesProcessed,
            detailPagesProcessed: stats.detailPagesProcessed,
            blockedResponses: stats.blockedResponses,
            successRate: stats.successRate,
            recencyFiltered: stats.recencyFiltered,
            duplicateJobsSkipped: stats.duplicateJobsSkipped,
        });
        
        if (saved === 0) {
            log.warning('No jobs were extracted. Possible causes:', {
                possibleReasons: [
                    'All requests were blocked (check blockedResponses count)',
                    'Site selectors may have changed',
                    'postedWithin filter too restrictive',
                    'Proxy configuration issues',
                ],
                blockedCount: stats.blockedResponses,
                successRate: stats.successRate,
            });
        }
        
        if (stats.blockedResponses > totalRequests * 0.3) {
            log.warning('High rate of blocked requests detected. Consider:', {
                blockedPercentage: ((stats.blockedResponses / totalRequests) * 100).toFixed(2) + '%',
                suggestions: [
                    'Use residential proxies instead of datacenter',
                    'Reduce concurrency further',
                    'Increase delays between requests',
                    'Check if IP is rate-limited',
                ],
            });
        }
    } catch (error) {
        // Log enriched error details to help debugging on the platform
        try {
            const details = {
                name: error?.name,
                message: error?.message,
                stack: error?.stack,
                validationErrors: error?.validationErrors || null,
            };
            log.error('Fatal error in main():', details);
            console.error('Fatal error in main():', JSON.stringify(details, null, 2));
        } catch (logErr) {
            // Fallback log
            log.error('Fatal error in main():', error);
            console.error(error);
        }
        // Re-throw so the caller/process sees non-zero exit. This ensures platform marks the run as failed.
        throw error;
    } finally {
        try {
            await Actor.exit();
        } catch (exitErr) {
            log.warning('Actor.exit() failed:', exitErr && exitErr.message ? exitErr.message : exitErr);
        }
    }
}

main().catch(err => { console.error(err); process.exit(1); });
