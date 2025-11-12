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
        const startUrl = safeStr(input.startUrl, '');
        const url = safeStr(input.url, '');
        const startUrls = Array.isArray(input.startUrls) ? input.startUrls : undefined;
        const proxyConfiguration = safeObj(input.proxyConfiguration, undefined);
        const postedWithinInput = safeStr(input.postedWithin, 'any');

        // Defensive input validation and logging
        if (typeof input !== 'object' || Array.isArray(input)) {
            log.error('Input must be a JSON object. Received:', input);
            throw new Error('INPUT_ERROR: Input must be a JSON object.');
        }

        const RESULTS_WANTED = Number.isFinite(+results_wanted) ? Math.max(1, +results_wanted) : Number.MAX_SAFE_INTEGER;
        const MAX_PAGES = Number.isFinite(+max_pages) ? Math.max(1, +max_pages) : 999;
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
        // Dynamic header generation for anti-bot evasion
        const getHeaders = () => {
            if (headerGeneratorInstance) {
                try {
                    return headerGeneratorInstance.getHeaders();
                } catch (error) {
                    log.warning('HeaderGenerator getHeaders failed, using fallback headers:', error.message);
                }
            }
            fallbackHeaderHits += 1;
            return {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-User': '?1',
                'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-ch-ua-platform-version': '"15.0.0"',
                'sec-ch-ua-model': '""',
                'Cache-Control': 'max-age=0',
            };
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
        if (startUrl) initial.push(startUrl);
        if (url) initial.push(url);
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
            maxRequestRetries: 5,
            useSessionPool: true,
            minConcurrency: 1,
            maxConcurrency: 4,
            autoscaledPoolOptions: {
                desiredConcurrency: 2,
            },
            requestHandlerTimeoutSecs: 120,
            navigationTimeoutSecs: 90,
            sessionPoolOptions: {
                maxPoolSize: 50,
                sessionOptions: {
                    maxUsageCount: 10,
                    maxAgeSecs: 600,
                },
            },
            
            // Stealth headers and throttling handled in hooks
            preNavigationHooks: [
                async ({ request }) => {
                    const headers = getHeaders();
                    const referer = request.userData?.referrer || 'https://www.caterer.com/';
                    const fetchSite = referer.includes('caterer.com') ? 'same-origin' : 'same-site';
                    request.headers = {
                        ...headers,
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Referer': referer,
                        'Origin': 'https://www.caterer.com',
                        'Sec-Fetch-Site': fetchSite,
                        'Priority': 'u=1, i',
                    };
                    await sleep((Math.random() * 1.5 + 0.5) * 1000);
                },
            ],

            async errorHandler({ request, error, session, log: crawlerLog }) {
                const retries = request.retryCount ?? 0;
                if (session) session.retire();
                const message = error?.message || '';
                const isHttp2Reset = /nghttp2/i.test(message);
                const baseWait = Math.min(20000, (2 ** Math.min(retries, 6)) * 400 + Math.random() * 600);
                const waitMs = isHttp2Reset ? Math.min(30000, baseWait * 1.5) : baseWait;
                crawlerLog.warning(isHttp2Reset ? 'HTTP/2 stream reset detected, backing off before retry' : 'Request error, applying exponential backoff before retry', {
                    url: request.url,
                    message,
                    waitMs,
                    retryCount: retries,
                });
                await sleep(waitMs);
            },
            async failedRequestHandler({ request, error }, { session }) {
                log.error(`Request failed after ${request.retryCount} retries: ${request.url}`, { 
                    error: error.message,
                    statusCode: error.statusCode 
                });
                if (session) session.retire();
            },
            async requestHandler({ request, $, enqueueLinks, log: crawlerLog, response, session }) {
                if (saved >= RESULTS_WANTED) {
                    crawlerLog.info('Results limit reached, skipping further processing');
                    return;
                }

                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;
                const statusCode = response?.statusCode ?? response?.status;
                if (statusCode && [403, 429].includes(Number(statusCode))) {
                    stats.blockedResponses += 1;
                    crawlerLog.warning(`Blocked with status ${statusCode} on ${request.url}`);
                    if (session) session.retire();
                    throw new Error(`Blocked with status ${statusCode}`);
                }
                const pageTitle = typeof $ === 'function' ? $('title').first().text().toLowerCase() : '';
                if ($ && (pageTitle.includes('access denied') || pageTitle.includes('temporarily blocked'))) {
                    stats.blockedResponses += 1;
                    crawlerLog.warning(`Detected access denial content on ${request.url}`);
                    if (session) session.retire();
                    throw new Error('Access denied or captcha detected');
                }

                if (label === 'LIST') {
                    stats.listPagesProcessed += 1;
                    crawlerLog.info(`Processing LIST page ${pageNo}: ${request.url}`);
                    
                    // Extract jobs directly from listing page
                    const jobs = [];
                    
                    // Find all job links in h2 tags - more reliable selector
                    const jobElements = $('h2 a[href*="/job/"]');
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
                            const title = $link.text().trim();
                            
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
                            
                            if (title && jobUrl && title.length > 2) {
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

                    // Handle pagination
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
                    await sleep((Math.random() * 0.5 + 0.5) * 1000);
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
                    await sleep((Math.random() * 0.5 + 0.5) * 1000);
                }
            }
        });

        log.info(`Starting crawler with ${initial.length} initial URL(s):`, initial);
        await crawler.run(initial.map(u => ({ url: u, userData: { label: 'LIST', pageNo: 1, referrer: 'https://www.caterer.com/' } })));

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
        await Actor.setValue('RUN_STATS', stats);
        
        if (saved === 0) {
            log.warning('No jobs were extracted. This might indicate selectors need updating or the site structure has changed.');
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
