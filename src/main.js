// Caterer.com Job Scraper v5.1 - Hybrid Mode
// Primary: Cheerio (fast HTTP) for data extraction
// Fallback: PlaywrightCrawler (proper Apify integration) when blocked
// Batch saves all jobs from listing pages

import { Actor, log } from 'apify';
import { CheerioCrawler, PlaywrightCrawler, Dataset } from 'crawlee';

// ============================================================================
// CONSTANTS
// ============================================================================

const RELATIVE_UNIT_MS = {
    minute: 60 * 1000,
    hour: 60 * 60 * 1000,
    day: 24 * 60 * 60 * 1000,
    week: 7 * 24 * 60 * 60 * 1000,
    month: 30 * 24 * 60 * 60 * 1000,
};

const RECENCY_WINDOWS = {
    '24h': RELATIVE_UNIT_MS.day,
    '7d': 7 * RELATIVE_UNIT_MS.day,
    '30d': 30 * RELATIVE_UNIT_MS.day,
};

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
];

const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

let recencyWindowMs = null;

// ============================================================================
// UTILITIES
// ============================================================================

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const normalizePostedWithin = (value) => {
    const allowed = new Set(['any', '24h', '7d', '30d']);
    if (!value || typeof value !== 'string') return 'any';
    return allowed.has(value.trim().toLowerCase()) ? value.trim().toLowerCase() : 'any';
};

const parsePostedDate = (value) => {
    if (!value) return null;
    if (value instanceof Date) return value;
    if (typeof value === 'number') return new Date(value);

    const text = String(value).trim();
    const parsed = Date.parse(text);
    if (!Number.isNaN(parsed)) return new Date(parsed);

    const lower = text.toLowerCase();
    if (/just|today|new|featured/.test(lower)) return new Date();
    if (lower.includes('yesterday')) return new Date(Date.now() - RELATIVE_UNIT_MS.day);

    const relMatch = lower.match(/(\d+)\s*(minute|hour|day|week|month)s?\s*ago/);
    if (relMatch) {
        const multiplier = RELATIVE_UNIT_MS[relMatch[2]];
        if (multiplier) return new Date(Date.now() - Number(relMatch[1]) * multiplier);
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

const normalizeText = (text) => (text || '').replace(/\s+/g, ' ').trim();

const toAbs = (href, base = 'https://www.caterer.com') => {
    try { return new URL(href, base).href; } catch { return null; }
};

// ============================================================================
// JSON EXTRACTION (from Cheerio $ or raw HTML)
// ============================================================================

function extractStateFromCheerio($) {
    let resultListJson = null;

    $('script').each((_, script) => {
        const content = $(script).html() || '';

        if (content.includes('app-unifiedResultlist')) {
            const marker = '"app-unifiedResultlist"]';
            const markerIdx = content.indexOf(marker);

            if (markerIdx !== -1) {
                const afterMarker = content.slice(markerIdx + marker.length);
                const eqIdx = afterMarker.indexOf('=');

                if (eqIdx !== -1) {
                    const jsonStart = afterMarker.slice(eqIdx + 1).trim();

                    // Balanced brace matching
                    let depth = 0, jsonEnd = 0, inString = false, escape = false;

                    for (let i = 0; i < jsonStart.length; i++) {
                        const c = jsonStart[i];
                        if (escape) { escape = false; continue; }
                        if (c === '\\' && inString) { escape = true; continue; }
                        if (c === '"') { inString = !inString; continue; }
                        if (inString) continue;
                        if (c === '{') depth++;
                        if (c === '}') { depth--; if (depth === 0) { jsonEnd = i + 1; break; } }
                    }

                    if (jsonEnd > 0) {
                        try {
                            resultListJson = JSON.parse(jsonStart.slice(0, jsonEnd));
                            return false;
                        } catch { }
                    }
                }
            }
        }
    });

    return resultListJson;
}

function extractJobsFromState(state) {
    if (!state) return [];
    const items = state?.searchResults?.items || [];

    return items.map(item => {
        let company = item.companyName || item.company || item.recruiterName || null;
        if (!company && item.companyCard) company = item.companyCard.name || null;

        // Clean description - remove HTML and normalize whitespace
        let description = item.textSnippet || item.description || item.snippet || '';
        description = description
            .replace(/<[^>]*>/g, ' ')  // Remove HTML tags
            .replace(/&nbsp;/g, ' ')    // Replace &nbsp;
            .replace(/&amp;/g, '&')     // Replace &amp;
            .replace(/&lt;/g, '<')      // Replace &lt;
            .replace(/&gt;/g, '>')      // Replace &gt;
            .replace(/\s+/g, ' ')       // Normalize whitespace
            .trim();

        return {
            source: 'caterer.com',
            title: item.title || item.jobTitle || null,
            company,
            location: item.location || item.jobLocation || item.locationLabel || null,
            salary: item.salary || item.salaryDescription || item.salaryLabel || null,
            job_type: item.contractType || item.employmentType || item.workType || null,
            date_posted: normalizePostedDateValue(item.datePosted || item.postedDate || item.listingDate),
            description,
            url: item.url ? toAbs(item.url) : null,
            job_id: item.id || item.jobId || null,
        };
    }).filter(job => job.url && job.title);
}

function extractPagination(state) {
    const p = state?.searchResults?.pagination;
    return p ? { pageCount: p.pageCount || 0, currentPage: p.currentPage || 1 } : null;
}

// ============================================================================
// URL BUILDING
// ============================================================================

const buildStartUrl = (kw, loc) => {
    const kwSlug = (kw || '').toLowerCase().trim().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
    const locSlug = (loc || '').toLowerCase().trim().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');

    if (kwSlug && locSlug) return `https://www.caterer.com/jobs/${kwSlug}/in-${locSlug}`;
    if (kwSlug) return `https://www.caterer.com/jobs/${kwSlug}`;
    if (locSlug) return `https://www.caterer.com/jobs/in-${locSlug}`;
    return 'https://www.caterer.com/jobs';
};

// ============================================================================
// MAIN
// ============================================================================

async function main() {
    try {
        await Actor.init();
        log.info('═══════════════════════════════════════════════════════');
        log.info('  Caterer.com Scraper v5.1 - Hybrid Mode');
        log.info('═══════════════════════════════════════════════════════');

        const input = (await Actor.getInput()) || {};

        const keyword = input.keyword || '';
        const location = input.location || '';
        const RESULTS_WANTED = Number(input.results_wanted) > 0 ? Number(input.results_wanted) : 100;
        const MAX_PAGES = Number(input.max_pages) > 0 ? Number(input.max_pages) : 20;
        const startUrl = input.startUrl || '';
        const startUrls = Array.isArray(input.startUrls) ? input.startUrls : [];

        recencyWindowMs = RECENCY_WINDOWS[normalizePostedWithin(input.postedWithin)] || null;

        log.info('Config:', { keyword: keyword || '(all)', location: location || '(all)', results_wanted: RESULTS_WANTED, max_pages: MAX_PAGES });

        const initialUrl = startUrl || startUrls[0] || buildStartUrl(keyword, location);
        log.info(`Start URL: ${initialUrl}`);

        // Setup proxy - use RESIDENTIAL group for better success
        let proxyConf;
        try {
            const proxyConfig = input.proxyConfiguration || {
                useApifyProxy: true,
                apifyProxyGroups: ['RESIDENTIAL'],
                apifyProxyCountry: 'GB'
            };
            proxyConf = await Actor.createProxyConfiguration(proxyConfig);
            log.info('✅ Proxy ready');
        } catch (e) {
            log.warning('⚠️ Proxy failed:', e.message);
        }

        // State
        let saved = 0;
        const pushedUrls = new Set();
        const stats = { pages: 0, extracted: 0, saved: 0, playwrightUsed: 0, cheerioUsed: 0 };

        // Process function for both Cheerio and Playwright results
        async function processJobs(state, pageNo) {
            if (!state) return null;

            const jobs = extractJobsFromState(state);
            stats.extracted += jobs.length;
            log.info(`✓ Extracted ${jobs.length} jobs from page ${pageNo}`);

            const toSave = [];
            for (const job of jobs) {
                if (saved >= RESULTS_WANTED) break;
                if (pushedUrls.has(job.url)) continue;
                if (!shouldKeepByRecency(job.date_posted)) continue;

                toSave.push({
                    ...job,
                    keyword_search: keyword || null,
                    location_search: location || null,
                    extracted_at: new Date().toISOString(),
                });

                pushedUrls.add(job.url);
                saved++;
            }

            if (toSave.length > 0) {
                await Dataset.pushData(toSave);
                stats.saved += toSave.length;
                log.info(`✅ SAVED ${toSave.length} jobs (Total: ${saved}/${RESULTS_WANTED})`);
            }

            stats.pages++;
            return extractPagination(state);
        }

        // Track blocked pages for Playwright fallback
        const blockedPages = [];
        let lastPagination = null;

        // ====================================================================
        // PHASE 1: Try CheerioCrawler first (fast HTTP)
        // ====================================================================
        log.info('🚀 Phase 1: Starting Cheerio crawler...');

        const cheerioCrawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 2,
            maxConcurrency: 2,
            requestHandlerTimeoutSecs: 30,

            // Use session pool for better anti-blocking
            useSessionPool: true,
            sessionPoolOptions: {
                maxPoolSize: 10,
                sessionOptions: {
                    maxUsageCount: 5,
                },
            },

            preNavigationHooks: [
                async ({ request }) => {
                    request.headers = {
                        'User-Agent': getRandomUserAgent(),
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-GB,en;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                    };

                    // Add delay for non-first requests
                    if (request.userData.pageNo > 1) {
                        await sleep(2000 + Math.random() * 2000);
                    }
                },
            ],

            async requestHandler({ $, request, response }) {
                const pageNo = request.userData.pageNo || 1;
                log.info(`📄 [Cheerio] Page ${pageNo}: ${request.url}`);

                // Check for blocking
                const bodyText = $('body').text() || '';
                const bodyLength = bodyText.length;

                if (response.statusCode === 403 ||
                    bodyText.includes('Access Denied') ||
                    bodyText.includes('blocked') ||
                    bodyLength < 5000) {
                    log.warning(`⚠️ Cheerio blocked on page ${pageNo} (status: ${response.statusCode}, body: ${bodyLength} chars)`);
                    blockedPages.push({ url: request.url, pageNo });
                    return;
                }

                const state = extractStateFromCheerio($);

                if (!state) {
                    log.warning(`⚠️ No JSON state on page ${pageNo}, will try Playwright`);
                    blockedPages.push({ url: request.url, pageNo });
                    return;
                }

                stats.cheerioUsed++;
                const pagination = await processJobs(state, pageNo);
                lastPagination = pagination;

                // Enqueue next page if needed
                if (saved < RESULTS_WANTED && pageNo < MAX_PAGES && pagination && pageNo < pagination.pageCount) {
                    const nextUrl = new URL(initialUrl);
                    nextUrl.searchParams.set('page', pageNo + 1);
                    await cheerioCrawler.addRequests([{
                        url: nextUrl.href,
                        userData: { pageNo: pageNo + 1 }
                    }]);
                    log.info(`→ Queued page ${pageNo + 1}`);
                }
            },

            async failedRequestHandler({ request }) {
                const pageNo = request.userData.pageNo || 1;
                log.warning(`❌ Cheerio failed for page ${pageNo}`);
                blockedPages.push({ url: request.url, pageNo });
            },
        });

        // Run Cheerio crawler
        await cheerioCrawler.run([{ url: initialUrl, userData: { pageNo: 1 } }]);

        // ====================================================================
        // PHASE 2: Use PlaywrightCrawler for blocked pages (proper Apify integration)
        // ====================================================================
        if (blockedPages.length > 0 && saved < RESULTS_WANTED) {
            log.info(`🎭 Phase 2: Retrying ${blockedPages.length} blocked page(s) with Playwright...`);

            const playwrightCrawler = new PlaywrightCrawler({
                proxyConfiguration: proxyConf,
                maxRequestRetries: 2,
                maxConcurrency: 1,
                navigationTimeoutSecs: 45,
                requestHandlerTimeoutSecs: 60,

                // Use session pool
                useSessionPool: true,
                sessionPoolOptions: {
                    maxPoolSize: 5,
                },

                // Browser configuration
                launchContext: {
                    launchOptions: {
                        headless: true,
                        args: [
                            '--disable-blink-features=AutomationControlled',
                            '--disable-dev-shm-usage',
                            '--no-sandbox',
                            '--disable-setuid-sandbox',
                        ],
                    },
                },

                // Pre-navigation hooks for stealth
                preNavigationHooks: [
                    async ({ page, request }) => {
                        // Add stealth scripts
                        await page.addInitScript(() => {
                            // Hide webdriver
                            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

                            // Override permissions
                            const originalQuery = window.navigator.permissions.query;
                            window.navigator.permissions.query = (parameters) =>
                                parameters.name === 'notifications'
                                    ? Promise.resolve({ state: Notification.permission })
                                    : originalQuery(parameters);
                        });

                        // Block heavy resources to speed up
                        await page.route('**/*', (route) => {
                            const type = route.request().resourceType();
                            if (['image', 'media', 'font'].includes(type)) {
                                return route.abort();
                            }
                            return route.continue();
                        });

                        // Add delay
                        await sleep(1000 + Math.random() * 1000);
                    },
                ],

                async requestHandler({ page, request }) {
                    const pageNo = request.userData.pageNo || 1;
                    log.info(`🎭 [Playwright] Page ${pageNo}: ${request.url}`);

                    // Wait for page to stabilize
                    await page.waitForLoadState('domcontentloaded', { timeout: 30000 });
                    await sleep(2000);

                    // Try to extract state
                    const state = await page.evaluate(() => {
                        return window.__PRELOADED_STATE__?.['app-unifiedResultlist'] || null;
                    });

                    if (!state) {
                        log.warning(`⚠️ No state found on page ${pageNo} with Playwright`);
                        return;
                    }

                    stats.playwrightUsed++;
                    const pagination = await processJobs(state, pageNo);

                    // Enqueue next page if needed
                    if (saved < RESULTS_WANTED && pageNo < MAX_PAGES && pagination && pageNo < pagination.pageCount) {
                        const nextUrl = new URL(initialUrl);
                        nextUrl.searchParams.set('page', pageNo + 1);
                        await playwrightCrawler.addRequests([{
                            url: nextUrl.href,
                            userData: { pageNo: pageNo + 1 }
                        }]);
                        log.info(`→ Queued page ${pageNo + 1} (Playwright)`);
                    }
                },

                async failedRequestHandler({ request, error }) {
                    const pageNo = request.userData.pageNo || 1;
                    log.error(`❌ Playwright failed for page ${pageNo}: ${error.message}`);
                },
            });

            // Run Playwright for blocked pages
            await playwrightCrawler.run(blockedPages.map(p => ({
                url: p.url,
                userData: { pageNo: p.pageNo }
            })));
        }

        log.info('═══════════════════════════════════════════════════════');
        log.info(`✅ COMPLETE: ${saved} jobs saved`);
        log.info('Stats:', stats);
        log.info('═══════════════════════════════════════════════════════');

        await Actor.setValue('RUN_STATS', { ...stats, totalSaved: saved });

    } catch (error) {
        log.error('Fatal:', error.message);
        throw error;
    } finally {
        await Actor.exit();
    }
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});
