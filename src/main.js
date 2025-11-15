// Caterer Job Scraper - Production-ready implementation
// Stack: Apify + Crawlee + CheerioCrawler + gotScraping + header-generator
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
// gotScraping removed - not needed after simplifying error handling
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
    hour: RELATIVE_UNIT_MS.hour,
    '24hours': RELATIVE_UNIT_MS.day,
    week: RELATIVE_UNIT_MS.week,
    month: RELATIVE_UNIT_MS.month,
    any: null,
};

const toAbs = (href, base) => {
    try {
        if (!href) return null;
        // Ignore javascript: and mailto: links explicitly
        if (/^(javascript|mailto):/i.test(href)) return null;
        // Handle protocol-relative URLs (//example.com/foo)
        if (/^\/\//.test(href)) {
            const baseUrl = new URL(base);
            return `${baseUrl.protocol}${href}`;
        }
        // Relative or absolute HTTP(S)
        return new URL(href, base).href;
    } catch {
        return null;
    }
};

const normalizeText = (str) => {
    if (!str || typeof str !== 'string') return '';
    return str.replace(/\s+/g, ' ').trim();
};

const normalizeLocation = (str) => {
    const normalized = normalizeText(str);
    if (!normalized) return null;
    // Basic normalization for UK cities/regions, can be extended as needed
    return normalized
        .replace(/\b(london,?\s*uk)\b/i, 'London, United Kingdom')
        .replace(/\b(manchester,?\s*uk)\b/i, 'Manchester, United Kingdom')
        .replace(/\b(birmingham,?\s*uk)\b/i, 'Birmingham, United Kingdom');
};

const parseRelativeDate = (text) => {
    if (!text || typeof text !== 'string') return null;
    const normalized = text.toLowerCase().trim();

    // Handle obvious explicit dates quickly
    if (/\b\d{1,2}\s+[a-z]{3,9}\s+\d{4}\b/i.test(normalized)) {
        const date = new Date(normalized);
        return Number.isNaN(date.getTime()) ? null : date;
    }

    const now = Date.now();
    if (/just\s+now|moments\s+ago|few\s+seconds\s+ago/.test(normalized)) {
        return new Date(now - 30 * 1000);
    }

    if (/today/.test(normalized)) {
        return new Date(now);
    }

    if (/yesterday/.test(normalized)) {
        return new Date(now - RELATIVE_UNIT_MS.day);
    }

    const patterns = [
        { regex: /(\d+)\s+minute/, unit: 'minute' },
        { regex: /(\d+)\s+hour/, unit: 'hour' },
        { regex: /(\d+)\s+day/, unit: 'day' },
        { regex: /(\d+)\s+week/, unit: 'week' },
        { regex: /(\d+)\s+month/, unit: 'month' },
        { regex: /(\d+)\s+year/, unit: 'year' },
    ];

    for (const { regex, unit } of patterns) {
        const match = normalized.match(regex);
        if (match && match[1]) {
            const count = Number(match[1]);
            if (Number.isFinite(count) && RELATIVE_UNIT_MS[unit]) {
                return new Date(now - count * RELATIVE_UNIT_MS[unit]);
            }
        }
    }

    return null;
};

const cleanDescription = (html) => {
    if (!html) return '';
    const $ = cheerioLoad(html);
    // Remove script and style tags as they are not part of human-readable content
    $('script, style, noscript').remove();

    // Remove elements that are clearly boilerplate or unrelated to job content
    const boilerplateSelectors = [
        'header',
        'footer',
        'nav',
        '.breadcrumb',
        '.breadcrumbs',
        '.cookie-banner',
        '.cookie-consent',
        '.social-share',
        '.share-buttons',
        '.newsletter',
        '#newsletter',
        '.job-apply',
        '.apply-button-container',
        '.similar-jobs',
        '.related-jobs',
    ];
    $(boilerplateSelectors.join(',')).remove();

    // Simplify links to just their text content to avoid URL noise
    $('a').each((_, el) => {
        const text = $(el).text();
        $(el).replaceWith(text);
    });

    // Convert <br> and similar to newlines so paragraph structure is somewhat preserved
    $('br').replaceWith('\n');
    $('p').each((_, el) => {
        const text = $(el).text().trim();
        $(el).replaceWith(`${text}\n\n`);
    });

    let text = $('body').text();
    if (!text || typeof text !== 'string') text = '';

    text = text
        .replace(/\r/g, '')
        .replace(/\n{3,}/g, '\n\n')
        .replace(/[ \t]+/g, ' ')
        .replace(/\s+\n/g, '\n')
        .replace(/\n\s+/g, '\n')
        .trim();

    return text;
};

const detectJobTypeFromText = ($, rootSelector = 'body') => {
    const root = $(rootSelector);

    const typeKeywords = [
        'full-time',
        'part-time',
        'contract',
        'permanent',
        'temporary',
        'freelance',
        'internship',
        'apprenticeship',
    ];

    const possibleTypeElements = root.find(
        'li, span, div, p, strong, b, em, h4, h5, h6, .job-type, .employment-type, .tag',
    );

    const typeCounts = new Map();

    possibleTypeElements.each((_, el) => {
        const text = $(el).text().toLowerCase();
        for (const keyword of typeKeywords) {
            if (text.includes(keyword)) {
                const count = typeCounts.get(keyword) || 0;
                typeCounts.set(keyword, count + 1);
            }
        }
    });

    if (typeCounts.size === 0) {
        return null;
    }

    let bestType = null;
    let bestCount = 0;
    for (const [type, count] of typeCounts.entries()) {
        if (count > bestCount) {
            bestCount = count;
            bestType = type;
        }
    }

    if (!bestType) return null;

    const normalizedMap = {
        'full-time': 'Full-time',
        'part-time': 'Part-time',
        contract: 'Contract',
        permanent: 'Permanent',
        temporary: 'Temporary',
        freelance: 'Freelance',
        internship: 'Internship',
        apprenticeship: 'Apprenticeship',
    };

    return normalizedMap[bestType] || bestType;
};

const extractJobType = ($, detailRootSelector) => {
    if (!detailRootSelector) detailRootSelector = 'body';

    const knownTypeSelectors = [
        'li:contains("Job type")',
        'li:contains("Employment type")',
        'li:contains("Contract type")',
        '.job-type',
        '.employment-type',
        'span:contains("Full-time")',
        'span:contains("Part-time")',
        'span:contains("Contract")',
        'span:contains("Permanent")',
        'span:contains("Temporary")',
        'span:contains("Freelance")',
    ];

    for (const selector of knownTypeSelectors) {
        const el = $(selector).first();
        if (el && el.length > 0) {
            const text = el.text().trim();
            if (text) {
                const lower = text.toLowerCase();
                if (lower.includes('full-time')) return 'Full-time';
                if (lower.includes('part-time')) return 'Part-time';
                if (lower.includes('contract')) return 'Contract';
                if (lower.includes('permanent')) return 'Permanent';
                if (lower.includes('temporary')) return 'Temporary';
                if (lower.includes('freelance')) return 'Freelance';
                if (lower.includes('internship')) return 'Internship';
                if (lower.includes('apprenticeship')) return 'Apprenticeship';
                const parts = text.split(/[:\-]/);
                if (parts.length > 1) {
                    return normalizeText(parts[1]);
                }
                return normalizeText(text);
            }
        }
    }

    const metaTypeSelectors = [
        'meta[itemprop="employmentType"]',
        'meta[property="employmentType"]',
        'meta[name="employmentType"]',
    ];
    for (const sel of metaTypeSelectors) {
        const val = $(sel).attr('content') || $(sel).attr('value');
        if (val) {
            const lower = val.toLowerCase();
            if (lower.includes('full')) return 'Full-time';
            if (lower.includes('part')) return 'Part-time';
            if (lower.includes('contract')) return 'Contract';
            if (lower.includes('permanent')) return 'Permanent';
            if (lower.includes('temporary')) return 'Temporary';
            if (lower.includes('freelance')) return 'Freelance';
            return normalizeText(val);
        }
    }

    const jobTypeFromStructure = detectJobTypeFromText($, detailRootSelector);
    if (jobTypeFromStructure) return jobTypeFromStructure;

    const pageText = $('body').text();
    const typePatterns = [
        /(?:job type|employment type|contract type)\s*:\s*(full[- ]?time|part[- ]?time|contract|permanent|temporary|freelance)/i,
        /(full[- ]?time|part[- ]?time)\s+(?:position|role|employment)/i,
    ];
    for (const pattern of typePatterns) {
        const match = pageText.match(pattern);
        if (match && match[1]) {
            const lower = match[1].toLowerCase();
            if (lower.includes('full')) return 'Full-time';
            if (lower.includes('part')) return 'Part-time';
            if (lower.includes('contract')) return 'Contract';
            if (lower.includes('permanent')) return 'Permanent';
            if (lower.includes('temporary')) return 'Temporary';
            if (lower.includes('freelance')) return 'Freelance';
            return normalizeText(match[1]);
        }
    }

    return null;
};

const parsePostedWithin = (postedWithin, fallbackLabel = 'any') => {
    if (!postedWithin) return fallbackLabel;

    const value = String(postedWithin).toLowerCase().trim();
    if (!value || value === 'any') return 'any';

    if (value === 'hour' || value === 'past_hour') return 'hour';
    if (value === '24hours' || value === 'day' || value === '24h' || value === 'past_24_hours') return '24hours';
    if (value === 'week' || value === 'past_week' || value === '7d') return 'week';
    if (value === 'month' || value === 'past_month' || value === '30d') return 'month';

    if (value.startsWith('last_')) {
        const rawUnit = value.replace(/^last_/, '');
        if (rawUnit === 'hour') return 'hour';
        if (rawUnit === '24hours' || rawUnit === 'day') return '24hours';
        if (rawUnit === 'week') return 'week';
        if (rawUnit === 'month') return 'month';
    }

    return fallbackLabel;
};

const normalizePostedWithin = (postedWithinRaw) => {
    if (Array.isArray(postedWithinRaw) && postedWithinRaw.length > 0) {
        const first = postedWithinRaw[0];
        return parsePostedWithin(first, 'any');
    }

    return parsePostedWithin(postedWithinRaw, 'any');
};

const buildSearchUrl = ({ keyword, location, postedWithin }) => {
    const baseUrl = new URL('https://www.caterer.com/jobs');
    const params = baseUrl.searchParams;

    if (keyword) {
        params.set('keywords', keyword.trim());
    }

    if (location) {
        params.set('location', location.trim());
    }

    const normalized = normalizePostedWithin(postedWithin);
    if (normalized && normalized !== 'any') {
        if (normalized === 'hour') params.set('postedWithin', '1');
        if (normalized === '24hours') params.set('postedWithin', '24');
        if (normalized === 'week') params.set('postedWithin', '168');
        if (normalized === 'month') params.set('postedWithin', '720');
    }

    return baseUrl.href;
};

const safeStr = (val, fallback = '') => {
    if (typeof val === 'string') return val;
    if (val == null) return fallback;
    try {
        return String(val);
    } catch {
        return fallback;
    }
};

const safeObj = (val, fallback = {}) => {
    if (val && typeof val === 'object' && !Array.isArray(val)) return val;
    return fallback;
};

const safeBool = (val, fallback = false) => {
    if (typeof val === 'boolean') return val;
    if (typeof val === 'string') {
        const lower = val.toLowerCase().trim();
        if (['true', '1', 'yes', 'y'].includes(lower)) return true;
        if (['false', '0', 'no', 'n'].includes(lower)) return false;
    }
    if (typeof val === 'number') {
        if (val === 1) return true;
        if (val === 0) return false;
    }
    return fallback;
};

const parseSalaryFromText = (text) => {
    if (!text || typeof text !== 'string') return null;

    const normalized = text.replace(/[, ]+/g, ' ').replace(/\s+/g, ' ').trim();
    const regex = /£\s?(\d[\d\s,]*)\s*(?:-|to|–)\s*£\s?(\d[\d\s,]*)/i;
    const match = normalized.match(regex);
    if (!match || !match[1] || !match[2]) return null;

    const min = Number(match[1].replace(/[^\d]/g, ''));
    const max = Number(match[2].replace(/[^\d]/g, ''));
    if (!Number.isFinite(min) || !Number.isFinite(max) || min <= 0 || max <= 0) return null;

    let period = 'year';
    if (/per\s+hour|hourly|hr\b/i.test(normalized)) period = 'hour';
    else if (/per\s+day|daily\b/i.test(normalized)) period = 'day';
    else if (/per\s+week|weekly\b/i.test(normalized)) period = 'week';
    else if (/per\s+month|monthly\b/i.test(normalized)) period = 'month';

    return {
        min,
        max,
        currency: 'GBP',
        period,
        raw: text.trim(),
    };
};

// *** UPDATED: more robust Caterer pagination ***
function findNextPage($, base) {
    if (!$) return null;

    // 1) Primary: look for explicit "Next" anchor with page=
    const nextLink = $('a[href*="page="]').filter((_, el) => {
        const text = $(el).text().trim().toLowerCase();
        return text === 'next' || text.includes('next');
    }).first().attr('href');

    if (nextLink) return toAbs(nextLink, base);

    // 2) Secondary: infer from current page number and numeric anchors
    const currentUrl = new URL(base);
    const currentPage = parseInt(currentUrl.searchParams.get('page') || '1', 10);
    const nextPageNum = currentPage + 1;

    const numericHref = $(`a[href*="page=${nextPageNum}"]`).first().attr('href');
    if (numericHref) return toAbs(numericHref, base);

    // 3) Fallback: parse "Page X of Y" block and synthesize URL
    let paginationText = '';
    $('[class*="page"], [class*="pagination"], :contains("Page ")').each((_, el) => {
        const text = $(el).text();
        if (/Page\s+\d+\s+of\s+\d+/i.test(text)) {
            paginationText = text;
            return false; // break .each
        }
    });

    if (paginationText) {
        const match = paginationText.match(/Page\s+(\d+)\s+of\s+(\d+)/i);
        if (match) {
            const curr = parseInt(match[1], 10);
            const total = parseInt(match[2], 10);
            if (Number.isFinite(curr) && Number.isFinite(total) && curr < total) {
                const u = new URL(base);
                u.searchParams.set('page', String(curr + 1));
                return u.href;
            }
        }
    }

    return null;
}

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
            log.error('Actor.init() failed:', {
                name: initErr.name,
                message: initErr.message,
                stack: initErr.stack,
                validationErrors: initErr.validationErrors,
            });
            // If this appears to be an input validation problem, log the raw environment input if available
            try {
                if (process.env.APIFY_INPUT) {
                    log.warning('APIFY_INPUT env var present; logging its type and truncated content');
                    const raw = String(process.env.APIFY_INPUT);
                    log.warning('APIFY_INPUT (truncated 1k):', raw.slice(0, 1024));
                }
            } catch (envErr) {
                // Ignore secondary logging failures
            }
            throw initErr;
        }

        let input;
        try {
            input = await Actor.getInput();
            log.info('Actor.getInput() succeeded');
        } catch (error) {
            log.error('Error in Actor.getInput():', error);
            log.error('Error details:', {
                name: error.name,
                message: error.message,
                validationErrors: error.validationErrors,
            });
            if (error && error.name === 'ArgumentError') {
                // Try to fall back to a local INPUT.json if present (useful for local runs or malformed platform input)
                try {
                    const raw = await fs.readFile(new URL('../INPUT.json', import.meta.url));
                    input = JSON.parse(String(raw));
                    log.warning('Loaded fallback INPUT.json from repository root');
                } catch (fsErr) {
                    log.warning('Could not read fallback INPUT.json:', fsErr?.message || fsErr);
                    throw error;
                }
            } else {
                throw error;
            }
        }

        if (!input || typeof input !== 'object') {
            log.warning('No or invalid input received from Actor.getInput(). Using empty object.');
            input = {};
        }

        log.info('Parsed input (truncated safely)', {
            hasKeyword: !!input.keyword,
            hasLocation: !!input.location,
            hasStartUrl: !!input.startUrl,
            hasUrl: !!input.url,
            hasStartUrls: Array.isArray(input.startUrls) && input.startUrls.length > 0,
        });

        let postedWithinLabel = 'any';
        let recencyWindowMs = null;

        const keyword = safeStr(input.keyword, '');
        const location = safeStr(input.location, '');
        const results_wanted = input.results_wanted || input.resultsWanted || input.maxResults || 999;
        const max_pages = input.max_pages || input.maxPages || 999;
        const collectDetails = safeBool(input.collectDetails, true);
        const saveRawHtml = safeBool(input.saveRawHtml, false);
        const debug = safeBool(input.debug, false);
        const startUrl = safeStr(input.startUrl, '');
        const url = safeStr(input.url, '');
        const startUrls = Array.isArray(input.startUrls) ? input.startUrls : undefined;
        const proxyConfiguration = safeObj(input.proxyConfiguration, undefined);

        const useResidentialProxy = true;
        const minDelayMs = Number(process.env.APIFY_MIN_DELAY_MS || 150);
        const maxDelayMs = Number(process.env.APIFY_MAX_DELAY_MS || 600);
        const userMaxConcurrency = Math.max(1, Number(process.env.APIFY_MAX_CONCURRENCY || 8));
        const userMaxRequestRetries = 8;
        const persistCookiesPerSessionInput = true;
        const postedWithinInput = safeStr(input.postedWithin, 'any');

        if (typeof input !== 'object' || Array.isArray(input)) {
            log.error('Input must be a JSON object. Received:', input);
            throw new Error('INPUT_ERROR: Input must be a JSON object.');
        }

        const RESULTS_WANTED = Number.isFinite(+results_wanted) ? Math.max(1, +results_wanted) : Number.MAX_SAFE_INTEGER;
        const MAX_PAGES = Number.isFinite(+max_pages) ? Math.max(1, +max_pages) : 999;
        postedWithinLabel = normalizePostedWithin(postedWithinInput);
        recencyWindowMs = RECENCY_WINDOWS[postedWithinLabel] || null;

        log.info('Starting Caterer Job Scraper', {
            keyword,
            location,
            RESULTS_WANTED,
            MAX_PAGES,
            collectDetails,
            saveRawHtml,
            postedWithinLabel,
            recencyWindowMs,
        });

        const stats = {
            listPagesProcessed: 0,
            detailPagesProcessed: 0,
            listPagesEnqueued: 0,
            totalSaved: 0,
            droppedByRecency: 0,
            droppedByLocationMissing: 0,
            droppedByTitleMissing: 0,
            droppedByUrlMissing: 0,
            skippedDuplicateUrl: 0,
            blockedRequests: 0,
            retriedRequests: 0,
            sessionRetired: 0,
            htmlSavedFiles: 0,
            listingStubsCreated: 0,
            listingStubsWithoutDetail: 0,
        };

        const pendingListings = new Map();
        const seenListingUrls = new Set();
        let saved = 0;

        const startUrlsToUse = [];
        const allRawStartSources = [];

        if (startUrls && startUrls.length > 0) {
            for (const raw of startUrls) {
                if (!raw) continue;
                if (typeof raw === 'string') {
                    allRawStartSources.push(raw);
                } else if (raw && typeof raw === 'object' && raw.url) {
                    allRawStartSources.push(String(raw.url));
                }
            }
        }

        if (startUrl) {
            allRawStartSources.push(startUrl);
        }

        if (url) {
            allRawStartSources.push(url);
        }

        const uniqueRawSources = Array.from(new Set(allRawStartSources));

        const buildStartUrl = (raw) => {
            const lower = (raw || '').toLowerCase();
            if (!lower) return null;

            if (lower.startsWith('http://') || lower.startsWith('https://')) {
                return raw;
            }

            if (lower.includes('http')) {
                const match = raw.match(/https?:\/\/[^\s"']+/);
                if (match) return match[0];
            }

            if (/^\/jobs/.test(lower)) {
                return `https://www.caterer.com${raw.startsWith('/') ? raw : `/${raw}`}`;
            }

            if (lower.includes('jobs') || lower.includes('chef') || lower.includes('restaurant')) {
                try {
                    const asUrl = new URL(raw);
                    if (asUrl.hostname.includes('caterer')) return asUrl.href;
                    return raw;
                } catch {
                    return raw;
                }
            }

            return null;
        };

        for (const raw of uniqueRawSources) {
            const start = buildStartUrl(raw);
            if (start) startUrlsToUse.push(start);
        }

        if (startUrlsToUse.length === 0) {
            const searchUrl = buildSearchUrl({ keyword, location, postedWithin: postedWithinLabel });
            startUrlsToUse.push(searchUrl);
        }

        log.info('Effective start URLs', { startUrlsToUse });

        const headerGeneratorOptions = {
            browsers: [
                { name: 'chrome', minVersion: 90 },
                { name: 'edge', minVersion: 90 },
            ],
            devices: ['desktop'],
            operatingSystems: ['windows', 'macos'],
            locales: ['en-GB', 'en-US'],
        };

        const headerGenerator = {
            getHeaders: () => {
                const uas = [
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15',
                ];
                const ua = uas[Math.floor(Math.random() * uas.length)];
                return {
                    'user-agent': ua,
                    accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                    'accept-encoding': 'gzip, deflate, br',
                    'sec-ch-ua': '"Chromium";v="123", "Not:A-Brand";v="8"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'upgrade-insecure-requests': '1',
                    dnt: '1',
                };
            },
        };

        const persistentState = await Actor.getValue('STATE') || {};
        const globalSeenUrls = new Set(
            Array.isArray(persistentState.seenListingUrls) ? persistentState.seenListingUrls : [],
        );

        const persistState = async () => {
            try {
                await Actor.setValue('STATE', {
                    seenListingUrls: Array.from(globalSeenUrls),
                    lastUpdatedAt: new Date().toISOString(),
                    saved,
                    stats,
                });
            } catch (err) {
                log.error('Failed to persist state', { err });
            }
        };

        const persistCookiesPerSession = persistCookiesPerSessionInput;

        const crawler = new CheerioCrawler({
            proxyConfiguration:
                proxyConfiguration && Object.keys(proxyConfiguration).length
                    ? await Actor.createProxyConfiguration(proxyConfiguration)
                    : await Actor.createProxyConfiguration(
                          useResidentialProxy
                              ? {
                                    groups: ['RESIDENTIAL'],
                                }
                              : {},
                      ),
            useSessionPool: true,
            persistCookiesPerSession,
            maxConcurrency: userMaxConcurrency,
            maxRequestsPerCrawl: 5000,
            requestHandlerTimeoutSecs: 120,
            maxRequestRetries: userMaxRequestRetries,
            errorHandler: async ({ error, request, session, log: crawlerLog, retryCount }) => {
                stats.retriedRequests += 1;

                const statusCode = error && error.statusCode;
                const isBlockedLike =
                    statusCode === 403 ||
                    statusCode === 429 ||
                    /access\s+denied|forbidden|temporarily\s+unavailable/i.test(error.message || '');

                if (isBlockedLike) {
                    stats.blockedRequests += 1;
                    if (session) {
                        crawlerLog.warning(
                            `Blocked request detected (status: ${statusCode}). Retiring session.`,
                            { url: request.url, retryCount, sessionId: session.id },
                        );
                        session.retire();
                        stats.sessionRetired += 1;
                    } else {
                        crawlerLog.warning('Blocked request detected but no session to retire', {
                            url: request.url,
                            retryCount,
                        });
                    }
                } else {
                    crawlerLog.warning('Request failed, will be retried if retries remain', {
                        url: request.url,
                        retryCount,
                        errorMessage: error.message,
                    });
                }
            },
            preNavigationHooks: [
                async ({ request, session, log: crawlerLog }) => {
                    const delay = minDelayMs + Math.random() * (maxDelayMs - minDelayMs);
                    await sleep(delay);

                    if (!request.headers) request.headers = {};

                    const generated = headerGenerator.getHeaders(headerGeneratorOptions);
                    Object.assign(request.headers, generated);

                    request.headers['referer'] = request.headers['referer'] || 'https://www.caterer.com/';
                    request.headers['origin'] = 'https://www.caterer.com';

                    if (session) {
                        request.headers['cookie'] = session.getPuppeteerCookiesString
                            ? session.getPuppeteerCookiesString('https://www.caterer.com')
                            : request.headers['cookie'];
                    }

                    crawlerLog.debug('Pre-navigation hook applied headers and delay', {
                        url: request.url,
                        delayMs: delay,
                        hasSession: !!session,
                    });
                },
            ],
            requestHandler: async ({ request, $, session, log: crawlerLog, enqueueLinks }) => {
                if (!$) {
                    crawlerLog.error('Cheerio instance is missing, cannot process page', {
                        url: request.url,
                        label: request.userData.label,
                    });
                    return;
                }

                const { label = 'LIST', pageNo = 1 } = request.userData || {};

                const statusCodeMeta =
                    request.loadedUrl && typeof request.loadedUrl === 'string'
                        ? undefined
                        : undefined;

                crawlerLog.info(`Processing ${label} page: ${request.url}`, {
                    label,
                    pageNo,
                    loadedUrl: request.loadedUrl,
                    statusCode: statusCodeMeta,
                });

                if (label === 'LIST') {
                    stats.listPagesProcessed += 1;

                    if (debug) {
                        try {
                            const outDir = 'debug/html';
                            await fs.mkdir(outDir, { recursive: true });
                            const fileName = `list-page-${pageNo}-${Date.now()}.html`;
                            const html = $.root().html() || '';
                            await fs.writeFile(`${outDir}/${fileName}`, html, 'utf8');
                            stats.htmlSavedFiles += 1;
                        } catch (err) {
                            crawlerLog.error('Failed to save debug HTML for LIST page', { err });
                        }
                    }

                    const jobCards = $('article, .job, .job-result, .job-card, .job-item')
                        .filter((_, el) => $(el).find('a[href*="/job/"]').length > 0)
                        .toArray();

                    const toEnqueue = [];

                    for (const card of jobCards) {
                        const cardEl = $(card);
                        const linkEl =
                            cardEl.find('a[href*="/job/"]').first() ||
                            cardEl.find('a[href*="/jobs/"]').first();
                        const href = linkEl.attr('href');
                        const absUrl = toAbs(href, request.url);
                        if (!absUrl) {
                            stats.droppedByUrlMissing += 1;
                            continue;
                        }

                        if (seenListingUrls.has(absUrl) || globalSeenUrls.has(absUrl)) {
                            stats.skippedDuplicateUrl += 1;
                            continue;
                        }

                        const title =
                            normalizeText(linkEl.text()) ||
                            normalizeText(cardEl.find('h2, h3, .job-title').first().text());
                        if (!title) {
                            stats.droppedByTitleMissing += 1;
                            continue;
                        }

                        const company = normalizeText(
                            cardEl
                                .find('.job-company, .company, .job__company, [class*="company-name"]')
                                .first()
                                .text(),
                        );

                        const locationText = normalizeText(
                            cardEl.find('.job-location, .location, [class*="job-location"]').first().text(),
                        );
                        const locationNormalized = normalizeLocation(locationText);

                        if (!locationNormalized) {
                            stats.droppedByLocationMissing += 1;
                        }

                        const postedText = normalizeText(
                            cardEl
                                .find(
                                    '.job-date, .posted-date, time, .job__date, [class*="date"], [datetime]',
                                )
                                .first()
                                .text(),
                        );
                        const postedDateFromRelative = parseRelativeDate(postedText);

                        let postedAt = postedDateFromRelative;
                        const nowMs = Date.now();

                        if (recencyWindowMs != null && postedAt) {
                            const ageMs = nowMs - postedAt.getTime();
                            if (ageMs > recencyWindowMs) {
                                stats.droppedByRecency += 1;
                                continue;
                            }
                        }

                        const idMatch = absUrl.match(/\/job\/([^/?#]+)/i);
                        const jobId = idMatch ? idMatch[1] : undefined;

                        const stub = {
                            url: absUrl,
                            title,
                            company: company || null,
                            location: locationNormalized || locationText || null,
                            listedAt: postedAt ? postedAt.toISOString() : null,
                            listedAtText: postedText || null,
                            jobId: jobId || null,
                            source: 'caterer.com',
                            sourcePage: request.url,
                            crawledAt: new Date().toISOString(),
                            salaryText: normalizeText(
                                cardEl
                                    .find(
                                        '.salary, .job-salary, [class*="salary"], [data-testid*="salary"]',
                                    )
                                    .first()
                                    .text(),
                            ),
                        };

                        pendingListings.set(absUrl, stub);
                        seenListingUrls.add(absUrl);
                        globalSeenUrls.add(absUrl);
                        stats.listingStubsCreated += 1;

                        if (collectDetails) {
                            toEnqueue.push({
                                url: absUrl,
                                userData: { label: 'DETAIL', referrer: request.url },
                            });
                        } else {
                            if (saved >= RESULTS_WANTED) continue;
                            await Dataset.pushData(stub);
                            saved += 1;
                            stats.totalSaved = saved;
                        }
                    }

                    if (collectDetails && toEnqueue.length > 0) {
                        const existing = pendingListings.size;
                        await enqueueLinks({ requests: toEnqueue });
                        if (debug) {
                            crawlerLog.debug('Enqueued detail pages from LIST', {
                                count: toEnqueue.length,
                                existingPending: existing,
                                newPending: pendingListings.size,
                            });
                        }
                        if (!Number.isFinite(stats.detailPagesEnqueued)) {
                            stats.detailPagesEnqueued = 0;
                        }
                        stats.detailPagesEnqueued += toEnqueue.length;
                    }

                    // *** UPDATED: pagination uses only actually saved jobs, not pendingListings.size ***
                    if (saved < RESULTS_WANTED && pageNo < MAX_PAGES) {
                        const next = findNextPage($, request.url);
                        if (next && next !== request.url) {
                            crawlerLog.info(`Pagination: Moving to page ${pageNo + 1}`, {
                                nextUrl: next,
                                currentPage: pageNo,
                                totalSaved: saved,
                                maxPages: MAX_PAGES,
                            });
                            await enqueueLinks({
                                urls: [next],
                                userData: { label: 'LIST', pageNo: pageNo + 1, referrer: request.url },
                            });
                            stats.listPagesEnqueued += 1;
                        } else {
                            crawlerLog.info('Pagination complete - no next page found or next equals current', {
                                currentPage: pageNo,
                                totalSaved: saved,
                                maxPages: MAX_PAGES,
                                nextUrl: next,
                            });
                        }
                    } else if (pageNo >= MAX_PAGES) {
                        crawlerLog.info('Max pages limit reached', {
                            maxPages: MAX_PAGES,
                            currentPage: pageNo,
                        });
                    }
                    // Pagination logic completed, continue to next request
                }

                if (label === 'DETAIL') {
                    stats.detailPagesProcessed += 1;
                    if (saved >= RESULTS_WANTED) {
                        crawlerLog.info('Results limit reached, skipping detail page');
                        return;
                    }

                    try {
                        crawlerLog.info(`Processing DETAIL page: ${request.url}`);

                        const listingStub = pendingListings.get(request.url);
                        if (listingStub) pendingListings.delete(request.url);

                        const jsonLdScripts = $('script[type="application/ld+json"]').toArray();
                        let jobFromJsonLd = null;

                        for (const script of jsonLdScripts) {
                            try {
                                const jsonText = $(script).contents().text() || $(script).text() || '';
                                if (!jsonText.trim()) continue;

                                let data;
                                try {
                                    data = JSON.parse(jsonText);
                                } catch {
                                    const fixed = jsonText.replace(/,\s*}/g, '}').replace(/,\s*]/g, ']');
                                    data = JSON.parse(fixed);
                                }

                                const candidates = Array.isArray(data) ? data : [data];
                                for (const item of candidates) {
                                    if (!item || typeof item !== 'object') continue;
                                    if (
                                        item['@type'] === 'JobPosting' ||
                                        (Array.isArray(item['@type']) &&
                                            item['@type'].includes('JobPosting'))
                                    ) {
                                        jobFromJsonLd = item;
                                        break;
                                    }
                                }
                                if (jobFromJsonLd) break;
                            } catch (err) {
                                if (debug) {
                                    crawlerLog.debug('Failed to parse JSON-LD script', { err });
                                }
                            }
                        }

                        const titleFromLd =
                            (jobFromJsonLd && (jobFromJsonLd.title || jobFromJsonLd.name)) || null;
                        const titleFromPage = normalizeText(
                            $('h1, .job-title, .job__title').first().text(),
                        );
                        const finalTitleFromPage = titleFromLd || titleFromPage;

                        let description = null;
                        if (jobFromJsonLd && jobFromJsonLd.description) {
                            description = cleanDescription(jobFromJsonLd.description);
                        }
                        if (!description) {
                            description = cleanDescription(
                                $(
                                    '.job-description, #job-description, .job-body, .job__body, main, article',
                                ).html() ||
                                    $('body').html() ||
                                    '',
                            );
                        }

                        let company = null;
                        if (jobFromJsonLd && jobFromJsonLd.hiringOrganization) {
                            const org = jobFromJsonLd.hiringOrganization;
                            if (typeof org === 'string') company = org;
                            else if (org.name) company = org.name;
                        }
                        if (!company) {
                            company = normalizeText(
                                $('[class*="company"], .job__company, .job-company')
                                    .first()
                                    .text(),
                            );
                        }

                        let location = null;
                        let rawLocationText = null;

                        if (jobFromJsonLd && jobFromJsonLd.jobLocation) {
                            const jobLocation = Array.isArray(jobFromJsonLd.jobLocation)
                                ? jobFromJsonLd.jobLocation[0]
                                : jobFromJsonLd.jobLocation;
                            if (jobLocation && typeof jobLocation === 'object') {
                                const address = jobLocation.address || jobLocation.addressLocality;
                                if (typeof address === 'string') {
                                    rawLocationText = address;
                                } else if (address && typeof address === 'object') {
                                    const parts = [
                                        address.addressLocality,
                                        address.addressRegion,
                                        address.addressCountry,
                                    ]
                                        .filter(Boolean)
                                        .join(', ');
                                    rawLocationText = parts || null;
                                }
                            }
                        }

                        if (!rawLocationText) {
                            rawLocationText = normalizeText(
                                $('.job-location, .location, [class*="location"]')
                                    .first()
                                    .text(),
                            );
                        }

                        if (listingStub && listingStub.location && !rawLocationText) {
                            rawLocationText = listingStub.location;
                        }

                        location = normalizeLocation(rawLocationText) || rawLocationText || null;

                        let salaryText = null;
                        if (jobFromJsonLd && jobFromJsonLd.baseSalary) {
                            const base = jobFromJsonLd.baseSalary;
                            if (typeof base === 'string') salaryText = base;
                            else if (base.value) {
                                const value = base.value;
                                if (typeof value === 'string') salaryText = value;
                                else if (typeof value === 'object') {
                                    const min = value.minValue;
                                    const max = value.maxValue;
                                    const unit = value.unitText || 'YEAR';
                                    const currency =
                                        base.currency || value.currency || 'GBP';
                                    const parts = [];
                                    if (min != null && max != null)
                                        parts.push(`${min} - ${max}`);
                                    else if (min != null) parts.push(`${min}+`);
                                    else if (max != null) parts.push(`${max}`);
                                    parts.push(currency);
                                    parts.push(unit);
                                    salaryText = parts.join(' ');
                                }
                            }
                        }

                        if (!salaryText && listingStub && listingStub.salaryText) {
                            salaryText = listingStub.salaryText;
                        }

                        const normalizedSalary = parseSalaryFromText(salaryText || '');

                        let postedAtIso = listingStub && listingStub.listedAt;
                        let postedAtTextFinal = listingStub && listingStub.listedAtText;

                        if (jobFromJsonLd && jobFromJsonLd.datePosted) {
                            const dt = new Date(jobFromJsonLd.datePosted);
                            if (!Number.isNaN(dt.getTime())) {
                                postedAtIso = dt.toISOString();
                                postedAtTextFinal =
                                    jobFromJsonLd.datePosted ||
                                    listingStub?.listedAtText ||
                                    null;
                            }
                        }

                        if (!postedAtIso && postedAtTextFinal) {
                            const fromRel = parseRelativeDate(postedAtTextFinal);
                            if (fromRel) postedAtIso = fromRel.toISOString();
                        }

                        let jobType = extractJobType($, 'body');

                        let image = null;
                        if (jobFromJsonLd && jobFromJsonLd.image) {
                            if (typeof jobFromJsonLd.image === 'string') {
                                image = jobFromJsonLd.image;
                            } else if (Array.isArray(jobFromJsonLd.image)) {
                                image = jobFromJsonLd.image[0];
                            } else if (jobFromJsonLd.image.url) {
                                image = jobFromJsonLd.image.url;
                            }
                        }

                        if (!image) {
                            const metaImage =
                                $('meta[property="og:image"]').attr('content') ||
                                $('meta[name="twitter:image"]').attr('content');
                            if (metaImage) image = metaImage;
                        }

                        let applyUrl = null;
                        if (jobFromJsonLd && jobFromJsonLd.hiringOrganization) {
                            const org = jobFromJsonLd.hiringOrganization;
                            if (typeof org === 'object') {
                                applyUrl =
                                    org.sameAs ||
                                    org.url ||
                                    jobFromJsonLd.directApplyUrl ||
                                    null;
                            }
                        }
                        if (!applyUrl) {
                            const applyLink =
                                $('a:contains("Apply"), button:contains("Apply")')
                                    .filter((_, el) => {
                                        const text =
                                            $(el).text().trim().toLowerCase();
                                        return (
                                            text.includes('apply') ||
                                            text.includes('apply now') ||
                                            text.includes('apply for')
                                        );
                                    })
                                    .first() || null;
                            if (applyLink && applyLink.length > 0) {
                                applyUrl = toAbs(applyLink.attr('href'), request.url);
                            }
                        }

                        const record = {
                            url: request.url,
                            title: finalTitleFromPage || listingStub?.title || null,
                            company: company || listingStub?.company || null,
                            location: location || listingStub?.location || null,
                            description: description || null,
                            listedAt: postedAtIso || null,
                            listedAtText: postedAtTextFinal || null,
                            jobType: jobType || null,
                            salaryText: salaryText || null,
                            salary: normalizedSalary || null,
                            applyUrl: applyUrl || null,
                            image: image || null,
                            jobId: listingStub?.jobId || null,
                            crawledAt: new Date().toISOString(),
                            source: 'caterer.com',
                            sourcePage: listingStub?.sourcePage || null,
                            debug: debug
                                ? {
                                      fromJsonLd: !!jobFromJsonLd,
                                      hasListingStub: !!listingStub,
                                      rawLocationText,
                                      postedWithinLabel,
                                  }
                                : undefined,
                        };

                        await Dataset.pushData(record);
                        saved += 1;
                        stats.totalSaved = saved;

                        if (saveRawHtml) {
                            try {
                                const html = $.root().html() || '';
                                await Actor.setValue(
                                    `HTML-${saved}-${Date.now()}`,
                                    {
                                        url: request.url,
                                        html,
                                        record,
                                    },
                                );
                                stats.htmlSavedFiles += 1;
                            } catch (err) {
                                crawlerLog.error('Failed to save raw HTML for DETAIL page', {
                                    err,
                                    url: request.url,
                                });
                            }
                        }
                    } catch (err) {
                        crawlerLog.error('Failed to process DETAIL page', {
                            url: request.url,
                            err,
                        });
                        if (session) session.markBad();
                    }
                }

                if (saved >= RESULTS_WANTED) {
                    crawlerLog.info('Results limit reached globally. No further pagination enqueues.');
                }
            },
        });

        await crawler.run(
            startUrlsToUse.map((u) => ({
                url: u,
                userData: { label: 'LIST', pageNo: 1, referrer: null },
            })),
        );

        await persistState();

        log.info('Crawl finished', {
            stats,
            totalSaved: saved,
            postedWithinLabel,
            RESULTS_WANTED,
            MAX_PAGES,
        });

        await Actor.exit();
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
            log.error('Fatal error in main(), and failed to log details', logErr);
            console.error('Fatal error in main():', error);
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

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
