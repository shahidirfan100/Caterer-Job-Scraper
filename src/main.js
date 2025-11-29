// src/main.js
import { Actor, log, Dataset } from 'apify';
import { PlaywrightCrawler, RequestQueue } from 'crawlee';

/**
 * Small helpers
 */
const cleanText = (val) => {
    if (!val) return null;
    return val.replace(/\s+/g, ' ').trim() || null;
};

const guessStartUrl = ({ keyword, location, startUrl }) => {
    if (startUrl && startUrl.trim()) return startUrl.trim();

    const base = 'https://www.caterer.com/jobs/search';
    const params = new URLSearchParams();
    if (keyword) params.set('keywords', keyword.trim());
    if (location) params.set('location', location.trim());
    const url = `${base}?${params.toString()}`;
    return url;
};

const buildNextPageUrl = (currentUrl, nextPageNum) => {
    const url = new URL(currentUrl);
    // Caterer uses ?page=2 on listing URLs
    url.searchParams.set('page', String(nextPageNum));
    return url.toString();
};

/**
 * Extract jobs from a listing page using ONLY browser DOM & text,
 * no external libraries (robust against CSS noise).
 *
 * We deliberately:
 *  - Take title from the <a> text only (no CSS junk).
 *  - Derive company/location/salary/description from innerText lines.
 */
const extractJobsFromListingPage = async (page, requestUrl) => {
    return page.evaluate((pageUrl) => {
        const jobs = [];

        const jobAnchors = Array
            .from(document.querySelectorAll('h2 a[href*="/job/"]'))
            // avoid header links or recommended widgets if possible
            .filter((a) => a.href.includes('/job/'));

        for (const anchor of jobAnchors) {
            // Try to get a reasonable "card" container
            const card =
                anchor.closest('article') ||
                anchor.closest('li') ||
                anchor.closest('[class*="job"]') ||
                anchor.parentElement?.parentElement ||
                anchor.parentElement;

            if (!card) continue;

            const title = anchor.innerText || anchor.textContent || '';
            const url = anchor.href;

            // All text lines from the card
            const linesRaw = (card.innerText || '')
                .split('\n')
                .map((l) => l.trim())
                .filter((l) => !!l);

            if (!linesRaw.length) continue;

            // Normalize & de-duplicate consecutive duplicates
            const lines = [];
            for (const line of linesRaw) {
                if (lines.length === 0 || lines[lines.length - 1] !== line) {
                    lines.push(line);
                }
            }

            // 0 = title
            const titleLine = title.trim();
            if (!titleLine) continue;

            // Find index of our title in lines, then derive fields from subsequent lines.
            const titleIdx = lines.findIndex((l) => l === titleLine);
            if (titleIdx === -1) continue;

            // Heuristic: company is next non-money line
            let company = null;
            let location = null;
            let salary = null;
            let postedAt = null;
            let description = null;

            const isMoneyLine = (line) =>
                /£|\bper annum\b|\bper hour\b|\bper year\b|\bUp To\b/i.test(line);

            const isPostedLine = (line) =>
                /\b(ago|Today|Yesterday)\b/i.test(line);

            // Company guess
            for (let i = titleIdx + 1; i < lines.length; i++) {
                const line = lines[i];
                if (isMoneyLine(line) || isPostedLine(line)) continue;
                company = line;
                break;
            }

            // Location guess: first line after company that looks like a place
            const companyIdx = company ? lines.indexOf(company, titleIdx + 1) : -1;
            for (let i = companyIdx + 1; i < lines.length; i++) {
                const line = lines[i];
                if (isMoneyLine(line) || isPostedLine(line)) continue;
                // crude location heuristic: comma or postcode-like or capitalised words
                if (
                    /[A-Z]{1,2}\d[\dA-Z]? ?\d[A-Z]{2}/.test(line) || // UK postcode-ish
                    line.includes(',') ||
                    /\b(UK|London|Manchester|Birmingham|Scotland|Wales|England)\b/i.test(line)
                ) {
                    location = line;
                    break;
                }
            }

            // Salary: first money-like line after location
            const locIdx = location ? lines.indexOf(location, (companyIdx + 1) || 0) : companyIdx;
            for (let i = locIdx + 1; i < lines.length; i++) {
                const line = lines[i];
                if (isMoneyLine(line)) {
                    salary = line;
                    break;
                }
            }

            // Posted at: first "x hours ago" type line near bottom
            for (let i = lines.length - 1; i >= 0; i--) {
                if (isPostedLine(lines[i])) {
                    postedAt = lines[i];
                    break;
                }
            }

            // Description: lines between salary and "more"/posted line
            const descLines = [];
            const salaryIdx = salary ? lines.indexOf(salary) : -1;
            const stopTokens = ['more', 'NEW', 'FEATURED', 'NEWFEATURED'];

            if (salaryIdx !== -1) {
                for (let i = salaryIdx + 1; i < lines.length; i++) {
                    const line = lines[i];
                    if (
                        stopTokens.includes(line.toUpperCase()) ||
                        isPostedLine(line)
                    ) break;

                    // Avoid duplicating the salary line or pure labels
                    if (!isMoneyLine(line)) {
                        descLines.push(line);
                    }
                }
            }

            const uniqDescLines = Array.from(new Set(descLines));
            const descriptionText = uniqDescLines.join(' ');

            jobs.push({
                title: titleLine,
                url,
                // no access to process.env here, but send current page URL for debugging
                listingPageUrl: pageUrl,
                company: company || null,
                location: location || null,
                salary: salary || null,
                description: descriptionText || null,
                postedAt: postedAt || null,
            });
        }

        return jobs;
    }, requestUrl);
};

/**
 * MAIN
 */
await Actor.main(async () => {
    const input = (await Actor.getInput()) || {};

    const {
        keyword = '',
        location = '',
        startUrl: inputStartUrl = '',
        results_wanted = 50,
        max_pages = 20,
        maxConcurrency = 4,
        collectDetails = true, // kept for compatibility but NOT used (we already have description from list)
        proxyConfiguration: proxyInput,
    } = input;

    const targetResults = Number.isFinite(results_wanted) && results_wanted > 0
        ? results_wanted
        : 50;

    const maxPages = Number.isFinite(max_pages) && max_pages > 0
        ? max_pages
        : 20;

    const startUrl = guessStartUrl({ keyword, location, startUrl: inputStartUrl });

    log.info('Starting Caterer.com PlaywrightCrawler scraper (listing-only & fast)', {
        keyword,
        location,
        startUrl,
        results_wanted: targetResults,
        max_pages: maxPages,
        maxConcurrency,
        collectDetails,
    });

    const proxyConfiguration = await Actor.createProxyConfiguration(proxyInput || {});
    log.info('Proxy configured', { usingCustom: !!proxyInput });

    const requestQueue = await RequestQueue.open();
    await requestQueue.addRequest({
        url: startUrl,
        userData: {
            label: 'LIST',
            pageNum: 1,
        },
    });

    let jobsSaved = 0;

    const crawler = new PlaywrightCrawler({
        requestQueue,
        proxyConfiguration,
        maxConcurrency,
        navigationTimeoutSecs: 25,
        maxRequestRetries: 2,
        useSessionPool: true,
        persistCookiesPerSession: true,

        // Make it stealthy and light
        browserPoolOptions: {
            // Use real-world fingerprints
            fingerprintOptions: {
                useFingerprintCache: true,
                fingerprintCacheSize: 5000,
                fingerprintGeneratorOptions: {
                    devices: ['desktop'],
                    browsers: ['chrome'],
                    operatingSystems: ['windows', 'macos'],
                },
            },
        },

        launchContext: {
            launchOptions: {
                headless: true,
                viewport: { width: 1280, height: 800 },
            },
        },

        preNavigationHooks: [
            async ({ page, request, session }, gotoOptions) => {
                // Block junk resources
                await page.route('**/*', (route) => {
                    const r = route.request();
                    const type = r.resourceType();
                    const url = r.url();

                    if (
                        ['image', 'media', 'font', 'stylesheet'].includes(type) ||
                        url.includes('google-analytics.com') ||
                        url.includes('doubleclick.net') ||
                        url.includes('facebook.com/tr') ||
                        url.includes('hotjar.com') ||
                        url.includes('segment.io')
                    ) {
                        return route.abort();
                    }

                    return route.continue();
                });

                gotoOptions.waitUntil = 'domcontentloaded';
                gotoOptions.timeout = 25000;
            },
        ],

        async requestHandler({ page, request, log: crawlerLog }) {
            const { label, pageNum } = request.userData;
            if (label !== 'LIST') {
                crawlerLog.info('Unknown label, skipping', { label, url: request.url });
                return;
            }

            const currentPageNum = pageNum || 1;

            // Hard stop if we already have enough jobs
            if (jobsSaved >= targetResults) {
                crawlerLog.info('Target results already reached, skipping request', {
                    url: request.url,
                    jobsSaved,
                    targetResults,
                });
                return;
            }

            crawlerLog.info('Processing listing page', {
                url: request.url,
                pageNum: currentPageNum,
            });

            await page.goto(request.url, { waitUntil: 'domcontentloaded', timeout: 25000 });

            // Wait for at least one job heading to appear
            await page.waitForTimeout(1000); // small delay to allow SSR content to stabilise

            const jobsOnPage = await extractJobsFromListingPage(page, request.url);
            const remaining = targetResults - jobsSaved;
            const toTake = Math.min(remaining, jobsOnPage.length);

            const jobsToSave = jobsOnPage.slice(0, toTake).map((job) => ({
                ...job,
                // include some INPUT metadata in each item
                searchKeyword: keyword || null,
                searchLocation: location || null,
            }));

            if (jobsToSave.length) {
                await Dataset.pushData(jobsToSave);
                jobsSaved += jobsToSave.length;

                crawlerLog.info(
                    `Saved ${jobsToSave.length} jobs from page ${currentPageNum}/${maxPages}`,
                    {
                        url: request.url,
                        pageNum: currentPageNum,
                        jobsSaved,
                        targetResults,
                    },
                );
            } else {
                crawlerLog.warning('No jobs extracted from listing page', {
                    url: request.url,
                    pageNum: currentPageNum,
                });
            }

            // Pagination: only queue next page if:
            //  - We still need more results
            //  - We haven't hit max_pages yet
            if (jobsSaved < targetResults && currentPageNum < maxPages) {
                const nextPageNum = currentPageNum + 1;
                const nextUrl = buildNextPageUrl(request.url, nextPageNum);

                await requestQueue.addRequest({
                    url: nextUrl,
                    userData: {
                        label: 'LIST',
                        pageNum: nextPageNum,
                    },
                });

                crawlerLog.info('Queued next listing page', {
                    nextPage: nextPageNum,
                    nextUrl,
                });
            } else {
                crawlerLog.info('Pagination finished or target reached', {
                    pageNum: currentPageNum,
                    jobsSaved,
                    targetResults,
                    max_pages: maxPages,
                });
            }
        },

        failedRequestHandler: async ({ request, error, log: crawlerLog }) => {
            // Mark session as bad for PlaywrightCrawler so it rotates proxy/fingerprint
            crawlerLog.error('Request failed after retries', {
                url: request.url,
                message: error?.message || String(error),
            });
        },
    });

    await crawler.run();

    log.info('Scraping completed', {
        jobsSaved,
        targetResults,
    });
});
