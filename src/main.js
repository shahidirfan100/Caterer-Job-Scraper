// src/main.js
import { Actor, log } from 'apify';
import { Dataset } from 'crawlee';
import { chromium } from 'playwright';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

const BASE_URL = 'https://www.caterer.com';

const buildSearchUrl = (keyword, location, page = 1) => {
    const url = new URL('/jobs/search', BASE_URL);
    if (keyword) url.searchParams.set('keywords', keyword);
    if (location) url.searchParams.set('location', location);
    if (page > 1) url.searchParams.set('page', String(page));
    return url.href;
};

const randomFingerprint = () => {
    const mobile = Math.random() < 0.35;
    if (mobile) {
        return {
            ua: 'Mozilla/5.0 (Linux; Android 14; Pixel 7 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
            viewport: { width: 390, height: 844 },
        };
    }
    return {
        ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        viewport: { width: 1366, height: 768 },
    };
};

const dismissPopupsPlaywright = async (page) => {
    const selectors = [
        'button:has-text("Accept all")',
        'button:has-text("Accept All")',
        '[id*="accept"][type="button"]',
        '[data-testid*="accept"]',
        '.js-accept-consent',
    ];
    for (const selector of selectors) {
        try {
            const btn = page.locator(selector).first();
            if (await btn.isVisible({ timeout: 1000 })) {
                await btn.click({ delay: 50 }).catch(() => {});
                break;
            }
        } catch {
            // ignore
        }
    }
};

const looksBlockedHtml = (html) => {
    const lower = html.toLowerCase();
    return ['captcha', 'access denied', 'verify you are a human', 'unusual traffic'].some((s) =>
        lower.includes(s),
    );
};

/**
 * Extract RecommenderWidget_listing_list JSON from HTML using regex.
 * Falls back to null if the structure changes.
 */
const extractRecommenderState = (html) => {
    const match = html.match(
        /__PRELOADED_STATE__\.RecommenderWidget_listing_list\s*=\s*(\{[\s\S]*?\});/,
    );
    if (!match) return null;
    try {
        return JSON.parse(match[1]);
    } catch {
        return null;
    }
};

/**
 * Parse jobs from the JSON state (preferred: clean salary, company, location, datePosted, url).
 */
const extractJobsFromJsonState = (html, pageUrl) => {
    const state = extractRecommenderState(html);
    if (!state?.props?.jobAdsData?.items?.length) return [];

    const urlObj = new URL(pageUrl);
    return state.props.jobAdsData.items.map((item) => {
        const fullUrl = new URL(item.url, urlObj.origin).href;

        return {
            source: 'caterer.com',
            job_id: item.id ?? null,
            title: item.title ?? null,
            company: item.companyName ?? null,
            location: item.location ?? null,
            salary: item.salary ?? null,
            date_posted: item.datePosted ?? null,
            url: fullUrl,
        };
    });
};

/**
 * Fallback: parse jobs from DOM using cheerio.
 */
const extractJobsFromDom = (html, pageUrl) => {
    const $ = cheerio.load(html);
    const urlObj = new URL(pageUrl);
    const seen = new Set();
    const jobs = [];

    $('a[href*="/job/"]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href) return;
        const jobUrl = new URL(href, urlObj.origin).href;
        if (seen.has(jobUrl)) return;

        const title = $(el).text().trim();
        if (!title || title.length < 3) return;

        const container =
            $(el).closest('article').length
                ? $(el).closest('article')
                : $(el).closest('li').length
                ? $(el).closest('li')
                : $(el).closest('div').length
                ? $(el).closest('div')
                : $(el).parent();

        const siblingTexts = [];
        container.find('span, p, div').each((__, node) => {
            const t = $(node).text().trim();
            if (t) siblingTexts.push(t);
        });

        const salary =
            siblingTexts.find((t) => t.includes('£')) ||
            siblingTexts.find((t) => /per (hour|annum|year)/i.test(t)) ||
            null;

        const company =
            siblingTexts.find(
                (t) =>
                    !t.includes('£') &&
                    !/ago$/i.test(t) &&
                    t.length <= 80 &&
                    /[A-Za-z]/.test(t),
            ) || null;

        const location =
            siblingTexts.find(
                (t) => /[A-Za-z]{3,}/.test(t) && /[,]/.test(t) && t.length <= 80,
            ) || null;

        jobs.push({
            source: 'caterer.com',
            job_id: null,
            title,
            company,
            location,
            salary,
            date_posted: null,
            url: jobUrl,
        });

        seen.add(jobUrl);
    });

    return jobs;
};

/**
 * Do one Playwright "handshake" to get cookies & prove we're a real browser.
 * Returns { userAgent, cookieHeader } which are then used by got-scraping.
 */
const doPlaywrightHandshake = async (
    startUrl,
    proxyConfiguration,
) => {
    const fp = randomFingerprint();
    const proxyUrl = proxyConfiguration ? await proxyConfiguration.newUrl() : null;

    for (let attempt = 1; attempt <= 2; attempt++) {
        try {
            const browser = await chromium.launch({
                headless: true,
                proxy: proxyUrl ? { server: proxyUrl } : undefined,
                args: [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--lang=en-GB,en-US',
                    '--disable-http2',
                    '--disable-features=UseChromeHttpsFirstMode',
                    '--disable-features=UseDnsHttpsSvcb',
                ],
            });

            const context = await browser.newContext({
                userAgent: fp.ua,
                viewport: fp.viewport,
                ignoreHTTPSErrors: true,
                locale: 'en-GB',
                extraHTTPHeaders: {
                    Accept:
                        'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8',
                    Connection: 'keep-alive',
                },
            });

            const page = await context.newPage();

            await page.addInitScript(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-GB', 'en-US', 'en'],
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3],
                });
                // eslint-disable-next-line no-undef
                window.chrome = { runtime: {} };
            });

            await dismissPopupsPlaywright(page);

            const response = await page.goto(startUrl, {
                waitUntil: 'domcontentloaded',
                timeout: 20000,
            });

            const status = response ? response.status() : null;
            const html = await page.content();

            if (status === 403 || looksBlockedHtml(html)) {
                throw new Error(`Blocked in handshake (status: ${status ?? 'N/A'})`);
            }

            const cookies = await context.cookies(BASE_URL);
            const cookieHeader = cookies
                .map((c) => `${c.name}=${c.value}`)
                .join('; ');

            await browser.close();

            log.info('Playwright handshake succeeded', {
                status,
                haveCookies: !!cookieHeader,
            });

            return { userAgent: fp.ua, cookieHeader, proxyUrl };
        } catch (err) {
            log.warning('Playwright handshake failed', {
                attempt,
                error: err.message,
            });
            if (attempt === 2) {
                log.warning(
                    'Falling back to HTTP-only scraping (got-scraping) without browser cookies.',
                );
                return { userAgent: fp.ua, cookieHeader: '', proxyUrl };
            }
        }
    }

    // Should never get here due to return in loop, but TS-style safety:
    const fallbackFp = randomFingerprint();
    return { userAgent: fallbackFp.ua, cookieHeader: '', proxyUrl: null };
};

await Actor.main(async () => {
    const input = (await Actor.getInput()) ?? {};
    const {
        keyword = '',
        location = '',
        startUrl = '',
        results_wanted = 20,
        max_pages = 5,
        proxyConfiguration: proxyFromInput,
    } = input;

    const startUrlToUse = startUrl?.trim() || buildSearchUrl(keyword, location, 1);

    log.info('Starting Caterer.com hybrid job scraper', {
        keyword,
        location,
        startUrl: startUrlToUse,
        results_wanted,
        max_pages,
    });

    // Proxy configuration
    const hasProxyCredentials =
        Boolean(proxyFromInput) || process.env.APIFY_PROXY_PASSWORD || process.env.APIFY_TOKEN;

    let proxyConfiguration = null;
    if (hasProxyCredentials) {
        try {
            proxyConfiguration = await Actor.createProxyConfiguration(
                proxyFromInput ?? {
                    groups: ['RESIDENTIAL'],
                    countryCode: 'GB',
                },
            );
            log.info('Proxy configured', { usingCustom: Boolean(proxyFromInput) });
        } catch (proxyError) {
            log.warning('Proxy setup failed, continuing without proxy', {
                error: proxyError.message,
            });
        }
    } else {
        log.info('No Apify proxy credentials detected, running without proxy');
    }

    // 1) One Playwright handshake to get cookies & UA
    const { userAgent, cookieHeader, proxyUrl } = await doPlaywrightHandshake(
        startUrlToUse,
        proxyConfiguration,
    );

    const stats = {
        pagesFetched: 0,
        pagesFailed: 0,
        jobsFromJson: 0,
        jobsFromDomFallback: 0,
        jobsSaved: 0,
        pagesBlockedOrCaptcha: 0,
    };

    const savedUrls = new Set();
    let savedCount = 0;

    // 2) Fast HTTP loop over listing pages using got-scraping
    const pagesToVisit = Math.max(1, Math.min(max_pages, 20)); // safety cap

    for (let pageNum = 1; pageNum <= pagesToVisit; pageNum++) {
        if (savedCount >= results_wanted) break;

        const url =
            pageNum === 1 ? startUrlToUse : buildSearchUrl(keyword, location, pageNum);
        log.info(`Fetching listing page ${pageNum}/${pagesToVisit}`, { url });

        try {
            const res = await gotScraping({
                url,
                proxyUrl: proxyUrl || undefined,
                timeout: { request: 15000 },
                http2: false, // HTTP/2 off often helps with finicky sites
                headers: {
                    'User-Agent': userAgent,
                    Accept:
                        'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8',
                    Connection: 'keep-alive',
                    Referer: BASE_URL + '/',
                    ...(cookieHeader ? { Cookie: cookieHeader } : {}),
                },
            });

            const { statusCode, body } = res;
            const html = body || '';

            if (statusCode === 403 || looksBlockedHtml(html)) {
                stats.pagesBlockedOrCaptcha++;
                log.warning('Blocked on HTTP (listing page)', {
                    url,
                    statusCode,
                });
                break; // stop further pages, we’re clearly being blocked
            }

            stats.pagesFetched++;

            // Prefer JSON state
            let jobs = extractJobsFromJsonState(html, url);
            stats.jobsFromJson += jobs.length;

            // Fallback to DOM via cheerio
            if (!jobs.length) {
                const domJobs = extractJobsFromDom(html, url);
                jobs = domJobs;
                stats.jobsFromDomFallback += domJobs.length;
                log.info(`DOM fallback extracted ${domJobs.length} jobs`, {
                    url,
                    pageNum,
                });
            } else {
                log.info(`JSON state extracted ${jobs.length} jobs`, {
                    url,
                    pageNum,
                });
            }

            if (!jobs.length) {
                log.info('No jobs found on page, stopping pagination.', {
                    url,
                    pageNum,
                });
                break;
            }

            // Save jobs, respecting results_wanted & de-duplicating
            for (const job of jobs) {
                if (savedCount >= results_wanted) break;
                if (!job.url || savedUrls.has(job.url)) continue;

                const finalJob = {
                    ...job,
                    keyword_search: keyword || null,
                    location_search: location || null,
                    extracted_at: new Date().toISOString(),
                };

                await Dataset.pushData(finalJob);
                savedUrls.add(job.url);
                savedCount++;
                stats.jobsSaved++;

                log.info(`Saved job ${savedCount}/${results_wanted}`, {
                    title: finalJob.title,
                    url: finalJob.url,
                });
            }

            // Small jitter between pages
            if (pageNum < pagesToVisit && savedCount < results_wanted) {
                const waitMs = 300 + Math.floor(Math.random() * 400);
                await Actor.sleep(waitMs);
            }
        } catch (err) {
            stats.pagesFailed++;
            log.warning('Failed to fetch listing page', {
                url,
                pageNum,
                error: err.message,
            });
            // If many pages fail, break; for now we just continue to next page.
        }
    }

    log.info('Scraping completed', {
        jobsSaved: savedCount,
        target: results_wanted,
        stats,
    });

    await Actor.setValue('STATS', stats);
});
