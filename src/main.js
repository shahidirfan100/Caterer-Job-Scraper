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
 * Parse job detail page HTML to get description (and optionally company/salary/date, but we
 * mainly care about description here).
 */
const extractJobDetail = (html) => {
    const $ = cheerio.load(html);
    const result = {};

    // 1) JSON-LD JobPosting
    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const json = $(el).text();
            const data = JSON.parse(json || '{}');
            const entries = Array.isArray(data) ? data : [data];

            for (const item of entries) {
                if (item['@type'] !== 'JobPosting') continue;

                if (item.description && !result.description_html) {
                    result.description_html = item.description;
                    result.description_text = item.description
                        .replace(/<[^>]*>/g, ' ')
                        .replace(/\s+/g, ' ')
                        .trim();
                }
                if (item.hiringOrganization?.name && !result.company) {
                    result.company = item.hiringOrganization.name;
                }
                if (item.baseSalary && !result.salary) {
                    const val = item.baseSalary.value;
                    if (typeof val === 'string') {
                        result.salary = val;
                    } else if (val?.value || val?.minValue || val?.maxValue) {
                        const v = val.value ?? val.minValue ?? val.maxValue;
                        result.salary = `${v} ${val.currency || ''}`.trim();
                    }
                }
                if (item.datePosted && !result.date_posted) {
                    result.date_posted = item.datePosted;
                }
                if (item.employmentType && !result.job_type) {
                    result.job_type = item.employmentType;
                }
            }
        } catch {
            // ignore
        }
    });

    // 2) DOM fallback for description
    if (!result.description_html) {
        const descEl =
            $('[data-at="job-description"]').first() ||
            $('.job-description').first() ||
            $('main article').first() ||
            $('main section').first();

        if (descEl && descEl.length) {
            result.description_html = descEl.html();
            result.description_text = descEl.text().replace(/\s+/g, ' ').trim();
        }
    }

    return result;
};

/**
 * Playwright handshake: one real browser visit to get cookies, UA, and the HTML of the first page.
 */
const doPlaywrightHandshake = async (startUrl, proxyConfiguration) => {
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

            await page.route('**/*', (route) => {
                const req = route.request();
                const type = req.resourceType();
                const url = req.url();
                if (['image', 'media', 'font'].includes(type)) {
                    return route.abort();
                }
                if (/\.(png|jpe?g|gif|webp|svg)(\?|$)/i.test(url)) {
                    return route.abort();
                }
                return route.continue();
            });

            const response = await page.goto(startUrl, {
                waitUntil: 'domcontentloaded',
                timeout: 20000,
            });

            await dismissPopupsPlaywright(page);

            const status = response ? response.status() : null;
            const html = await page.content();

            if (status === 403 || looksBlockedHtml(html)) {
                throw new Error(`Blocked in handshake (status: ${status ?? 'N/A'})`);
            }

            const cookies = await context.cookies(BASE_URL);
            const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ');

            await browser.close();

            log.info('Playwright handshake succeeded', {
                status,
                haveCookies: !!cookieHeader,
            });

            return {
                userAgent: fp.ua,
                cookieHeader,
                proxyUrl,
                initialHtml: html,
            };
        } catch (err) {
            log.warning('Playwright handshake failed', {
                attempt,
                error: err.message,
            });
            if (attempt === 2) {
                log.warning(
                    'Falling back to HTTP-only scraping without browser cookies. Blocking risk may increase.',
                );
                const fallbackFp = randomFingerprint();
                return {
                    userAgent: fallbackFp.ua,
                    cookieHeader: '',
                    proxyUrl: null,
                    initialHtml: null,
                };
            }
        }
    }
};

/**
 * HTTP fetch helper using got-scraping with browser-like headers + proxy.
 */
const httpFetchHtml = async (url, userAgent, cookieHeader, proxyUrl) => {
    const res = await gotScraping({
        url,
        proxyUrl: proxyUrl || undefined,
        timeout: { request: 15000 },
        http2: false,
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
        throw new Error(`Blocked on HTTP (status: ${statusCode})`);
    }

    return html;
};

/**
 * Enrich a set of jobs with detail data (description, maybe better salary/company/date).
 */
const enrichJobsWithDetails = async (jobs, userAgent, cookieHeader, proxyUrl, maxConcurrency = 3) => {
    const enriched = [];
    let index = 0;

    const worker = async () => {
        while (true) {
            const i = index++;
            if (i >= jobs.length) break;
            const baseJob = jobs[i];

            try {
                const html = await httpFetchHtml(baseJob.url, userAgent, cookieHeader, proxyUrl);
                const detail = extractJobDetail(html);

                const job = {
                    ...baseJob,
                    // Only override if detail provides something new
                    company: detail.company || baseJob.company || null,
                    salary: detail.salary || baseJob.salary || null,
                    date_posted: detail.date_posted || baseJob.date_posted || null,
                    job_type: detail.job_type || null,
                    description_html: detail.description_html || null,
                    description_text: detail.description_text || null,
                };

                enriched.push(job);
            } catch (err) {
                log.warning('Detail fetch failed, keeping list-only data', {
                    url: baseJob.url,
                    error: err.message,
                });
                enriched.push({
                    ...baseJob,
                    description_html: null,
                    description_text: null,
                });
            }
        }
    };

    const workers = [];
    const workersCount = Math.min(maxConcurrency, jobs.length || 1);
    for (let i = 0; i < workersCount; i++) {
        workers.push(worker());
    }
    await Promise.all(workers);

    return enriched;
};

await Actor.main(async () => {
    const input = (await Actor.getInput()) ?? {};
    const {
        keyword = '',
        location = '',
        startUrl = '',
        results_wanted = 20,
        max_pages = 5,
        max_detail_concurrency = 3,
        proxyConfiguration: proxyFromInput,
    } = input;

    const startUrlToUse = startUrl?.trim() || buildSearchUrl(keyword, location, 1);

    log.info('Starting Caterer.com HYBRID job scraper (with descriptions)', {
        keyword,
        location,
        startUrl: startUrlToUse,
        results_wanted,
        max_pages,
        max_detail_concurrency,
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

    // 1) Handshake
    const { userAgent, cookieHeader, proxyUrl, initialHtml } = await doPlaywrightHandshake(
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

    const pagesToVisit = Math.max(1, Math.min(max_pages, 20)); // safety cap
    let initialHtmlUsed = false;

    for (let pageNum = 1; pageNum <= pagesToVisit; pageNum++) {
        if (savedCount >= results_wanted) break;

        const url = pageNum === 1 ? startUrlToUse : buildSearchUrl(keyword, location, pageNum);
        log.info(`Processing listing page ${pageNum}/${pagesToVisit}`, { url });

        let html;
        try {
            if (pageNum === 1 && initialHtml && !initialHtmlUsed) {
                html = initialHtml;
                initialHtmlUsed = true;
                log.info('Using HTML from Playwright handshake for page 1');
            } else {
                html = await httpFetchHtml(url, userAgent, cookieHeader, proxyUrl);
            }
        } catch (err) {
            stats.pagesFailed++;
            if (String(err.message || '').includes('Blocked on HTTP')) {
                stats.pagesBlockedOrCaptcha++;
                log.warning('Blocked while fetching listing page, stopping pagination', {
                    url,
                    error: err.message,
                });
                break;
            }
            log.warning('Failed to fetch listing page', {
                url,
                pageNum,
                error: err.message,
            });
            continue;
        }

        stats.pagesFetched++;

        // Extract jobs from JSON or DOM
        let jobs = extractJobsFromJsonState(html, url);
        if (jobs.length) {
            stats.jobsFromJson += jobs.length;
            log.info(`Extracted ${jobs.length} jobs from JSON state`, {
                url,
                pageNum,
            });
        } else {
            const domJobs = extractJobsFromDom(html, url);
            jobs = domJobs;
            stats.jobsFromDomFallback += domJobs.length;
            log.info(`Extracted ${domJobs.length} jobs from DOM fallback`, {
                url,
                pageNum,
            });
        }

        if (!jobs.length) {
            log.info('No jobs found on page; stopping pagination.', { url, pageNum });
            break;
        }

        // Filter + cap jobs we actually need from this page
        const remainingNeeded = results_wanted - savedCount;
        const pageJobsToProcess = [];
        for (const job of jobs) {
            if (pageJobsToProcess.length >= remainingNeeded) break;
            if (!job.url || savedUrls.has(job.url)) continue;
            pageJobsToProcess.push(job);
            savedUrls.add(job.url); // reserve URL early to avoid duplicates across pages
        }

        if (!pageJobsToProcess.length) {
            log.info('No new jobs from this page (all duplicates or already enough).', {
                url,
                pageNum,
            });
            continue;
        }

        // Enrich with descriptions in parallel via HTTP
        log.info(`Fetching details for ${pageJobsToProcess.length} jobs`, {
            pageNum,
            concurrency: max_detail_concurrency,
        });

        const enrichedJobs = await enrichJobsWithDetails(
            pageJobsToProcess,
            userAgent,
            cookieHeader,
            proxyUrl,
            max_detail_concurrency,
        );

        for (const job of enrichedJobs) {
            if (savedCount >= results_wanted) break;

            const finalJob = {
                ...job,
                keyword_search: keyword || null,
                location_search: location || null,
                extracted_at: new Date().toISOString(),
            };

            await Dataset.pushData(finalJob);
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
    }

    log.info('Scraping completed', {
        jobsSaved: savedCount,
        target: results_wanted,
        stats,
    });

    await Actor.setValue('STATS', stats);
});
