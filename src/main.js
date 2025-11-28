// src/main.js
import { Actor, log } from 'apify';
import { Dataset, PlaywrightCrawler } from 'crawlee';
import { chromium } from 'playwright';

// Turn "3 days ago", "1 week ago", etc. into ISO timestamp.
const parsePostedRelative = (text) => {
    if (!text) return null;
    const lower = text.toLowerCase().replace('posted', '').replace('published', '').trim();

    if (lower.includes('today') || lower.includes('just now')) {
        return new Date().toISOString();
    }

    const match = lower.match(/(\d+)\s*(hour|day|week|month)s?\s*ago/);
    if (!match) return null;

    const num = Number(match[1]);
    const unit = match[2];

    const msMap = {
        hour: 3600000,
        day: 86400000,
        week: 604800000,
        month: 2592000000,
    };

    const ms = msMap[unit] ?? 0;
    if (!ms) return null;

    return new Date(Date.now() - num * ms).toISOString();
};

const buildSearchUrl = (keyword, location, page = 1) => {
    const url = new URL('https://www.caterer.com/jobs/search');
    if (keyword) url.searchParams.set('keywords', keyword);
    if (location) url.searchParams.set('location', location);
    if (page > 1) url.searchParams.set('page', String(page));
    return url.href;
};

const randomFingerprint = () => {
    const mobile = Math.random() < 0.35;
    if (mobile) {
        return {
            ua: 'Mozilla/5.0 (Linux; Android 14; Pixel 7 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36',
            viewport: { width: 390, height: 844 },
        };
    }
    return {
        ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        viewport: { width: 1366, height: 768 },
    };
};

const dismissPopups = async (page) => {
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

await Actor.main(async () => {
    const rawInput = (await Actor.getInput()) ?? {};
    const {
        keyword = '',
        location = '',
        startUrl = '',
        results_wanted = 20,
        max_pages = 5,
        collectDetails = true,
        proxyConfiguration: proxyFromInput,
    } = rawInput;

    const startUrlToUse = startUrl?.trim() || buildSearchUrl(keyword, location, 1);

    log.info('Starting Caterer.com job scraper', {
        keyword,
        location,
        startUrl: startUrlToUse,
        results_wanted,
        max_pages,
        collectDetails,
    });

    // Proxy configuration
    const hasProxyCredentials =
        Boolean(proxyFromInput) || process.env.APIFY_PROXY_PASSWORD || process.env.APIFY_TOKEN;

    let proxyConfiguration;
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
            log.warning('Proxy setup failed, continuing without proxy', { error: proxyError.message });
        }
    } else {
        log.info('No Apify proxy credentials detected, running without proxy');
    }

    const savedUrls = new Set();
    let savedCount = 0;
    let queuedDetailCount = 0;

    const stats = {
        listPagesProcessed: 0,
        detailPagesProcessed: 0,
        jobsExtractedFromState: 0,
        jobsExtractedFromDom: 0,
        jobsSaved: 0,
        pagesBlockedOrCaptcha: 0,
    };

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        // Make it faster but still safe:
        minConcurrency: 1,
        maxConcurrency: 4, // more parallel detail pages
        maxRequestsPerCrawl: max_pages * 40,
        maxRequestRetries: 3,
        requestHandlerTimeoutSecs: 50,
        navigationTimeoutSecs: 22,

        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 30,
            sessionOptions: {
                maxUsageCount: 6, // reuse sessions a bit more for speed
                maxAgeSecs: 600,
            },
        },

        launchContext: {
            launcher: chromium,
            launchOptions: {
                headless: true,
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
                ignoreHTTPSErrors: true,
            },
        },

        // Stealth + resource blocking
        preNavigationHooks: [
            async ({ page, request }) => {
                const fingerprint = randomFingerprint();

                try {
                    await page.setViewportSize(fingerprint.viewport);
                } catch {
                    // ignore
                }

                try {
                    await page.setExtraHTTPHeaders({
                        Accept:
                            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8',
                        Connection: 'keep-alive',
                        Referer: 'https://www.caterer.com/',
                    });
                } catch {
                    // ignore
                }

                try {
                    await page.setUserAgent(fingerprint.ua);
                } catch {
                    // ignore
                }

                // Block heavy resources
                try {
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
                } catch {
                    // ignore
                }

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

                log.debug('Navigating', { url: request.url });
            },
        ],

        async requestHandler({ request, page, log: crawlerLog, session, crawler: crawlerInstance }) {
            const { label = 'LIST', pageNum = 1, listData } = request.userData ?? {};
            await dismissPopups(page);

            const html = await page.content();
            const lowerHtml = html.toLowerCase();
            const mainResponse = page.mainFrame()?.response?.();

            let status;
            try {
                status = await mainResponse?.status?.();
            } catch {
                status = undefined;
            }

            const blocked = ['captcha', 'access denied', 'verify you are a human', 'unusual traffic'].some(
                (needle) => lowerHtml.includes(needle),
            );

            if (blocked || status === 403) {
                stats.pagesBlockedOrCaptcha++;
                try {
                    await Actor.setValue(
                        `BLOCKED_${Date.now()}.png`,
                        await page.screenshot({ fullPage: true }),
                        { contentType: 'image/png' },
                    );
                } catch {
                    // ignore
                }
                session?.retire();
                throw new Error(`Blocked or captcha detected (status: ${status ?? 'N/A'})`);
            }

            if (label === 'LIST') {
                stats.listPagesProcessed++;

                if (status && status >= 400) {
                    throw new Error(`Bad status on list page: ${status}`);
                }

                // Prefer JS state (fast, structured)
                await page
                    .waitForFunction(
                        () =>
                            !!(
                                window.__PRELOADED_STATE__ &&
                                window.__PRELOADED_STATE__.RecommenderWidget_listing_list &&
                                window.__PRELOADED_STATE__.RecommenderWidget_listing_list.props &&
                                window.__PRELOADED_STATE__.RecommenderWidget_listing_list.props
                                    .jobAdsData &&
                                window.__PRELOADED_STATE__.RecommenderWidget_listing_list.props.jobAdsData
                                    .items &&
                                window.__PRELOADED_STATE__.RecommenderWidget_listing_list.props.jobAdsData
                                    .items.length
                            ),
                        { timeout: 8000 },
                    )
                    .catch(() => {});

                const jobsFromState = await page.evaluate((pageUrl) => {
                    const results = [];
                    try {
                        const state = window.__PRELOADED_STATE__;
                        const props =
                            state?.RecommenderWidget_listing_list?.props ||
                            state?.SearchListing?.props ||
                            null;

                        const items = props?.jobAdsData?.items || [];
                        const urlObj = new URL(pageUrl);

                        for (const item of items) {
                            const fullUrl = new URL(item.url, urlObj.origin).href;

                            results.push({
                                title: item.title || null,
                                company: item.companyName || null,
                                location: item.location || null,
                                salary: item.salary || null,
                                date_posted: item.datePosted || null,
                                url: fullUrl,
                                description_html: null,
                                description_text: null,
                            });
                        }
                    } catch {
                        // ignore, fallback to DOM below
                    }
                    return results;
                }, request.url);

                let jobs = jobsFromState;
                if (jobs && jobs.length) {
                    stats.jobsExtractedFromState += jobs.length;
                    crawlerLog.info(`Extracted ${jobs.length} jobs from JS state`, {
                        url: request.url,
                        pageNum,
                    });
                } else {
                    // DOM fallback (slower but robust)
                    await page
                        .waitForSelector('a[href*="/job/"]', { timeout: 12000 })
                        .catch(() => null);

                    jobs = await page.evaluate((pageUrl) => {
                        const results = [];
                        const seen = new Set();
                        const anchors = Array.from(
                            document.querySelectorAll('main a[href*="/job/"], a[href*="/job/"]'),
                        );
                        const urlObj = new URL(pageUrl);

                        for (const anchor of anchors) {
                            const href = anchor.getAttribute('href');
                            if (!href) continue;

                            const jobUrl = new URL(href, urlObj.origin).href;
                            if (seen.has(jobUrl)) continue;

                            const titleText = anchor.textContent?.trim() || '';
                            if (!titleText || titleText.length < 3) continue;

                            const heading =
                                anchor.closest('h2, h3') ||
                                anchor.parentElement?.closest('h2, h3') ||
                                anchor.parentElement;

                            const container = heading?.parentElement || heading;
                            const siblings = [];
                            if (container) {
                                let el = container.nextElementSibling;
                                let steps = 0;
                                while (el && steps < 10) {
                                    siblings.push(el);
                                    el = el.nextElementSibling;
                                    steps++;
                                }
                            }

                            const siblingTexts = siblings
                                .map((el) => el.textContent?.trim())
                                .filter(Boolean);

                            const salaryText =
                                siblingTexts.find((t) => t.includes('£')) ||
                                siblingTexts.find((t) => /per (hour|annum|year)/i.test(t)) ||
                                null;

                            const rawDate =
                                siblingTexts.find(
                                    (t) =>
                                        /ago$/i.test(t) ||
                                        /last \d+ days?/i.test(t) ||
                                        /\bday\b|\bweek\b/i.test(t),
                                ) || null;

                            const locationText =
                                siblingTexts.find(
                                    (t) =>
                                        /\b[A-Z]{1,2}\d{1,2}\b/.test(t) ||
                                        /[A-Za-z]+,\s*[A-Za-z]/.test(t),
                                ) || null;

                            let company = null;
                            if (siblingTexts.length) {
                                const candidate = siblingTexts[0];
                                if (
                                    candidate &&
                                    !candidate.includes('£') &&
                                    !candidate.toLowerCase().includes('ago') &&
                                    candidate.length <= 80
                                ) {
                                    company = candidate;
                                }
                            }

                            results.push({
                                title: titleText,
                                company,
                                location: locationText,
                                salary: salaryText,
                                raw_date: rawDate,
                                url: jobUrl,
                                description_html: null,
                                description_text: null,
                            });

                            seen.add(jobUrl);
                        }

                        return results;
                    }, request.url);

                    stats.jobsExtractedFromDom += jobs.length;
                    crawlerLog.info(`Extracted ${jobs.length} jobs from DOM`, {
                        url: request.url,
                        pageNum,
                    });
                }

                // Save / enqueue details
                const detailRequests = [];

                for (const job of jobs) {
                    if (savedCount >= results_wanted) break;
                    if (queuedDetailCount >= results_wanted && collectDetails) break;

                    if (savedUrls.has(job.url)) continue;

                    // Clean & normalize date
                    if (job.raw_date && !job.date_posted) {
                        const iso = parsePostedRelative(job.raw_date);
                        if (iso) job.date_posted = iso;
                        delete job.raw_date;
                    }

                    if (collectDetails) {
                        detailRequests.push({
                            url: job.url,
                            userData: { label: 'DETAIL', listData: job },
                        });
                        queuedDetailCount++;
                    } else {
                        await Dataset.pushData(job);
                        savedUrls.add(job.url);
                        savedCount++;
                        stats.jobsSaved++;
                        crawlerLog.info(`Saved job ${savedCount}/${results_wanted}`, {
                            title: job.title,
                            url: job.url,
                        });
                    }
                }

                if (detailRequests.length) {
                    await crawlerInstance.addRequests(detailRequests);
                    crawlerLog.info('Queued detail pages', {
                        count: detailRequests.length,
                        queuedDetailCount,
                    });
                }

                // Only paginate if we still need more jobs queued
                if (
                    collectDetails
                        ? queuedDetailCount < results_wanted
                        : savedCount < results_wanted
                ) {
                    if (pageNum < max_pages) {
                        const nextPage = pageNum + 1;

                        const hasNext = await page.evaluate((nextNum) => {
                            const selector = `a[href*="page=${nextNum}"]`;
                            if (document.querySelector(selector)) return true;
                            const anchors = Array.from(document.querySelectorAll('a'));
                            return anchors.some((a) => a.textContent.trim() === String(nextNum));
                        }, nextPage);

                        if (hasNext) {
                            await crawlerInstance.addRequests([
                                {
                                    url: buildSearchUrl(keyword, location, nextPage),
                                    userData: { label: 'LIST', pageNum: nextPage },
                                },
                            ]);
                            crawlerLog.info('Queued next page', { nextPage });
                        } else {
                            crawlerLog.info('Pagination ended (no link for next page)');
                        }
                    }
                }

                // Small jitter to avoid obvious patterns
                await page.waitForTimeout(400 + Math.random() * 600);
            } else if (label === 'DETAIL') {
                if (!listData) return;
                if (savedCount >= results_wanted) return;
                if (savedUrls.has(listData.url)) return;

                stats.detailPagesProcessed++;

                await page.waitForLoadState('domcontentloaded', { timeout: 15000 }).catch(() => {});
                await dismissPopups(page);

                await page
                    .waitForSelector('h1, [data-at="job-description"], article', {
                        timeout: 10000,
                    })
                    .catch(() => {});

                const detailData = await page.evaluate(() => {
                    const result = {};

                    // 1) JSON-LD JobPosting (most reliable)
                    const scripts = Array.from(
                        document.querySelectorAll('script[type="application/ld+json"]'),
                    );
                    for (const script of scripts) {
                        try {
                            const data = JSON.parse(script.textContent || '{}');
                            const entries = Array.isArray(data) ? data : [data];

                            for (const item of entries) {
                                if (item['@type'] !== 'JobPosting') continue;

                                if (item.title && !result.title) result.title = item.title;
                                if (item.hiringOrganization?.name && !result.company) {
                                    result.company = item.hiringOrganization.name;
                                }
                                if (item.jobLocation?.address?.addressLocality && !result.location) {
                                    result.location = item.jobLocation.address.addressLocality;
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
                                if (item.employmentType && !result.job_type) {
                                    result.job_type = item.employmentType;
                                }
                                if (item.datePosted && !result.date_posted) {
                                    result.date_posted = item.datePosted;
                                }
                                if (item.description && !result.description_html) {
                                    result.description_html = item.description;
                                    result.description_text = item.description
                                        .replace(/<[^>]*>/g, ' ')
                                        .replace(/\s+/g, ' ')
                                        .trim();
                                }
                            }
                        } catch {
                            // malformed JSON-LD
                        }
                    }

                    // 2) DOM fallbacks
                    if (!result.title) {
                        result.title =
                            document.querySelector('h1')?.textContent?.trim() || null;
                    }

                    if (!result.company) {
                        const companyEl =
                            document.querySelector('[data-at*="company"], [class*="company"]') ||
                            document.querySelector('section h3');
                        result.company = companyEl?.textContent?.trim() || null;
                    }

                    if (!result.location) {
                        const locEl =
                            document.querySelector('[data-at*="location"], [class*="location"]') ||
                            document.querySelector('h1 + div, h1 + p');
                        result.location = locEl?.textContent?.trim() || null;
                    }

                    if (!result.salary) {
                        const salaryEl = Array.from(
                            document.querySelectorAll('span, p, li, div'),
                        ).find((el) => el.textContent.includes('£'));
                        if (salaryEl) {
                            let text = salaryEl.textContent || '';
                            text = text.replace(/\s+/g, ' ').trim();

                            // If it's ridiculously long / contains preloaded state, trim it down.
                            if (text.length > 150 || text.includes('window.__PRELOADED_STATE__')) {
                                const match = text.match(/£[^,]+/);
                                text = (match && match[0]) || text.slice(0, 150);
                            }

                            result.salary = text;
                        }
                    }

                    if (!result.description_html) {
                        const descEl =
                            document.querySelector(
                                '[data-at="job-description"], .job-description, article',
                            ) ||
                            document.querySelector('main article') ||
                            document.querySelector('main section');
                        if (descEl) {
                            result.description_html = descEl.innerHTML;
                            result.description_text =
                                descEl.textContent?.replace(/\s+/g, ' ').trim() || null;
                        }
                    }

                    return result;
                });

                const job = {
                    ...listData,
                    ...Object.fromEntries(
                        Object.entries(detailData).filter(([, value]) => value != null),
                    ),
                };

                // If list page had a relative date string but we still don't have date_posted
                if (!job.date_posted && job.raw_date) {
                    const parsed = parsePostedRelative(job.raw_date);
                    if (parsed) job.date_posted = parsed;
                }
                delete job.raw_date;

                await Dataset.pushData(job);
                savedUrls.add(job.url);
                savedCount++;
                stats.jobsSaved++;
                crawlerLog.info(`Saved detail job ${savedCount}/${results_wanted}`, {
                    title: job.title,
                    url: job.url,
                });
            }
        },

        async failedRequestHandler({ request, error, session, log: crawlerLog, page }) {
            const retryCount = request.retryCount ?? 0;
            crawlerLog.warn('Reclaiming failed request back to the list or queue.', {
                url: request.url,
                retryCount,
                message: error.message,
            });

            session?.retire();

            if (retryCount >= 2) {
                let shot;
                try {
                    if (page) {
                        shot = await page.screenshot({ fullPage: true });
                    }
                } catch {
                    // ignore
                }

                await Actor.setValue(
                    `FAILED_${Date.now()}.txt`,
                    `URL: ${request.url}\nError: ${error.message}\nStack: ${
                        error.stack ?? ''
                    }`,
                );
                if (shot) {
                    await Actor.setValue(`FAILED_${Date.now()}.png`, shot, {
                        contentType: 'image/png',
                    });
                }
            }
        },
    });

    log.info('Crawler created, starting run', { startUrl: startUrlToUse });

    await crawler.run([
        {
            url: startUrlToUse,
            userData: { label: 'LIST', pageNum: 1 },
        },
    ]);

    log.info('Scraping completed', {
        jobsSaved: savedCount,
        target: results_wanted,
        stats,
    });

    await Actor.setValue('STATS', stats);
});
