// src/main.js
import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { chromium } from 'playwright';

const BASE_URL = 'https://www.caterer.com';

/**
 * Parse relative posted dates like "3 days ago", "1 week ago".
 */
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

/**
 * Build a search URL when user did not provide a custom startUrl.
 */
const buildSearchUrl = (keyword, location, page = 1) => {
    const url = new URL('/jobs/search', BASE_URL);
    if (keyword) url.searchParams.set('keywords', keyword);
    if (location) url.searchParams.set('location', location);
    if (page > 1) url.searchParams.set('page', String(page));
    return url.href;
};

/**
 * Build URL for the next page by setting ?page=N on the current URL.
 */
const buildNextPageUrl = (currentUrl, nextPageNum) => {
    const url = new URL(currentUrl);
    url.searchParams.set('page', String(nextPageNum));
    return url.href;
};

/**
 * Random desktop / mobile fingerprint to rotate UA + viewport.
 */
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
            if (await btn.isVisible({ timeout: 1000 }).catch(() => false)) {
                await btn.click({ delay: 50 }).catch(() => {});
                break;
            }
        } catch {
            // ignore popup errors
        }
    }
};

await Actor.main(async () => {
    const input = (await Actor.getInput()) ?? {};
    const {
        keyword = '',
        location = '',
        startUrl = '',
        results_wanted = 20,
        max_pages = 5,
        collectDetails = true,
        proxyConfiguration: proxyFromInput,
    } = input;

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
        jobsFromState: 0,
        jobsFromDom: 0,
        jobsSaved: 0,
        pagesBlockedOrCaptcha: 0,
        listPagesFailed: 0,
        detailPagesFailed: 0,
    };

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        minConcurrency: 1,
        maxConcurrency: 4,
        maxRequestRetries: 2,
        maxRequestsPerCrawl: max_pages * 50,
        navigationTimeoutSecs: 25,
        requestHandlerTimeoutSecs: 60,

        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 30,
            sessionOptions: {
                maxUsageCount: 8,
                maxAgeSecs: 900,
            },
        },

        launchContext: {
            launcher: chromium,
            launchOptions: {
                headless: true,
                ignoreHTTPSErrors: true,
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
            },
        },

        preNavigationHooks: [
            async ({ page, request }) => {
                const fp = randomFingerprint();

                try {
                    await page.setViewportSize(fp.viewport);
                } catch {
                    // ignore
                }

                try {
                    await page.setExtraHTTPHeaders({
                        Accept:
                            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8',
                        Connection: 'keep-alive',
                        Referer: BASE_URL + '/',
                    });
                } catch {
                    // ignore
                }

                try {
                    await page.setUserAgent(fp.ua);
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

            const mainResponse = page.mainFrame()?.response?.();
            let status;
            try {
                status = await mainResponse?.status?.();
            } catch {
                status = undefined;
            }

            const html = await page.content();
            const lowerHtml = html.toLowerCase();
            const blocked = ['captcha', 'access denied', 'verify you are a human', 'unusual traffic'].some(
                (needle) => lowerHtml.includes(needle),
            );

            if (blocked || status === 403) {
                stats.pagesBlockedOrCaptcha++;
                try {
                    const screenshot = await page.screenshot({ fullPage: true });
                    await Actor.setValue(`BLOCKED_${Date.now()}.png`, screenshot, {
                        contentType: 'image/png',
                    });
                } catch {
                    // ignore
                }
                session?.retire();
                throw new Error(`Blocked or captcha detected (status: ${status ?? 'N/A'})`);
            }

            if (label === 'LIST') {
                stats.listPagesProcessed++;

                if (status && status >= 400) {
                    throw new Error(`Received bad status ${status}`);
                }

                // Prefer JS state (clean fields)
                let jobs = await page.evaluate((pageUrl) => {
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
                                source: 'caterer.com',
                                job_id: item.id ?? null,
                                title: item.title ?? null,
                                company: item.companyName ?? null,
                                location: item.location ?? null,
                                salary: item.salary ?? null,
                                date_posted: item.datePosted ?? null,
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

                if (jobs && jobs.length) {
                    stats.jobsFromState += jobs.length;
                    crawlerLog.info(`Extracted ${jobs.length} jobs from JS state`, {
                        url: request.url,
                        pageNum,
                    });
                } else {
                    // DOM fallback with cleanup
                    await page
                        .waitForSelector('a[href*="/job/"]', { timeout: 15000 })
                        .catch(() => null);

                    jobs = await page.evaluate((pageUrl) => {
                        const results = [];
                        const seen = new Set();
                        const urlObj = new URL(pageUrl);

                        const anchors = Array.from(
                            document.querySelectorAll('main a[href*="/job/"], a[href*="/job/"]'),
                        );

                        for (const anchor of anchors) {
                            const href = anchor.getAttribute('href');
                            if (!href) continue;

                            const jobUrl = new URL(href, urlObj.origin).href;
                            if (seen.has(jobUrl)) continue;

                            // Clone container and strip style/script to avoid CSS noise in text
                            const card =
                                anchor.closest('[data-at="job-item"]') ||
                                anchor.closest('article') ||
                                anchor.closest('li') ||
                                anchor.closest('div') ||
                                anchor.parentElement;

                            if (!card) continue;
                            const clone = card.cloneNode(true);
                            clone.querySelectorAll('style,script,noscript').forEach((el) => el.remove());

                            const titleEl =
                                clone.querySelector('h2 a[href*="/job/"]') ||
                                clone.querySelector('a[href*="/job/"]') ||
                                anchor;
                            const title = titleEl?.textContent?.replace(/\s+/g, ' ').trim() || '';
                            if (!title || title.length < 2) continue;

                            const fullText =
                                clone.textContent?.replace(/\s+/g, ' ').trim() || '';

                            // Salary: "£... per ..." or first £... segment
                            let salary = null;
                            const perMatch = fullText.match(
                                /(£[^£]+?per (?:hour|annum|year|week|day))/i,
                            );
                            if (perMatch) {
                                salary = perMatch[1].trim();
                            } else {
                                const simpleMatch = fullText.match(/(£[^£]+?)(?:\s{2,}|$)/);
                                if (simpleMatch) salary = simpleMatch[1].trim();
                            }

                            // Company: img alt or first short non-salary, non-title chunk
                            let company =
                                clone.querySelector('img[alt]')?.getAttribute('alt') || null;
                            if (!company || company.length < 2 || company.length > 80) {
                                const parts = fullText.split(/(?=[A-Z])/).map((p) => p.trim());
                                const candidate = parts.find(
                                    (p) =>
                                        p &&
                                        p !== title &&
                                        !p.includes('£') &&
                                        !/ago$/i.test(p) &&
                                        p.length <= 80,
                                );
                                if (candidate) company = candidate;
                            }

                            // Location: text between company and salary
                            let location = null;
                            if (company && salary) {
                                const idxCompany = fullText.indexOf(company);
                                const idxSalary = fullText.indexOf(salary);
                                if (idxCompany !== -1 && idxSalary !== -1 && idxSalary > idxCompany) {
                                    location = fullText
                                        .slice(idxCompany + company.length, idxSalary)
                                        .replace(/\s+/g, ' ')
                                        .trim();
                                }
                            }

                            results.push({
                                source: 'caterer.com',
                                job_id: null,
                                title,
                                company: company || null,
                                location: location || null,
                                salary: salary || null,
                                date_posted: null,
                                url: jobUrl,
                                description_html: null,
                                description_text: null,
                            });

                            seen.add(jobUrl);
                        }

                        return results;
                    }, request.url);

                    stats.jobsFromDom += jobs.length;
                    crawlerLog.info(`Extracted ${jobs.length} jobs from DOM`, {
                        url: request.url,
                        pageNum,
                    });
                }

                if (!jobs || !jobs.length) {
                    if (pageNum === 1) {
                        await Actor.setValue('DEBUG_HTML', html, { contentType: 'text/html' });
                    }
                    throw new Error('No jobs found on listing page');
                }

                const detailRequests = [];

                for (const job of jobs) {
                    if (savedCount >= results_wanted) break;
                    if (collectDetails && queuedDetailCount >= results_wanted) break;
                    if (!job.url || savedUrls.has(job.url)) continue;

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
                        crawlerLog.info(
                            `Saved job ${savedCount}/${results_wanted} (list only)`,
                            { title: job.title, url: job.url },
                        );
                    }
                }

                if (detailRequests.length) {
                    await crawlerInstance.addRequests(detailRequests);
                    crawlerLog.info('Queued detail pages', {
                        count: detailRequests.length,
                        queuedDetailCount,
                    });
                }

                // Pagination: only continue if we still need more
                const needMore = collectDetails
                    ? queuedDetailCount < results_wanted
                    : savedCount < results_wanted;

                if (needMore && pageNum < max_pages) {
                    const nextPage = pageNum + 1;

                    // Optional: check if a link to next page exists to avoid dead pages
                    const hasNext = await page.evaluate((nextNum) => {
                        const sel = `a[href*="page=${nextNum}"]`;
                        if (document.querySelector(sel)) return true;
                        const anchors = Array.from(document.querySelectorAll('a'));
                        return anchors.some(
                            (a) => a.textContent.trim() === String(nextNum),
                        );
                    }, nextPage);

                    if (hasNext) {
                        const nextUrl = buildNextPageUrl(request.url, nextPage);
                        await crawlerInstance.addRequests([
                            {
                                url: nextUrl,
                                userData: { label: 'LIST', pageNum: nextPage },
                            },
                        ]);
                        crawlerLog.info('Queued next page', { nextPage, nextUrl });
                    } else {
                        crawlerLog.info('Pagination ended (no next page link visible)', {
                            pageNum,
                        });
                    }
                }
            } else if (label === 'DETAIL') {
                if (!listData) return;
                if (savedCount >= results_wanted) return;
                if (savedUrls.has(listData.url)) return;

                stats.detailPagesProcessed++;

                await page.waitForLoadState('domcontentloaded', { timeout: 15000 }).catch(() => {});
                await dismissPopups(page);
                await page
                    .waitForSelector('h1, [data-at="job-description"], article', {
                        timeout: 12000,
                    })
                    .catch(() => {});

                const detailData = await page.evaluate(() => {
                    const result = {};

                    // 1) JSON-LD JobPosting
                    const scripts = Array.from(
                        document.querySelectorAll('script[type="application/ld+json"]'),
                    );
                    for (const script of scripts) {
                        try {
                            const json = script.textContent || '';
                            const data = JSON.parse(json);
                            const entries = Array.isArray(data) ? data : [data];

                            for (const item of entries) {
                                if (item['@type'] !== 'JobPosting') continue;

                                if (item.title && !result.title) {
                                    result.title = item.title;
                                }
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
                            // ignore malformed JSON
                        }
                    }

                    // 2) DOM fallback for description / salary / title
                    const cleanText = (node) =>
                        node.textContent?.replace(/\s+/g, ' ').trim() || '';

                    if (!result.description_html) {
                        const descEl =
                            document.querySelector('[data-at*="job-description"]') ||
                            document.querySelector('#job-description') ||
                            document.querySelector('.job-description') ||
                            document.querySelector('main article') ||
                            document.querySelector('main section');

                        if (descEl) {
                            const clone = descEl.cloneNode(true);
                            clone
                                .querySelectorAll('style,script,noscript')
                                .forEach((el) => el.remove());
                            result.description_html = clone.innerHTML;
                            result.description_text = cleanText(clone);
                        }
                    }

                    if (!result.salary) {
                        const salaryEl = Array.from(
                            document.querySelectorAll('span, p, li, div'),
                        ).find((el) => el.textContent.includes('£'));
                        if (salaryEl) {
                            let t = cleanText(salaryEl);
                            if (
                                t.length > 150 ||
                                t.includes('window.__PRELOADED_STATE__')
                            ) {
                                const m = t.match(/£[^,]+/);
                                t = (m && m[0]) || t.slice(0, 150);
                            }
                            result.salary = t;
                        }
                    }

                    if (!result.title) {
                        const h1 = document.querySelector('h1');
                        if (h1) result.title = cleanText(h1);
                    }

                    if (!result.company) {
                        const imgAlt = document.querySelector('img[alt]')?.getAttribute('alt');
                        if (imgAlt && imgAlt.length <= 80) {
                            result.company = imgAlt;
                        }
                    }

                    return result;
                });

                const merged = {
                    ...listData,
                    ...Object.fromEntries(
                        Object.entries(detailData).filter(([, v]) => v != null),
                    ),
                };

                // If we somehow carried a raw_date string from listing (not used in this version
                // but kept for compatibility), normalise it.
                if (!merged.date_posted && merged.raw_date) {
                    const parsed = parsePostedRelative(merged.raw_date);
                    if (parsed) merged.date_posted = parsed;
                    delete merged.raw_date;
                }

                await Dataset.pushData(merged);
                savedUrls.add(merged.url);
                savedCount++;
                stats.jobsSaved++;

                crawlerLog.info(`Saved detail job ${savedCount}/${results_wanted}`, {
                    title: merged.title,
                    url: merged.url,
                });
            }
        },

        async failedRequestHandler({ request, error, session, log: crawlerLog, page }) {
            const { label = 'LIST' } = request.userData ?? {};
            if (label === 'LIST') stats.listPagesFailed++;
            else stats.detailPagesFailed++;

            const retryCount = request.retryCount ?? 0;

            crawlerLog.warn('Request failed', {
                url: request.url,
                retryCount,
                label,
                message: error.message,
            });

            session?.retire();

            if (retryCount >= 1) {
                try {
                    if (page) {
                        const screenshot = await page.screenshot({ fullPage: true });
                        await Actor.setValue(
                            `FAILED_${Date.now()}.png`,
                            screenshot,
                            { contentType: 'image/png' },
                        );
                    }
                    await Actor.setValue(
                        `FAILED_${Date.now()}.txt`,
                        `URL: ${request.url}\nLabel: ${label}\nError: ${
                            error.message
                        }\nStack: ${error.stack ?? ''}`,
                    );
                } catch {
                    // ignore
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
