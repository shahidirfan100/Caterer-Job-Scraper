// src/main.js
import { Actor, log } from 'apify';
import { Dataset, PlaywrightCrawler } from 'crawlee';
import fs from 'fs/promises';
import { chromium } from 'playwright';
import { execSync } from 'child_process';

/**
 * Convert Caterer-style "X days ago" text into ISO timestamp.
 */
const parsePostedDate = (text) => {
    if (!text) return null;
    const lower = text.toLowerCase().replace('published', '').replace('posted', '').trim();

    if (lower.includes('today') || lower.includes('just now')) {
        return new Date().toISOString();
    }

    const match = lower.match(/(\d+)\s*(hour|day|week|month)s?\s*ago/);
    if (match) {
        const num = Number(match[1]);
        const unit = match[2];
        const ms = {
            hour: 3600000,
            day: 86400000,
            week: 604800000,
            month: 2592000000,
        }[unit] || 0;
        return new Date(Date.now() - num * ms).toISOString();
    }

    return null;
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
    // Quick runtime check so we fail loudly if Playwright is missing
    try {
        const tmpBrowser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
        await tmpBrowser.close();
        log.info('Playwright browsers check passed (Chromium launch succeeded)');
    } catch (err) {
        log.warning('Playwright browser check failed, attempting installation', { message: err.message });
        try {
            execSync('npx crawlee install-playwright-browsers --yes', {
                stdio: 'inherit',
                timeout: 180000,
            });
            log.info('Playwright browsers installed successfully');
        } catch (installErr) {
            log.error('Automatic Playwright installation failed', { error: installErr.message });
            throw installErr;
        }
    }

    // Load input either from Actor input or local INPUT.json (for local dev)
    let input = (await Actor.getInput()) ?? null;
    if (!input) {
        try {
            const rawLocalInput = await fs.readFile(new URL('../INPUT.json', import.meta.url), 'utf8');
            input = JSON.parse(rawLocalInput);
            log.info('Loaded input from local INPUT.json fallback');
        } catch {
            input = {};
        }
    }

    const {
        keyword = '',
        location = '',
        startUrl = '',
        results_wanted = 50,
        max_pages = 5,
        collectDetails = false,
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
    const stats = {
        listPagesProcessed: 0,
        detailPagesProcessed: 0,
        jobsExtracted: 0,
        jobsSaved: 0,
    };

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: max_pages * 40,
        maxRequestRetries: 4,
        requestHandlerTimeoutSecs: 90,
        navigationTimeoutSecs: 45,
        maxConcurrency: 2,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 30,
            sessionOptions: { maxUsageCount: 2, maxAgeSecs: 300 },
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

        /**
         * Main handler: handles both list and detail pages.
         */
        async requestHandler({ request, page, log: crawlerLog, session, crawler: crawlerInstance }) {
            const { label = 'LIST', pageNum = 1, listData } = request.userData ?? {};

            await dismissPopups(page);

            const pageTitle = await page.title();
            const html = await page.content();
            const htmlLength = html.length;

            const mainResponse = page.mainFrame()?.response?.();
            let status;
            try {
                status = await mainResponse?.status?.();
            } catch {
                status = undefined;
            }

            // Basic bot / block detection
            const lowerHtml = html.toLowerCase();
            const blocked = ['captcha', 'access denied', 'verify you are a human', 'unusual traffic'].some(
                (needle) => lowerHtml.includes(needle),
            );

            if (blocked) {
                try {
                    await Actor.setValue(
                        `BLOCKED_${Date.now()}`,
                        await page.screenshot({ fullPage: true }),
                        { contentType: 'image/png' },
                    );
                } catch {
                    // ignore
                }
                session?.retire();
                throw new Error('Blocked or captcha detected');
            }

            if (label === 'LIST') {
                stats.listPagesProcessed++;

                if (status && status >= 400) {
                    throw new Error(`Received bad status ${status}`);
                }

                // Wait for at least one job link to appear
                const found = await page
                    .waitForSelector('a[href*="/job/"]', { timeout: 15000 })
                    .catch(() => null);

                if (!found) {
                    if (pageNum === 1) {
                        await Actor.setValue('DEBUG_HTML', html, { contentType: 'text/html' });
                        await Actor.setValue(
                            'DEBUG_SCREENSHOT',
                            await page.screenshot({ fullPage: true }),
                            { contentType: 'image/png' },
                        );
                    }
                    throw new Error('No job listings found on page');
                }

                // Extract jobs from list page using robust heuristics
                const jobs = await page.evaluate((pageUrl) => {
                    const results = [];
                    const seen = new Set();

                    const anchors = Array.from(
                        document.querySelectorAll('main a[href*="/job/"], a[href*="/job/"]'),
                    );

                    for (const anchor of anchors) {
                        const href = anchor.getAttribute('href');
                        if (!href) continue;

                        const jobUrl = new URL(href, pageUrl).href;
                        if (seen.has(jobUrl)) continue;

                        const titleText = anchor.textContent?.trim() || '';
                        // Ignore nav / footer links by requiring non-trivial title
                        if (!titleText || titleText.length < 3) continue;

                        // Try to find a container around the job (heading + siblings)
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

                        const salary =
                            siblingTexts.find((t) => t.includes('£')) ||
                            siblingTexts.find((t) => /per (hour|annum|year)/i.test(t)) ||
                            null;

                        const rawDate =
                            siblingTexts.find((t) => /ago$/i.test(t) || /last \d+ days?/i.test(t)) ||
                            null;

                        const location =
                            siblingTexts.find((t) =>
                                /\b[A-Z]{1,2}\d{1,2}\b/.test(t) || /[A-Za-z]+,\s*[A-Za-z]/.test(t),
                            ) || null;

                        // Company often appears as a single short line between title and location
                        let company = null;
                        if (siblingTexts.length) {
                            const candidate = siblingTexts[0];
                            if (
                                candidate &&
                                !candidate.includes('£') &&
                                !candidate.toLowerCase().includes('ago') &&
                                candidate.length <= 60
                            ) {
                                company = candidate;
                            }
                        }

                        results.push({
                            title: titleText,
                            company,
                            location,
                            salary,
                            raw_date: rawDate,
                            url: jobUrl,
                            description_html: null,
                            description_text: null,
                        });

                        seen.add(jobUrl);
                    }

                    return results;
                }, request.url);

                stats.jobsExtracted += jobs.length;
                crawlerLog.info(`Extracted ${jobs.length} jobs on page ${pageNum}`, {
                    url: request.url,
                    title: pageTitle,
                    htmlLength,
                });

                for (const job of jobs) {
                    if (savedCount >= results_wanted) break;
                    if (savedUrls.has(job.url)) continue;

                    const normalizedDate = parsePostedDate(job.raw_date);
                    delete job.raw_date;
                    if (normalizedDate) job.date_posted = normalizedDate;

                    if (collectDetails) {
                        await crawlerInstance.addRequests([
                            {
                                url: job.url,
                                userData: { label: 'DETAIL', listData: job },
                            },
                        ]);
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

                // Small jitter to be less botty
                await page.waitForTimeout(500 + Math.random() * 1000);

                // Pagination
                if (savedCount < results_wanted && pageNum < max_pages) {
                    const nextPage = pageNum + 1;
                    const hasNext = await page.evaluate((nextNum) => {
                        // Look for any link mentioning the next page number in href or text
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
            } else if (label === 'DETAIL') {
                if (savedCount >= results_wanted || !listData) return;
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
                    const scripts = Array.from(
                        document.querySelectorAll('script[type="application/ld+json"]'),
                    );

                    for (const script of scripts) {
                        try {
                            const data = JSON.parse(script.textContent || '{}');
                            const entries = Array.isArray(data) ? data : [data];
                            for (const item of entries) {
                                if (item['@type'] === 'JobPosting') {
                                    result.title = item.title ?? result.title;
                                    result.company =
                                        item.hiringOrganization?.name ?? result.company;
                                    result.location =
                                        item.jobLocation?.address?.addressLocality ??
                                        result.location;
                                    result.salary =
                                        item.baseSalary?.value?.value ??
                                        item.baseSalary?.value?.minValue ??
                                        result.salary;
                                    result.job_type = item.employmentType ?? result.job_type;
                                    if (item.description && !result.description_html) {
                                        result.description_html = item.description;
                                        result.description_text = item.description
                                            .replace(/<[^>]*>/g, ' ')
                                            .replace(/\s+/g, ' ')
                                            .trim();
                                    }
                                    if (item.datePosted && !result.date_posted) {
                                        result.date_posted = item.datePosted;
                                    }
                                }
                            }
                        } catch {
                            // ignore malformed JSON-LD
                        }
                    }

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
                        result.salary = salaryEl?.textContent?.trim() || null;
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

                if (!job.date_posted && job.raw_date) {
                    const parsed = parsePostedDate(job.raw_date);
                    if (parsed) job.date_posted = parsed;
                }

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

        /**
         * Handle hard failures with diagnostics & gentle session retirement.
         */
        async failedRequestHandler({ request, error, session, log: crawlerLog, page }) {
            const retryCount = request.retryCount ?? 0;
            let shot;
            try {
                if (page) {
                    shot = await page.screenshot({ fullPage: true });
                }
            } catch {
                // ignore screenshot failure
            }

            crawlerLog.error('Request failed', {
                url: request.url,
                error: error.message,
                retryCount,
            });

            // Retire the session so we get a fresh identity / proxy
            session?.retire();

            // Persist diagnostics after a couple of retries
            if (retryCount >= 2) {
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
