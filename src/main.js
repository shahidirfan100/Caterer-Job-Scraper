import { Actor, log } from 'apify';
import { Dataset, PlaywrightCrawler } from 'crawlee';
import fs from 'fs/promises';
import { chromium } from 'playwright';
import { execSync } from 'child_process';

const parsePostedDate = (text) => {
    if (!text) return null;
    const lower = text.toLowerCase().replace('posted', '').trim();

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

const dismissPopups = async (page) => {
    const selectors = [
        'button:has-text("Accept all")',
        'button:has-text("Accept All")',
        '[id*="accept"]',
        '[data-testid*="accept"]',
        '.js-accept-consent',
    ];

    for (const selector of selectors) {
        const btn = page.locator(selector);
        if (await btn.first().isVisible({ timeout: 1000 }).catch(() => false)) {
            await btn.first().click().catch(() => {});
            break;
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
            execSync('npx crawlee install-playwright-browsers --yes', { stdio: 'inherit', timeout: 180000 });
            log.info('Playwright browsers installed successfully');
        } catch (installErr) {
            log.error('Automatic Playwright installation failed', { error: installErr.message });
            throw installErr;
        }
    }

    let input = (await Actor.getInput()) ?? null;

    if (!input) {
        try {
            const rawLocalInput = await fs.readFile(new URL('../INPUT.json', import.meta.url), 'utf8');
            input = JSON.parse(rawLocalInput);
            log.info('Loaded input from local INPUT.json fallback');
        } catch (err) {
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

    const hasProxyCredentials = Boolean(proxyFromInput) || process.env.APIFY_PROXY_PASSWORD || process.env.APIFY_TOKEN;
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
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 75,
        navigationTimeoutSecs: 35,
        maxConcurrency: 2,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 30,
            sessionOptions: { maxUsageCount: 5, maxAgeSecs: 300 },
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
                ],
            },
        },
        preNavigationHooks: [
            async ({ page }) => {
                await page.setViewportSize({ width: 1366, height: 768 }).catch(() => {});
                await page.setExtraHTTPHeaders({
                    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8',
                    Connection: 'keep-alive',
                });
                await page.addInitScript(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => false });
                    Object.defineProperty(navigator, 'languages', { get: () => ['en-GB', 'en-US', 'en'] });
                    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                    window.chrome = { runtime: {} };
                });
            },
        ],
        async requestHandler({ request, page, log: crawlerLog, session, crawler: crawlerInstance }) {
            const { label = 'LIST', pageNum = 1, listData } = request.userData ?? {};

            await dismissPopups(page);
            const pageTitle = await page.title();
            const html = await page.content();
            const htmlLength = html.length;

            const lowerHtml = html.toLowerCase();
            const blocked = ['captcha', 'access denied', 'just a moment', 'verify you are a human', 'unusual traffic']
                .some((needle) => lowerHtml.includes(needle));

            if (blocked) {
                await Actor.setValue(`BLOCKED_${Date.now()}`, await page.screenshot({ fullPage: true }), { contentType: 'image/png' });
                session?.retire();
                throw new Error('Blocked or captcha detected');
            }

            if (label === 'LIST') {
                stats.listPagesProcessed++;

                const found = await page.waitForSelector('[data-at="job-item"], a[href*="/job/"]', { timeout: 15000 }).catch(() => null);
                if (!found) {
                    if (pageNum === 1) {
                        await Actor.setValue('DEBUG_HTML', html, { contentType: 'text/html' });
                        await Actor.setValue('DEBUG_SCREENSHOT', await page.screenshot({ fullPage: true }), { contentType: 'image/png' });
                    }
                    throw new Error('No job listings found on page');
                }

                const jobs = await page.evaluate((pageUrl) => {
                    const cards = Array.from(document.querySelectorAll('[data-at="job-item"]'));
                    const seen = new Set();

                    return cards.map((card) => {
                        const titleAnchor = card.querySelector('[data-at="job-item-title"] a, a[href*="/job/"]');
                        const href = titleAnchor?.getAttribute('href');
                        if (!href) return null;

                        const jobUrl = new URL(href, pageUrl).href;
                        if (seen.has(jobUrl)) return null;
                        seen.add(jobUrl);

                        const salaryText = card.querySelector('[data-at="job-item-salary-info"]')?.textContent?.trim() || null;
                        const rawDate = card.querySelector('[data-at="job-item-timeago"]')?.textContent?.trim() || null;

                        return {
                            title: titleAnchor?.textContent?.trim() || null,
                            company: card.querySelector('[data-at="job-item-company-name"]')?.textContent?.trim() || null,
                            location: card.querySelector('[data-at="job-item-location"]')?.textContent?.trim() || null,
                            salary: salaryText,
                            raw_date: rawDate,
                            url: jobUrl,
                            description_html: null,
                            description_text: null,
                        };
                    }).filter(Boolean);
                }, request.url);

                stats.jobsExtracted += jobs.length;
                crawlerLog.info(`Extracted ${jobs.length} jobs on page ${pageNum}`, { url: request.url, title: pageTitle, htmlLength });

                for (const job of jobs) {
                    if (savedCount >= results_wanted) break;
                    if (savedUrls.has(job.url)) continue;

                    const normalizedDate = parsePostedDate(job.raw_date);
                    delete job.raw_date;
                    if (normalizedDate) job.date_posted = normalizedDate;

                    if (collectDetails) {
                        await crawlerInstance.addRequests([{
                            url: job.url,
                            userData: { label: 'DETAIL', listData: job },
                        }]);
                    } else {
                        await Dataset.pushData(job);
                        savedUrls.add(job.url);
                        savedCount++;
                        stats.jobsSaved++;
                        crawlerLog.info(`Saved job ${savedCount}/${results_wanted}`, { title: job.title, url: job.url });
                    }
                }

                if (savedCount < results_wanted && pageNum < max_pages) {
                    const nextPage = pageNum + 1;
                    const hasNext = await page.evaluate((nextNum) => {
                        return Boolean(document.querySelector(`a[href*="page=${nextNum}"]`));
                    }, nextPage);

                    if (hasNext) {
                        await crawlerInstance.addRequests([{
                            url: buildSearchUrl(keyword, location, nextPage),
                            userData: { label: 'LIST', pageNum: nextPage },
                        }]);
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
                await page.waitForSelector('h1, [data-at="job-description"], article', { timeout: 12000 }).catch(() => {});

                const detailData = await page.evaluate(() => {
                    const result = {};
                    const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));

                    for (const script of scripts) {
                        try {
                            const data = JSON.parse(script.textContent || '{}');
                            const entries = Array.isArray(data) ? data : [data];
                            for (const item of entries) {
                                if (item['@type'] === 'JobPosting') {
                                    result.title = item.title ?? result.title;
                                    result.company = item.hiringOrganization?.name ?? result.company;
                                    result.location = item.jobLocation?.address?.addressLocality ?? result.location;
                                    result.salary = item.baseSalary?.value?.value ?? result.salary;
                                    result.job_type = item.employmentType ?? result.job_type;
                                    if (item.description && !result.description_html) {
                                        result.description_html = item.description;
                                        result.description_text = item.description.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
                                    }
                                    if (item.datePosted && !result.date_posted) {
                                        result.date_posted = item.datePosted;
                                    }
                                }
                            }
                        } catch (e) {
                            /* ignore malformed JSON-LD */
                        }
                    }

                    if (!result.title) {
                        result.title = document.querySelector('h1')?.textContent?.trim() || null;
                    }
                    if (!result.company) {
                        result.company = document.querySelector('[data-at*="company"], [class*="company"]')?.textContent?.trim() || null;
                    }
                    if (!result.location) {
                        result.location = document.querySelector('[data-at*="location"], [class*="location"]')?.textContent?.trim() || null;
                    }
                    if (!result.salary) {
                        result.salary = document.querySelector('[data-at*="salary"], [class*="salary"]')?.textContent?.trim() || null;
                    }
                    if (!result.description_html) {
                        const descEl = document.querySelector('[data-at="job-description"], .job-description, article');
                        if (descEl) {
                            result.description_html = descEl.innerHTML;
                            result.description_text = descEl.textContent?.replace(/\s+/g, ' ').trim() || null;
                        }
                    }

                    return result;
                });

                const job = {
                    ...listData,
                    ...Object.fromEntries(Object.entries(detailData).filter(([, value]) => value != null)),
                };

                if (!job.date_posted && job.raw_date) {
                    const parsed = parsePostedDate(job.raw_date);
                    if (parsed) job.date_posted = parsed;
                }

                await Dataset.pushData(job);
                savedUrls.add(job.url);
                savedCount++;
                stats.jobsSaved++;
                crawlerLog.info(`Saved detail job ${savedCount}/${results_wanted}`, { title: job.title, url: job.url });
            }
        },
        async failedRequestHandler({ request, error, session, log: crawlerLog }) {
            const retryCount = request.retryCount ?? 0;
            crawlerLog.error('Request failed', { url: request.url, error: error.message, retryCount });
            session?.retire();
        },
    });

    log.info('Crawler created, starting run', { startUrl: startUrlToUse });

    await crawler.run([{
        url: startUrlToUse,
        userData: { label: 'LIST', pageNum: 1 },
    }]);

    log.info('Scraping completed', {
        jobsSaved: savedCount,
        target: results_wanted,
        stats,
    });

    await Actor.setValue('STATS', stats);
});
