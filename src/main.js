// src/main.js
// Caterer.com Job Scraper - Full Playwright Implementation
// Uses real browser for all page fetches to bypass anti-bot measures

import { Actor, log } from 'apify';
import { Dataset, PlaywrightCrawler } from 'crawlee';
import * as cheerio from 'cheerio';

const BASE_URL = 'https://www.caterer.com';

/**
 * Build search URL from keyword/location
 */
const buildSearchUrl = (keyword, location, page = 1) => {
    const url = new URL('/jobs/search', BASE_URL);
    if (keyword) url.searchParams.set('keywords', keyword);
    if (location) url.searchParams.set('location', location);
    if (page > 1) url.searchParams.set('page', String(page));
    return url.href;
};

/**
 * Build pagination URL
 */
const buildPaginatedUrl = (baseUrl, page) => {
    const url = new URL(baseUrl);
    url.searchParams.set('page', String(page));
    return url.href;
};

/**
 * Sleep helper
 */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Extract salary from text
 */
const extractSalary = (text) => {
    if (!text) return null;
    const compact = text.replace(/\s+/g, ' ').trim();
    const perMatch = compact.match(/(£[^£]+?per (?:hour|annum|year|week|day))/i);
    if (perMatch) return perMatch[1].trim();
    const rangeMatch = compact.match(/(£[\d,.]+ ?[-–] ?£[\d,.]+)/);
    if (rangeMatch) return rangeMatch[1].trim();
    return null;
};

/**
 * Extract jobs from listing page HTML
 */
const extractJobsFromDom = (html, pageUrl) => {
    const $ = cheerio.load(html);
    $('style,script,noscript').remove();

    const urlObj = new URL(pageUrl);
    const seen = new Set();
    const jobs = [];

    $('a[href*="/job/"]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href) return;
        const jobUrl = new URL(href, urlObj.origin).href;
        if (seen.has(jobUrl)) return;

        const $a = $(el).clone();
        $a.find('style,script,noscript').remove();
        const title = $a.text().replace(/\s+/g, ' ').trim();
        if (!title || title.length < 2) return;

        const container =
            $(el).closest('article').length ? $(el).closest('article') :
                $(el).closest('li').length ? $(el).closest('li') :
                    $(el).closest('div').length ? $(el).closest('div') :
                        $(el).parent();

        const $container = container.clone();
        $container.find('style,script,noscript').remove();
        const fullText = $container.text().replace(/\s+/g, ' ').trim();

        let company = $container.find('img[alt]').attr('alt') || null;
        if (!company || company.length < 2 || company.length > 80) company = null;

        const salary = extractSalary(fullText);

        let location = null;
        if (company && salary) {
            const idxCompany = fullText.indexOf(company);
            const idxSalary = fullText.indexOf(salary);
            if (idxCompany !== -1 && idxSalary !== -1 && idxSalary > idxCompany) {
                location = fullText.slice(idxCompany + company.length, idxSalary).replace(/\s+/g, ' ').trim();
            }
        }

        jobs.push({
            source: 'caterer.com',
            title,
            company,
            location,
            salary,
            url: jobUrl,
        });

        seen.add(jobUrl);
    });

    return jobs;
};

/**
 * Extract job details from detail page
 */
const extractJobDetail = (html) => {
    const result = {};
    const $ = cheerio.load(html);

    // Try JSON-LD first
    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const data = JSON.parse($(el).text() || '{}');
            const entries = Array.isArray(data) ? data : [data];
            for (const item of entries) {
                if (item['@type'] !== 'JobPosting') continue;
                if (item.description && !result.description_html) {
                    result.description_html = item.description;
                    result.description_text = item.description.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
                }
                if (item.hiringOrganization?.name && !result.company) {
                    result.company = item.hiringOrganization.name;
                }
                if (item.baseSalary && !result.salary) {
                    const val = item.baseSalary.value;
                    if (typeof val === 'string') result.salary = val;
                    else if (val?.value || val?.minValue) {
                        result.salary = `${val.value ?? val.minValue} ${val.currency || ''}`.trim();
                    }
                }
                if (item.datePosted && !result.date_posted) result.date_posted = item.datePosted;
                if (item.employmentType && !result.job_type) result.job_type = item.employmentType;
            }
        } catch { }
    });

    // DOM fallback
    $('style,script,noscript').remove();
    if (!result.description_html) {
        const descEl = $('[data-at="job-description"]').first().length ? $('[data-at="job-description"]').first() :
            $('#job-description').first().length ? $('#job-description').first() :
                $('.job-description').first().length ? $('.job-description').first() :
                    $('main article').first();
        if (descEl?.length) {
            result.description_html = descEl.html();
            result.description_text = descEl.text().replace(/\s+/g, ' ').trim();
        }
    }

    if (!result.salary) result.salary = extractSalary($('body').text());
    if (!result.company) result.company = $('img[alt]').first().attr('alt') || null;
    if (!result.title) result.title = $('h1').first().text().replace(/\s+/g, ' ').trim() || null;

    return result;
};

// ============ MAIN ============
await Actor.main(async () => {
    const input = (await Actor.getInput()) ?? {};
    const {
        keyword = '',
        location = '',
        startUrl = '',
        results_wanted = 50,
        max_pages = 10,
        max_detail_concurrency = 2,
        proxyConfiguration: proxyInput,
    } = input;

    const startUrlToUse = startUrl?.trim() || buildSearchUrl(keyword, location, 1);

    log.info('🚀 Starting Caterer.com Playwright Scraper', {
        keyword, location, startUrl: startUrlToUse, results_wanted, max_pages,
    });

    // Setup proxy
    let proxyConfig = null;
    if (proxyInput || process.env.APIFY_PROXY_PASSWORD) {
        try {
            proxyConfig = await Actor.createProxyConfiguration(
                proxyInput ?? { groups: ['RESIDENTIAL'], countryCode: 'GB' }
            );
            log.info('✅ Proxy configured');
        } catch (err) {
            log.warning('⚠️ Proxy setup failed', { error: err.message });
        }
    }

    // State for collecting jobs
    const savedUrls = new Set();
    let savedCount = 0;
    const allJobs = [];
    const stats = { pagesFetched: 0, jobsExtracted: 0, detailsFetched: 0, jobsSaved: 0 };

    // ============ LISTING CRAWLER ============
    const listingCrawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConfig,
        maxConcurrency: 1, // Sequential for stealth
        navigationTimeoutSecs: 60,
        requestHandlerTimeoutSecs: 120,
        maxRequestRetries: 3,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 5,
            sessionOptions: {
                maxUsageCount: 3,
            },
        },
        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--lang=en-GB',
                ],
            },
        },
        preNavigationHooks: [
            async ({ page }) => {
                // Anti-detection
                await page.addInitScript(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => false });
                    Object.defineProperty(navigator, 'languages', { get: () => ['en-GB', 'en-US', 'en'] });
                });

                // Block heavy resources
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    if (['image', 'media', 'font'].includes(type)) return route.abort();
                    return route.continue();
                });
            },
        ],
        requestHandler: async ({ page, request }) => {
            log.info(`📄 Processing: ${request.url.slice(0, 80)}`);

            // Wait for content
            await page.waitForLoadState('domcontentloaded');
            await sleep(1000 + Math.random() * 1000);

            // Dismiss cookie popups
            for (const sel of ['button:has-text("Accept all")', '[id*="accept"]']) {
                try {
                    const btn = page.locator(sel).first();
                    if (await btn.isVisible({ timeout: 1000 })) {
                        await btn.click().catch(() => { });
                        await sleep(500);
                        break;
                    }
                } catch { }
            }

            const html = await page.content();
            const jobs = extractJobsFromDom(html, request.url);

            stats.pagesFetched++;
            stats.jobsExtracted += jobs.length;
            log.info(`✅ Extracted ${jobs.length} jobs`);

            for (const job of jobs) {
                if (allJobs.length >= results_wanted) break;
                if (savedUrls.has(job.url)) continue;
                savedUrls.add(job.url);
                allJobs.push(job);
            }
        },
        failedRequestHandler: async ({ request }, error) => {
            log.warning(`❌ Failed: ${request.url.slice(0, 60)}`, { error: error.message });
        },
    });

    // Build listing URLs
    const listingUrls = [];
    for (let page = 1; page <= Math.min(max_pages, 20); page++) {
        const url = page === 1 ? startUrlToUse : buildPaginatedUrl(startUrlToUse, page);
        listingUrls.push(url);
    }

    // Run listing crawler
    await listingCrawler.run(listingUrls);

    log.info(`📋 Collected ${allJobs.length} jobs, now fetching details...`);

    // ============ DETAIL CRAWLER ============
    if (allJobs.length > 0) {
        const detailCrawler = new PlaywrightCrawler({
            proxyConfiguration: proxyConfig,
            maxConcurrency: max_detail_concurrency,
            navigationTimeoutSecs: 45,
            requestHandlerTimeoutSecs: 60,
            maxRequestRetries: 2,
            useSessionPool: true,
            launchContext: {
                launchOptions: {
                    headless: true,
                    args: [
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                    ],
                },
            },
            preNavigationHooks: [
                async ({ page }) => {
                    await page.addInitScript(() => {
                        Object.defineProperty(navigator, 'webdriver', { get: () => false });
                    });
                    await page.route('**/*', (route) => {
                        const type = route.request().resourceType();
                        if (['image', 'media', 'font', 'stylesheet'].includes(type)) return route.abort();
                        return route.continue();
                    });
                },
            ],
            requestHandler: async ({ page, request }) => {
                await page.waitForLoadState('domcontentloaded');
                await sleep(500 + Math.random() * 500);

                const html = await page.content();
                const detail = extractJobDetail(html);
                const jobIndex = request.userData.jobIndex;

                stats.detailsFetched++;

                // Update job with details
                if (allJobs[jobIndex]) {
                    allJobs[jobIndex] = {
                        ...allJobs[jobIndex],
                        title: detail.title || allJobs[jobIndex].title,
                        company: detail.company || allJobs[jobIndex].company,
                        salary: detail.salary || allJobs[jobIndex].salary,
                        date_posted: detail.date_posted || null,
                        job_type: detail.job_type || null,
                        description_html: detail.description_html || null,
                        description_text: detail.description_text || null,
                    };
                }
            },
            failedRequestHandler: async ({ request }) => {
                log.debug(`Detail failed: ${request.url.slice(0, 50)}`);
            },
        });

        // Run detail crawler
        const detailRequests = allJobs.slice(0, results_wanted).map((job, i) => ({
            url: job.url,
            userData: { jobIndex: i },
        }));
        await detailCrawler.run(detailRequests);
    }

    // ============ SAVE RESULTS ============
    for (const job of allJobs.slice(0, results_wanted)) {
        await Dataset.pushData({
            ...job,
            keyword_search: keyword || null,
            location_search: location || null,
            extracted_at: new Date().toISOString(),
        });
        savedCount++;
        stats.jobsSaved++;
    }

    log.info('🎉 Scraping completed', stats);
    await Actor.setValue('STATS', stats);
});
