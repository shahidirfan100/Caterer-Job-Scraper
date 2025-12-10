// src/main.js
// Caterer.com Hybrid Scraper - Camoufox + got-scraping
// Uses Playwright with Camoufox for anti-blocking handshake and pagination
// Uses got-scraping for fast detail page fetching, with Playwright fallback

import { Actor, log } from 'apify';
import { Dataset, PlaywrightCrawler } from 'crawlee';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';
import { gotScraping } from 'got-scraping';
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
 * Extract job details from detail page HTML
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

/**
 * Format cookies array to string for HTTP header
 */
const formatCookies = (cookies) => {
    return cookies.map(c => `${c.name}=${c.value}`).join('; ');
};

/**
 * Fetch detail page with got-scraping (fast path)
 */
const fetchDetailWithGot = async (url, cookies, proxyUrl, userAgent) => {
    const headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': BASE_URL,
        'Cookie': cookies,
        'User-Agent': userAgent,
    };

    const options = {
        url,
        headers,
        timeout: { request: 30000 },
        retry: { limit: 0 },
        throwHttpErrors: false,
    };

    if (proxyUrl) {
        options.proxyUrl = proxyUrl;
    }

    const response = await gotScraping(options);
    return { statusCode: response.statusCode, body: response.body };
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
        max_detail_concurrency = 5,
        proxyConfiguration: proxyInput,
    } = input;

    const startUrlToUse = startUrl?.trim() || buildSearchUrl(keyword, location, 1);

    log.info('🚀 Starting Caterer.com Hybrid Scraper (Camoufox + got-scraping)', {
        keyword, location, startUrl: startUrlToUse, results_wanted, max_pages,
    });

    // Setup proxy
    let proxyConfig = null;
    let proxyUrl = null;
    if (proxyInput || process.env.APIFY_PROXY_PASSWORD) {
        try {
            proxyConfig = await Actor.createProxyConfiguration(
                proxyInput ?? { groups: ['RESIDENTIAL'], countryCode: 'GB' }
            );
            proxyUrl = await proxyConfig.newUrl();
            log.info('✅ Proxy configured');
        } catch (err) {
            log.warning('⚠️ Proxy setup failed', { error: err.message });
        }
    }

    // State for collecting jobs
    const savedUrls = new Set();
    let savedCount = 0;
    const allJobs = [];
    let sessionCookies = '';
    let sessionUserAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0';
    const stats = {
        pagesFetched: 0,
        jobsExtracted: 0,
        gotSuccess: 0,
        gotFailed: 0,
        playwrightFallback: 0,
        jobsSaved: 0
    };

    // URLs that failed with got-scraping and need Playwright fallback
    const failedDetailUrls = [];

    // ============ LISTING CRAWLER (Camoufox) ============
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
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: true,
                proxy: proxyUrl ? { server: proxyUrl } : undefined,
                geoip: true,
            }),
        },
        preNavigationHooks: [
            async ({ page }) => {
                // Block heavy resources
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    if (['image', 'media', 'font'].includes(type)) return route.abort();
                    return route.continue();
                });
            },
        ],
        requestHandler: async ({ page, request }) => {
            log.info(`📄 [Camoufox] Processing listing: ${request.url.slice(0, 80)}`);

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

            // Extract cookies for got-scraping
            const cookies = await page.context().cookies();
            if (cookies.length > 0) {
                sessionCookies = formatCookies(cookies);
            }

            // Get user agent from page
            const ua = await page.evaluate(() => navigator.userAgent);
            if (ua) sessionUserAgent = ua;

            const html = await page.content();
            const jobs = extractJobsFromDom(html, request.url);

            stats.pagesFetched++;
            stats.jobsExtracted += jobs.length;
            log.info(`✅ Extracted ${jobs.length} jobs from listing page`);

            for (const job of jobs) {
                if (allJobs.length >= results_wanted) break;
                if (savedUrls.has(job.url)) continue;
                savedUrls.add(job.url);
                allJobs.push(job);
            }
        },
        failedRequestHandler: async ({ request }, error) => {
            log.warning(`❌ Listing failed: ${request.url.slice(0, 60)}`, { error: error.message });
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

    log.info(`📋 Collected ${allJobs.length} jobs, now fetching details with got-scraping...`);

    // ============ FAST DETAIL FETCHING (got-scraping) ============
    if (allJobs.length > 0) {
        const jobsToFetch = allJobs.slice(0, results_wanted);

        // Process details in batches for controlled concurrency
        const batchSize = max_detail_concurrency;
        for (let i = 0; i < jobsToFetch.length; i += batchSize) {
            const batch = jobsToFetch.slice(i, i + batchSize);

            await Promise.all(batch.map(async (job, batchIndex) => {
                const jobIndex = i + batchIndex;

                try {
                    const { statusCode, body } = await fetchDetailWithGot(
                        job.url,
                        sessionCookies,
                        proxyUrl,
                        sessionUserAgent
                    );

                    if (statusCode === 200 && body) {
                        const detail = extractJobDetail(body);

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

                        stats.gotSuccess++;
                        log.debug(`⚡ [got] Fetched: ${job.url.slice(0, 60)}`);
                    } else {
                        // Blocked or error - add to fallback queue
                        log.debug(`🚫 [got] Blocked (${statusCode}): ${job.url.slice(0, 50)}`);
                        failedDetailUrls.push({ url: job.url, jobIndex });
                        stats.gotFailed++;
                    }
                } catch (err) {
                    log.debug(`❌ [got] Error: ${job.url.slice(0, 50)} - ${err.message}`);
                    failedDetailUrls.push({ url: job.url, jobIndex });
                    stats.gotFailed++;
                }
            }));

            // Small delay between batches
            if (i + batchSize < jobsToFetch.length) {
                await sleep(200 + Math.random() * 300);
            }
        }

        log.info(`📊 got-scraping results: ${stats.gotSuccess} success, ${stats.gotFailed} need fallback`);

        // ============ PLAYWRIGHT FALLBACK FOR FAILED DETAILS ============
        if (failedDetailUrls.length > 0) {
            log.info(`🔄 Retrying ${failedDetailUrls.length} failed details with Playwright...`);

            const fallbackCrawler = new PlaywrightCrawler({
                proxyConfiguration: proxyConfig,
                maxConcurrency: 2,
                navigationTimeoutSecs: 45,
                requestHandlerTimeoutSecs: 60,
                maxRequestRetries: 2,
                useSessionPool: true,
                launchContext: {
                    launcher: firefox,
                    launchOptions: await camoufoxLaunchOptions({
                        headless: true,
                        proxy: proxyUrl ? { server: proxyUrl } : undefined,
                        geoip: true,
                    }),
                },
                preNavigationHooks: [
                    async ({ page }) => {
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

                    stats.playwrightFallback++;

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

                    log.debug(`✅ [Playwright] Fallback success: ${request.url.slice(0, 50)}`);
                },
                failedRequestHandler: async ({ request }) => {
                    log.debug(`❌ [Playwright] Fallback failed: ${request.url.slice(0, 50)}`);
                },
            });

            const fallbackRequests = failedDetailUrls.map(item => ({
                url: item.url,
                userData: { jobIndex: item.jobIndex },
            }));

            await fallbackCrawler.run(fallbackRequests);
        }
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

    log.info('🎉 Hybrid scraping completed', {
        ...stats,
        summary: `${stats.gotSuccess} fast + ${stats.playwrightFallback} fallback = ${stats.jobsSaved} saved`
    });
    await Actor.setValue('STATS', stats);
});
