// src/main.js
// Caterer.com Hybrid Scraper - Camoufox + got-scraping
// Uses Playwright with Camoufox for anti-blocking handshake
// Uses got-scraping for fast detail page fetching, with Playwright fallback

import { Actor, log } from 'apify';
import { Dataset } from 'crawlee';
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
 * Fetch page with got-scraping (fast path)
 */
const fetchWithGot = async (url, cookies, proxyUrl, userAgent) => {
    const headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': BASE_URL,
        'User-Agent': userAgent,
    };

    if (cookies) {
        headers['Cookie'] = cookies;
    }

    const options = {
        url,
        headers,
        timeout: { request: 20000 },
        retry: { limit: 0 },
        throwHttpErrors: false,
    };

    if (proxyUrl) {
        options.proxyUrl = proxyUrl;
    }

    const response = await gotScraping(options);
    return { statusCode: response.statusCode, body: response.body };
};

/**
 * Wait for job listings to appear on page
 */
const waitForJobListings = async (page, timeout = 15000) => {
    try {
        // Wait for any of these selectors that indicate jobs are loaded
        await page.waitForSelector('a[href*="/job/"]', { timeout });
        return true;
    } catch {
        return false;
    }
};

/**
 * Dismiss cookie consent banners
 */
const dismissCookieBanner = async (page) => {
    const selectors = [
        'button:has-text("Accept all")',
        'button:has-text("Accept All")',
        'button:has-text("Accept")',
        '[id*="accept"]',
        '[class*="cookie"] button',
        '[class*="consent"] button',
    ];

    for (const sel of selectors) {
        try {
            const btn = page.locator(sel).first();
            if (await btn.isVisible({ timeout: 2000 })) {
                await btn.click().catch(() => { });
                await sleep(500);
                return true;
            }
        } catch { }
    }
    return false;
};

/**
 * Extract jobs from Playwright page (with proper waiting)
 */
const extractJobsFromPage = async (page, pageUrl) => {
    // Wait for content to stabilize
    await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => { });

    // Wait for job links to appear
    const hasJobs = await waitForJobListings(page, 10000);
    if (!hasJobs) {
        log.warning('No job listings found on page after waiting');
    }

    // Small delay for any final rendering
    await sleep(500);

    const html = await page.content();
    return extractJobsFromDom(html, pageUrl);
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
        proxyConfiguration: proxyInput,
    } = input;

    const startUrlToUse = startUrl?.trim() || buildSearchUrl(keyword, location, 1);

    log.info('🚀 Starting Caterer.com Hybrid Scraper', {
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

    // State
    const savedUrls = new Set();
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

    // ============ PHASE 1: Camoufox handshake - get session cookies ============
    log.info('🔐 Phase 1: Establishing session with Camoufox...');

    let browser = null;
    let context = null;

    try {
        const launchOpts = await camoufoxLaunchOptions({
            headless: true,
            geoip: true,
        });

        // Add proxy to launch options if available
        if (proxyUrl) {
            try {
                const proxyUrlObj = new URL(proxyUrl);
                launchOpts.proxy = {
                    server: `${proxyUrlObj.protocol}//${proxyUrlObj.host}`,
                    username: proxyUrlObj.username || undefined,
                    password: proxyUrlObj.password || undefined,
                };
            } catch (e) {
                log.warning('Failed to parse proxy URL for Playwright', { error: e.message });
            }
        }

        browser = await firefox.launch(launchOpts);
        context = await browser.newContext();
        const page = await context.newPage();

        // Block heavy resources but keep stylesheets for proper rendering
        await page.route('**/*', (route) => {
            const type = route.request().resourceType();
            if (['image', 'media', 'font'].includes(type)) return route.abort();
            return route.continue();
        });

        // Navigate to first page to establish session
        log.info(`📄 Opening first listing page: ${startUrlToUse.slice(0, 60)}...`);
        await page.goto(startUrlToUse, { waitUntil: 'load', timeout: 60000 });

        // Handle cookie consent FIRST
        await dismissCookieBanner(page);

        // Wait for page to stabilize after dismissing cookie banner
        await sleep(2000);

        // Extract cookies and user agent
        const cookies = await context.cookies();
        sessionCookies = formatCookies(cookies);
        sessionUserAgent = await page.evaluate(() => navigator.userAgent);

        // Extract jobs from first page with proper waiting
        const jobs = await extractJobsFromPage(page, startUrlToUse);
        stats.pagesFetched++;
        stats.jobsExtracted += jobs.length;

        for (const job of jobs) {
            if (allJobs.length >= results_wanted) break;
            if (savedUrls.has(job.url)) continue;
            savedUrls.add(job.url);
            allJobs.push(job);
        }

        log.info(`✅ Page 1: Extracted ${jobs.length} jobs, total: ${allJobs.length}`);

        // If first page got 0 jobs, try scrolling and waiting more
        if (jobs.length === 0) {
            log.warning('⚠️ No jobs on page 1, retrying with scroll...');
            await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
            await sleep(2000);

            const retryJobs = await extractJobsFromPage(page, startUrlToUse);
            for (const job of retryJobs) {
                if (allJobs.length >= results_wanted) break;
                if (savedUrls.has(job.url)) continue;
                savedUrls.add(job.url);
                allJobs.push(job);
            }

            if (retryJobs.length > 0) {
                log.info(`✅ Page 1 (retry): Extracted ${retryJobs.length} jobs, total: ${allJobs.length}`);
                stats.jobsExtracted += retryJobs.length;
            }
        }

        await page.close();

    } catch (err) {
        log.error('❌ Camoufox handshake failed', { error: err.message });
    }

    // ============ PHASE 2: Fast listing pagination with got-scraping ============
    if (allJobs.length < results_wanted) {
        log.info('⚡ Phase 2: Fast pagination with got-scraping...');

        for (let pageNum = 2; pageNum <= max_pages && allJobs.length < results_wanted; pageNum++) {
            const pageUrl = buildSearchUrl(keyword, location, pageNum);

            try {
                const { statusCode, body } = await fetchWithGot(pageUrl, sessionCookies, proxyUrl, sessionUserAgent);

                if (statusCode === 200 && body) {
                    const jobs = extractJobsFromDom(body, pageUrl);
                    stats.pagesFetched++;
                    stats.jobsExtracted += jobs.length;

                    let addedCount = 0;
                    for (const job of jobs) {
                        if (allJobs.length >= results_wanted) break;
                        if (savedUrls.has(job.url)) continue;
                        savedUrls.add(job.url);
                        allJobs.push(job);
                        addedCount++;
                    }

                    log.info(`✅ Page ${pageNum}: ${jobs.length} jobs found, added ${addedCount}, total: ${allJobs.length}`);
                    stats.gotSuccess++;

                    // Small delay to be polite
                    await sleep(300 + Math.random() * 400);
                } else if (statusCode === 403 || statusCode === 0) {
                    log.warning(`🚫 Page ${pageNum}: Blocked (${statusCode}), using Playwright fallback...`);
                    stats.gotFailed++;

                    // Fallback to Playwright for this page
                    if (context) {
                        try {
                            const page = await context.newPage();
                            await page.route('**/*', (route) => {
                                const type = route.request().resourceType();
                                if (['image', 'media', 'font'].includes(type)) return route.abort();
                                return route.continue();
                            });

                            await page.goto(pageUrl, { waitUntil: 'load', timeout: 45000 });
                            await dismissCookieBanner(page);

                            const jobs = await extractJobsFromPage(page, pageUrl);
                            stats.pagesFetched++;
                            stats.jobsExtracted += jobs.length;
                            stats.playwrightFallback++;

                            let addedCount = 0;
                            for (const job of jobs) {
                                if (allJobs.length >= results_wanted) break;
                                if (savedUrls.has(job.url)) continue;
                                savedUrls.add(job.url);
                                allJobs.push(job);
                                addedCount++;
                            }

                            log.info(`✅ Page ${pageNum} (Playwright): ${jobs.length} jobs, added ${addedCount}, total: ${allJobs.length}`);

                            // Refresh cookies
                            const newCookies = await context.cookies();
                            sessionCookies = formatCookies(newCookies);

                            await page.close();
                        } catch (fallbackErr) {
                            log.warning(`❌ Playwright fallback failed for page ${pageNum}`, { error: fallbackErr.message });
                        }
                    }
                } else {
                    log.warning(`⚠️ Page ${pageNum}: Unexpected status ${statusCode}`);
                    stats.gotFailed++;
                }
            } catch (err) {
                log.warning(`❌ Page ${pageNum} failed`, { error: err.message });
                stats.gotFailed++;
            }
        }
    }

    // Close browser if still open
    if (browser) {
        await browser.close().catch(() => { });
    }

    log.info(`📋 Collected ${allJobs.length} jobs, fetching details...`);

    // ============ PHASE 3: Fast detail fetching with got-scraping ============
    const jobsToProcess = allJobs.slice(0, results_wanted);
    const failedDetailUrls = [];

    if (jobsToProcess.length > 0) {
        log.info('⚡ Phase 3: Fast detail fetching with got-scraping...');

        const concurrency = 8; // High concurrency for speed

        for (let i = 0; i < jobsToProcess.length; i += concurrency) {
            const batch = jobsToProcess.slice(i, i + concurrency);

            await Promise.all(batch.map(async (job, batchIdx) => {
                const jobIndex = i + batchIdx;

                try {
                    const { statusCode, body } = await fetchWithGot(
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
                    } else {
                        failedDetailUrls.push({ url: job.url, jobIndex });
                        stats.gotFailed++;
                    }
                } catch (err) {
                    failedDetailUrls.push({ url: job.url, jobIndex });
                    stats.gotFailed++;
                }
            }));

            // Small delay between batches
            if (i + concurrency < jobsToProcess.length) {
                await sleep(100 + Math.random() * 200);
            }
        }

        log.info(`📊 Detail fetch: ${stats.gotSuccess} fast, ${failedDetailUrls.length} need fallback`);
    }

    // ============ PHASE 4: Playwright fallback for failed details ============
    if (failedDetailUrls.length > 0) {
        log.info(`🔄 Phase 4: Playwright fallback for ${failedDetailUrls.length} blocked details...`);

        try {
            const launchOpts = await camoufoxLaunchOptions({ headless: true, geoip: true });
            if (proxyUrl) {
                try {
                    const proxyUrlObj = new URL(proxyUrl);
                    launchOpts.proxy = {
                        server: `${proxyUrlObj.protocol}//${proxyUrlObj.host}`,
                        username: proxyUrlObj.username || undefined,
                        password: proxyUrlObj.password || undefined,
                    };
                } catch (e) { }
            }

            const fallbackBrowser = await firefox.launch(launchOpts);
            const fallbackContext = await fallbackBrowser.newContext();

            for (const { url, jobIndex } of failedDetailUrls) {
                try {
                    const page = await fallbackContext.newPage();
                    await page.route('**/*', (route) => {
                        const type = route.request().resourceType();
                        if (['image', 'media', 'font'].includes(type)) return route.abort();
                        return route.continue();
                    });

                    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
                    await sleep(1000);

                    const html = await page.content();
                    const detail = extractJobDetail(html);

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

                    stats.playwrightFallback++;
                    await page.close();
                } catch (err) {
                    log.debug(`❌ Fallback failed: ${url.slice(0, 50)}`);
                }
            }

            await fallbackBrowser.close();
        } catch (err) {
            log.warning('Playwright fallback browser failed', { error: err.message });
        }
    }

    // ============ SAVE RESULTS ============
    log.info('💾 Saving results...');

    for (const job of allJobs.slice(0, results_wanted)) {
        await Dataset.pushData({
            ...job,
            keyword_search: keyword || null,
            location_search: location || null,
            extracted_at: new Date().toISOString(),
        });
        stats.jobsSaved++;
    }

    log.info('🎉 Scraping completed!', {
        ...stats,
        summary: `Pages: ${stats.pagesFetched}, Jobs: ${stats.jobsSaved} (${stats.gotSuccess} fast, ${stats.playwrightFallback} fallback)`
    });

    await Actor.setValue('STATS', stats);
});
