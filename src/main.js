// src/main.js
// Caterer.com Job Scraper - Production Ready
// Uses HTTP scraping with DOM extraction + Playwright fallback for cookies

import { Actor, log } from 'apify';
import { Dataset } from 'crawlee';
import { chromium } from 'playwright';
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
 * Build pagination URL (preserves path, adds/updates page param)
 */
const buildPaginatedUrl = (baseUrl, page) => {
    const url = new URL(baseUrl);
    url.searchParams.set('page', String(page));
    return url.href;
};

/**
 * Simple delay helper
 */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Random fingerprint for browser simulation
 */
const randomFingerprint = () => {
    const mobile = Math.random() < 0.3;
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

/**
 * Dismiss cookie popups in Playwright
 */
const dismissPopups = async (page) => {
    const selectors = [
        'button:has-text("Accept all")',
        'button:has-text("Accept All")',
        '[id*="accept"][type="button"]',
        '[data-testid*="accept"]',
    ];
    for (const selector of selectors) {
        try {
            const btn = page.locator(selector).first();
            if (await btn.isVisible({ timeout: 1000 })) {
                await btn.click({ delay: 50 }).catch(() => { });
                break;
            }
        } catch {
            // ignore
        }
    }
};

/**
 * Check if HTML looks like a block page
 */
const looksBlocked = (html) => {
    const lower = html.toLowerCase();
    return ['captcha', 'access denied', 'verify you are a human', 'unusual traffic'].some((s) =>
        lower.includes(s),
    );
};

/**
 * Extract salary from text
 */
const extractSalary = (text) => {
    if (!text) return null;
    const compact = text.replace(/\s+/g, ' ').trim();
    const perMatch = compact.match(/(£[^£]+?per (?:hour|annum|year|week|day))/i);
    if (perMatch) return perMatch[1].trim();
    const simpleMatch = compact.match(/(£[^£]+?)(?:\s{2,}|$)/);
    if (simpleMatch) return simpleMatch[1].trim();
    return null;
};

/**
 * Extract jobs from listing page HTML using DOM parsing
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

        // Find job card container
        const container =
            $(el).closest('article').length ? $(el).closest('article') :
                $(el).closest('li').length ? $(el).closest('li') :
                    $(el).closest('div').length ? $(el).closest('div') :
                        $(el).parent();

        const $container = container.clone();
        $container.find('style,script,noscript').remove();
        const fullText = $container.text().replace(/\s+/g, ' ').trim();

        // Extract company from img alt
        let company = $container.find('img[alt]').attr('alt') || null;
        if (!company || company.length < 2 || company.length > 80) {
            company = null;
        }

        const salary = extractSalary(fullText);

        // Extract location between company and salary
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

    // DOM fallback for description
    $('style,script,noscript').remove();
    if (!result.description_html) {
        const descEl = $('[data-at="job-description"]').first().length ? $('[data-at="job-description"]').first() :
            $('#job-description').first().length ? $('#job-description').first() :
                $('.job-description').first().length ? $('.job-description').first() :
                    $('main article').first();
        if (descEl && descEl.length) {
            result.description_html = descEl.html();
            result.description_text = descEl.text().replace(/\s+/g, ' ').trim();
        }
    }

    // Fallback extractions
    if (!result.salary) result.salary = extractSalary($('body').text());
    if (!result.company) result.company = $('img[alt]').first().attr('alt') || null;
    if (!result.title) result.title = $('h1').first().text().replace(/\s+/g, ' ').trim() || null;

    return result;
};

/**
 * HTTP fetch with retry logic
 */
const httpFetch = async (url, userAgent, cookieHeader, proxyUrl, retries = 3) => {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const res = await gotScraping({
                url,
                proxyUrl: proxyUrl || undefined,
                timeout: { request: 25000 },
                http2: false,
                headers: {
                    'User-Agent': userAgent,
                    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-GB,en;q=0.9',
                    Connection: 'keep-alive',
                    Referer: BASE_URL + '/',
                    ...(cookieHeader ? { Cookie: cookieHeader } : {}),
                },
            });

            const { statusCode, body } = res;
            if (statusCode === 403 || looksBlocked(body || '')) {
                if (attempt < retries) {
                    log.warning(`Blocked (${statusCode}), retry ${attempt}/${retries}`, { url });
                    await sleep(2000 * attempt);
                    continue;
                }
                throw new Error(`Blocked (status: ${statusCode})`);
            }
            return body || '';
        } catch (err) {
            if (attempt < retries && !String(err.message).includes('Blocked')) {
                log.warning(`HTTP error, retry ${attempt}/${retries}`, { url, error: err.message });
                await sleep(1500 * attempt);
                continue;
            }
            throw err;
        }
    }
};

/**
 * Playwright handshake - get cookies for HTTP requests
 */
const playwrightHandshake = async (startUrl, proxyConfig) => {
    const fp = randomFingerprint();
    const proxyUrl = proxyConfig ? await proxyConfig.newUrl() : null;

    for (let attempt = 1; attempt <= 2; attempt++) {
        try {
            const browser = await chromium.launch({
                headless: true,
                proxy: proxyUrl ? { server: proxyUrl } : undefined,
                args: [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--lang=en-GB',
                ],
            });

            const context = await browser.newContext({
                userAgent: fp.ua,
                viewport: fp.viewport,
                ignoreHTTPSErrors: true,
                locale: 'en-GB',
            });

            const page = await context.newPage();
            await page.addInitScript(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
            });

            // Block heavy resources
            await page.route('**/*', (route) => {
                const type = route.request().resourceType();
                if (['image', 'media', 'font', 'stylesheet'].includes(type)) return route.abort();
                return route.continue();
            });

            const response = await page.goto(startUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
            await dismissPopups(page);

            const status = response?.status() ?? null;
            const html = await page.content();

            if (status === 403 || looksBlocked(html)) {
                throw new Error(`Blocked (status: ${status})`);
            }

            const cookies = await context.cookies(BASE_URL);
            const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
            await browser.close();

            log.info('Playwright handshake OK', { status, cookies: cookies.length });
            return { userAgent: fp.ua, cookieHeader, proxyUrl, initialHtml: html };
        } catch (err) {
            log.warning(`Handshake failed attempt ${attempt}`, { error: err.message });
            if (attempt === 2) {
                log.warning('Using HTTP-only mode (no cookies)');
                return { userAgent: fp.ua, cookieHeader: '', proxyUrl: null, initialHtml: null };
            }
        }
    }
};

/**
 * Enrich jobs with detail page data
 */
const enrichWithDetails = async (jobs, userAgent, cookieHeader, proxyUrl, concurrency = 3) => {
    const results = [];
    let idx = 0;

    const worker = async () => {
        while (idx < jobs.length) {
            const i = idx++;
            const job = jobs[i];

            // Stealth delay
            if (i > 0) await sleep(500 + Math.random() * 500);

            try {
                const html = await httpFetch(job.url, userAgent, cookieHeader, proxyUrl);
                const detail = extractJobDetail(html);
                results.push({
                    ...job,
                    title: detail.title || job.title,
                    company: detail.company || job.company,
                    salary: detail.salary || job.salary,
                    date_posted: detail.date_posted || job.date_posted,
                    job_type: detail.job_type || null,
                    description_html: detail.description_html || null,
                    description_text: detail.description_text || null,
                });
            } catch (err) {
                log.warning('Detail fetch failed', { url: job.url, error: err.message });
                results.push({ ...job, description_html: null, description_text: null });
            }
        }
    };

    const workers = Array.from({ length: Math.min(concurrency, jobs.length) }, () => worker());
    await Promise.all(workers);
    return results;
};

// ============ MAIN ============
await Actor.main(async () => {
    const input = (await Actor.getInput()) ?? {};
    const {
        keyword = '',
        location = '',
        startUrl = '',
        results_wanted = 20,
        max_pages = 5,
        max_detail_concurrency = 3,
        proxyConfiguration: proxyInput,
    } = input;

    const startUrlToUse = startUrl?.trim() || buildSearchUrl(keyword, location, 1);

    log.info('Starting Caterer.com job scraper', {
        keyword, location, startUrl: startUrlToUse, results_wanted, max_pages,
    });

    // Setup proxy
    let proxyConfig = null;
    if (proxyInput || process.env.APIFY_PROXY_PASSWORD) {
        try {
            proxyConfig = await Actor.createProxyConfiguration(
                proxyInput ?? { groups: ['RESIDENTIAL'], countryCode: 'GB' }
            );
            log.info('Proxy configured');
        } catch (err) {
            log.warning('Proxy setup failed', { error: err.message });
        }
    }

    // Playwright handshake for cookies
    const { userAgent, cookieHeader, proxyUrl, initialHtml } = await playwrightHandshake(startUrlToUse, proxyConfig);

    const savedUrls = new Set();
    let savedCount = 0;
    const stats = { pagesFetched: 0, jobsExtracted: 0, jobsSaved: 0 };

    const pagesToVisit = Math.min(max_pages, 20);
    let initialUsed = false;

    for (let pageNum = 1; pageNum <= pagesToVisit; pageNum++) {
        if (savedCount >= results_wanted) break;

        const url = pageNum === 1 ? startUrlToUse : buildPaginatedUrl(startUrlToUse, pageNum);
        log.info(`Processing page ${pageNum}/${pagesToVisit}`, { url });

        // Fetch HTML
        let html;
        try {
            if (pageNum === 1 && initialHtml && !initialUsed) {
                html = initialHtml;
                initialUsed = true;
            } else {
                html = await httpFetch(url, userAgent, cookieHeader, proxyUrl);
            }
            stats.pagesFetched++;
        } catch (err) {
            log.warning('Page fetch failed', { url, error: err.message });
            if (String(err.message).includes('Blocked')) break;
            continue;
        }

        // Extract jobs from DOM
        const jobs = extractJobsFromDom(html, url);
        stats.jobsExtracted += jobs.length;
        log.info(`Extracted ${jobs.length} jobs from page ${pageNum}`);

        if (!jobs.length) {
            log.info('No jobs found, stopping pagination');
            break;
        }

        // Filter and cap
        const remaining = results_wanted - savedCount;
        const toProcess = [];
        for (const job of jobs) {
            if (toProcess.length >= remaining) break;
            if (!job.url || savedUrls.has(job.url)) continue;
            savedUrls.add(job.url);
            toProcess.push(job);
        }

        if (!toProcess.length) continue;

        // Enrich with details
        log.info(`Fetching details for ${toProcess.length} jobs`);
        const enriched = await enrichWithDetails(toProcess, userAgent, cookieHeader, proxyUrl, max_detail_concurrency);

        // Save to dataset
        for (const job of enriched) {
            if (savedCount >= results_wanted) break;
            await Dataset.pushData({
                ...job,
                keyword_search: keyword || null,
                location_search: location || null,
                extracted_at: new Date().toISOString(),
            });
            savedCount++;
            stats.jobsSaved++;
            log.info(`Saved job ${savedCount}/${results_wanted}: ${job.title}`);
        }

        // Delay between pages
        if (pageNum < pagesToVisit && savedCount < results_wanted) {
            await sleep(400 + Math.random() * 400);
        }
    }

    log.info('Scraping completed', stats);
    await Actor.setValue('STATS', stats);
});
