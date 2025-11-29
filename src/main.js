// src/main.js
import { Actor, log } from 'apify';
import { chromium } from 'playwright';

const BASE_URL = 'https://www.caterer.com';

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

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
 * Build URL for the next page by setting ?page=N on top of the original start URL.
 */
const buildNextPageUrl = (startUrl, pageNum) => {
    const url = new URL(startUrl);
    if (pageNum > 1) {
        url.searchParams.set('page', String(pageNum));
    } else {
        url.searchParams.delete('page');
    }
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

const looksBlockedHtml = (html) => {
    const lower = html.toLowerCase();
    return ['captcha', 'access denied', 'verify you are a human', 'unusual traffic'].some((s) =>
        lower.includes(s),
    );
};

/**
 * Robust goto with retries for transient transport errors like ERR_EMPTY_RESPONSE.
 */
const gotoWithRetries = async (page, url, maxRetries = 3) => {
    let lastError;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            log.info('Navigating', { url, attempt });
            const res = await page.goto(url, {
                waitUntil: 'domcontentloaded',
                timeout: 30000,
            });
            return res;
        } catch (err) {
            lastError = err;
            const msg = (err && err.message) || '';
            log.warning('Navigation failed', { url, attempt, message: msg });

            // Only retry for likely network / transport errors
            if (
                !msg.includes('ERR_EMPTY_RESPONSE') &&
                !msg.includes('ERR_CONNECTION_RESET') &&
                !msg.includes('ETIMEDOUT')
            ) {
                break;
            }

            if (attempt < maxRetries) {
                await sleep(1000 + Math.floor(Math.random() * 1000));
            }
        }
    }
    throw lastError;
};

/**
 * Extract jobs from listing page via JS state + DOM fallback.
 */
const extractJobsFromListingPage = async (page) => {
    return page.evaluate(() => {
        const jobs = [];
        const seen = new Set();

        const urlObj = new URL(location.href);

        const pushJob = (job) => {
            if (!job.url) return;
            if (seen.has(job.url)) return;
            seen.add(job.url);
            jobs.push(job);
        };

        // 1) JS state (preferred – clean title, companyName, salary, location)
        try {
            // eslint-disable-next-line no-undef
            const state = window.__PRELOADED_STATE__ || {};
            const recProps = state?.RecommenderWidget_listing_list?.props;
            const recItems = recProps?.jobAdsData?.items || [];

            for (const item of recItems) {
                const fullUrl = new URL(item.url, urlObj.origin).href;
                pushJob({
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
                    raw_date: null,
                });
            }

            // Sometimes main results are in a "SearchListing" slice
            const searchProps = state?.SearchListing?.props;
            const searchItems = searchProps?.jobAdsData?.items || [];
            for (const item of searchItems) {
                const fullUrl = new URL(item.url, urlObj.origin).href;
                pushJob({
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
                    raw_date: null,
                });
            }
        } catch {
            // ignore and rely on DOM fallback
        }

        // 2) DOM fallback if JS state gave nothing or only partial
        if (!jobs.length) {
            // Clean up style/script/noscript
            document.querySelectorAll('style,script,noscript').forEach((el) => el.remove());

            const anchors = Array.from(
                document.querySelectorAll('main a[href*="/job/"], a[href*="/job/"]'),
            );

            for (const anchor of anchors) {
                const href = anchor.getAttribute('href');
                if (!href) continue;

                const jobUrl = new URL(href, urlObj.origin).href;
                if (seen.has(jobUrl)) continue;

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

                // Salary
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

                // Company
                let company = clone.querySelector('img[alt]')?.getAttribute('alt') || null;
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

                // Location: between company and salary
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

                // Relative date
                const dateChunk = fullText.split(' ').slice(-4).join(' ');
                const raw_date = /ago/i.test(dateChunk) ? dateChunk : null;

                pushJob({
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
                    raw_date,
                });
            }
        }

        return jobs;
    });
};

/**
 * Extract detail data (description, better salary/company/title/date) from a job detail page.
 */
const extractJobDetailFromPage = async (page) => {
    return page.evaluate(() => {
        const result = {};

        const cleanText = (node) =>
            node.textContent?.replace(/\s+/g, ' ').trim() || '';

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

        // 2) DOM fallback
        document.querySelectorAll('style,script,noscript').forEach((el) => el.remove());

        if (!result.description_html) {
            const descEl =
                document.querySelector('[data-at*="job-description"]') ||
                document.querySelector('#job-description') ||
                document.querySelector('.job-description') ||
                document.querySelector('main article') ||
                document.querySelector('main section');

            if (descEl) {
                const clone = descEl.cloneNode(true);
                clone.querySelectorAll('style,script,noscript').forEach((el) => el.remove());
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

        // Try to find a "posted x days ago" text for date
        if (!result.date_posted) {
            const txtNode = Array.from(
                document.querySelectorAll('span, p, li, div'),
            ).find((el) => /ago$/i.test(cleanText(el)));
            if (txtNode) {
                result.raw_date = cleanText(txtNode);
            }
        }

        return result;
    });
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

    log.info('Starting Caterer.com Playwright scraper (no Crawlee)', {
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

    let proxyConfiguration = null;
    let proxyUrl = null;

    if (hasProxyCredentials) {
        try {
            proxyConfiguration = await Actor.createProxyConfiguration(
                proxyFromInput ?? {
                    groups: ['RESIDENTIAL'],
                    countryCode: 'GB',
                },
            );
            proxyUrl = await proxyConfiguration.newUrl();
            log.info('Proxy configured', { usingCustom: Boolean(proxyFromInput) });
        } catch (proxyError) {
            log.warning('Proxy setup failed, continuing without proxy', {
                error: proxyError.message,
            });
        }
    } else {
        log.info('No Apify proxy credentials detected, running without proxy');
    }

    const stats = {
        listPagesVisited: 0,
        listPagesFailed: 0,
        detailPagesVisited: 0,
        detailPagesFailed: 0,
        jobsFromState: 0,
        jobsFromDom: 0,
        jobsSaved: 0,
        detailBlocked403: 0,
        blockedPages: 0,
    };

    const savedUrls = new Set();
    let savedCount = 0;

    const fp = randomFingerprint();

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

    try {
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

        // Block heavy resources
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

        const allJobs = [];

        for (let pageNum = 1; pageNum <= max_pages; pageNum++) {
            if (savedCount >= results_wanted && !collectDetails) break;

            const url = buildNextPageUrl(startUrlToUse, pageNum);
            log.info(`Processing listing page ${pageNum}/${max_pages}`, { url });

            try {
                const res = await gotoWithRetries(page, url);
                const status = res ? res.status() : null;

                const html = await page.content();
                if (status === 403 || looksBlockedHtml(html)) {
                    stats.blockedPages++;
                    log.warning('Listing page looks blocked; stopping pagination', {
                        url,
                        status,
                    });
                    break;
                }

                await dismissPopups(page);

                stats.listPagesVisited++;

                const jobs = await extractJobsFromListingPage(page);

                if (!jobs || !jobs.length) {
                    log.info('No jobs found on listing page, stopping pagination.', {
                        url,
                        pageNum,
                    });
                    break;
                }

                const fromStateCount = jobs.filter((j) => j.job_id != null).length;
                const fromDomCount = jobs.length - fromStateCount;
                stats.jobsFromState += fromStateCount;
                stats.jobsFromDom += fromDomCount;

                log.info(`Extracted ${jobs.length} jobs from listing page`, {
                    url,
                    pageNum,
                    fromState: fromStateCount,
                    fromDom: fromDomCount,
                });

                if (!collectDetails) {
                    for (const job of jobs) {
                        if (savedCount >= results_wanted) break;
                        if (!job.url || savedUrls.has(job.url)) continue;

                        if (!job.date_posted && job.raw_date) {
                            job.date_posted = parsePostedRelative(job.raw_date);
                            delete job.raw_date;
                        }

                        const finalJob = {
                            ...job,
                            keyword_search: keyword || null,
                            location_search: location || null,
                            extracted_at: new Date().toISOString(),
                        };

                        await Actor.pushData(finalJob);
                        savedUrls.add(job.url);
                        savedCount++;
                        stats.jobsSaved++;

                        log.info(`Saved job ${savedCount}/${results_wanted} (listing only)`, {
                            title: finalJob.title,
                            url: finalJob.url,
                        });
                    }
                } else {
                    for (const job of jobs) {
                        if (!job.url || savedUrls.has(job.url)) continue;
                        allJobs.push(job);
                        savedUrls.add(job.url);
                    }
                }

                const needMoreListing =
                    collectDetails || savedCount < results_wanted;

                if (!needMoreListing) {
                    break;
                }

                // Small jitter between listing pages for stealth
                if (pageNum < max_pages) {
                    await sleep(300 + Math.floor(Math.random() * 400));
                }
            } catch (err) {
                stats.listPagesFailed++;
                log.error('Listing page failed, stopping pagination', {
                    url,
                    pageNum,
                    message: err?.message,
                });
                break;
            }
        }

        // Detail scraping phase (sequential, stealthy)
        if (collectDetails && allJobs.length) {
            log.info('Starting detail scraping', {
                totalJobs: allJobs.length,
                target: results_wanted,
            });

            let detailIndex = 0;
            for (const baseJob of allJobs) {
                if (savedCount >= results_wanted) break;

                detailIndex++;
                log.info(`Detail ${detailIndex}/${allJobs.length}`, {
                    url: baseJob.url,
                });

                try {
                    const res = await gotoWithRetries(page, baseJob.url, 2);
                    const status = res ? res.status() : null;
                    const detailHtml = await page.content();

                    if (status === 403 || looksBlockedHtml(detailHtml)) {
                        stats.detailBlocked403++;
                        stats.detailPagesFailed++;
                        log.warning('Detail page blocked (403/CAPTCHA), keeping listing data', {
                            url: baseJob.url,
                            status,
                        });

                        const jobCopy = { ...baseJob };

                        if (!jobCopy.date_posted && jobCopy.raw_date) {
                            jobCopy.date_posted = parsePostedRelative(jobCopy.raw_date);
                        }
                        delete jobCopy.raw_date;

                        const finalJob = {
                            ...jobCopy,
                            keyword_search: keyword || null,
                            location_search: location || null,
                            extracted_at: new Date().toISOString(),
                        };

                        await Actor.pushData(finalJob);
                        savedCount++;
                        stats.jobsSaved++;

                        continue;
                    }

                    await dismissPopups(page);

                    stats.detailPagesVisited++;

                    const detailData = await extractJobDetailFromPage(page);

                    const merged = {
                        ...baseJob,
                        ...Object.fromEntries(
                            Object.entries(detailData).filter(([, v]) => v != null),
                        ),
                    };

                    if (!merged.date_posted && (merged.raw_date || detailData.raw_date)) {
                        const parsed = parsePostedRelative(
                            merged.raw_date || detailData.raw_date,
                        );
                        if (parsed) merged.date_posted = parsed;
                    }
                    delete merged.raw_date;

                    const finalJob = {
                        ...merged,
                        keyword_search: keyword || null,
                        location_search: location || null,
                        extracted_at: new Date().toISOString(),
                    };

                    await Actor.pushData(finalJob);
                    savedCount++;
                    stats.jobsSaved++;

                    log.info(`Saved job ${savedCount}/${results_wanted} (with detail)`, {
                        title: finalJob.title,
                        url: finalJob.url,
                    });

                    // Small delay between detail pages for stealth
                    await sleep(300 + Math.floor(Math.random() * 500));
                } catch (err) {
                    stats.detailPagesFailed++;
                    log.warning('Detail page failed, keeping listing data only', {
                        url: baseJob.url,
                        message: err?.message,
                    });

                    const jobCopy = { ...baseJob };
                    if (!jobCopy.date_posted && jobCopy.raw_date) {
                        jobCopy.date_posted = parsePostedRelative(jobCopy.raw_date);
                    }
                    delete jobCopy.raw_date;

                    const finalJob = {
                        ...jobCopy,
                        description_html: null,
                        description_text: null,
                        keyword_search: keyword || null,
                        location_search: location || null,
                        extracted_at: new Date().toISOString(),
                    };

                    await Actor.pushData(finalJob);
                    savedCount++;
                    stats.jobsSaved++;

                    await sleep(300 + Math.floor(Math.random() * 500));
                }
            }
        }

    } finally {
        await browser.close().catch(() => {});
    }

    log.info('Scraping completed', {
        jobsSaved: savedCount,
        target: results_wanted,
        stats,
    });

    await Actor.setValue('STATS', stats);
});
