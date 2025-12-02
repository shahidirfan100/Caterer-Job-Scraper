import { Actor, Dataset, log } from 'apify';
import { PlaywrightCrawler, RequestQueue } from 'crawlee';
import { impit } from 'impit';
import * as cheerio from 'cheerio';

/**
 * Helpers
 */
const cleanText = (val) => (val ? val.replace(/\s+/g, ' ').trim() || null : null);

const toNumberOrDefault = (value, fallback) => {
    const num = Number(value);
    if (Number.isFinite(num) && num > 0) return num;
    return fallback;
};

const getJobIdFromUrl = (url) => {
    try {
        const u = new URL(url);
        const slug = u.pathname.split('/').filter(Boolean).pop();
        if (!slug) return null;
        return slug.split('?')[0];
    } catch {
        return null;
    }
};

const normalizeUrl = (url, baseUrl = 'https://www.caterer.com') => {
    try {
        if (!url) return null;
        if (url.startsWith('http')) return url;
        if (url.startsWith('//')) return `https:${url}`;
        if (url.startsWith('/')) return `${baseUrl}${url}`;
        return `${baseUrl}/${url}`;
    } catch {
        return null;
    }
};

const guessStartUrl = ({ keyword, location, startUrl }) => {
    if (startUrl && startUrl.trim()) return startUrl.trim();

    const base = 'https://www.caterer.com/jobs/search';
    const params = new URLSearchParams();
    if (keyword) params.set('keywords', keyword.trim());
    if (location) params.set('location', location.trim());
    return `${base}?${params.toString()}`;
};

const buildNextPageUrl = (currentUrl, nextPageNum) => {
    const url = new URL(currentUrl);
    url.searchParams.set('page', String(nextPageNum));
    return url.toString();
};

const randomDelay = (min = 1000, max = 3000) => {
    const delay = Math.floor(Math.random() * (max - min + 1)) + min;
    return new Promise(resolve => setTimeout(resolve, delay));
};

/**
 * JSON/HTML extraction helpers
 */
const tryParseJsonApi = (htmlString, requestUrl) => {
    try {
        const json = JSON.parse(htmlString);
        const records =
            (Array.isArray(json?.results) && json.results) ||
            (Array.isArray(json?.data?.results) && json.data.results) ||
            (Array.isArray(json?.jobs) && json.jobs) ||
            (Array.isArray(json) && json);

        if (!records) return [];

        return records
            .map((item) => {
                const title =
                    item.title ||
                    item.jobTitle ||
                    item.name ||
                    item.position ||
                    item.positionTitle;
                const company =
                    item.company ||
                    item.companyName ||
                    item.employer ||
                    item.hiringOrganization?.name;
                const location =
                    item.location ||
                    item.locationName ||
                    item.city ||
                    item.region ||
                    item.address;
                const salary = item.salary || item.salaryText || item.compensation;
                const url = item.url || item.link || item.jobUrl || item.applyUrl;
                const description = item.description || item.summary || item.teaser;
                const postedAt = item.datePosted || item.postedAt || item.posted || item.postedDate;
                const jobId =
                    item.id ||
                    item.jobId ||
                    item.jobReference ||
                    item.reference ||
                    getJobIdFromUrl(url);

                if (!title || !url) return null;

                return {
                    title: cleanText(title),
                    company: cleanText(company),
                    location: cleanText(location),
                    salary: cleanText(salary),
                    description: cleanText(description),
                    postedAt: cleanText(postedAt),
                    url,
                    jobId: cleanText(jobId),
                    source: 'api',
                    listingPageUrl: requestUrl,
                };
            })
            .filter(Boolean);
    } catch {
        return [];
    }
};

const extractJsonLdJobs = (html, requestUrl) => {
    const $ = cheerio.load(html);
    const jobs = [];

    $('script[type="application/ld+json"]').each((_, el) => {
        const raw = $(el).contents().text();
        if (!raw) return;
        try {
            const parsed = JSON.parse(raw.trim());
            const items = Array.isArray(parsed) ? parsed : [parsed];
            for (const item of items) {
                if (!item || (item['@type'] !== 'JobPosting' && item['@type'] !== 'Job')) continue;
                const title = item.title || item.name;
                const url = item.url || item.directApplyUrl || item.applicationContact?.url;
                const company =
                    item.hiringOrganization?.name ||
                    item.employer?.name ||
                    item.organization?.name;
                const location =
                    item.jobLocation?.address?.addressLocality ||
                    item.jobLocation?.address?.addressRegion ||
                    item.jobLocation?.address?.streetAddress ||
                    item.jobLocation?.address?.addressCountry;
                const salary =
                    item.baseSalary?.value?.value ||
                    item.baseSalary?.value ||
                    item.baseSalary ||
                    item.salary;
                const postedAt = item.datePosted || item.datePublished;
                const description = item.description;
                const jobId =
                    item.identifier?.value ||
                    item.identifier ||
                    item.id ||
                    getJobIdFromUrl(url);

                if (!title || !url) continue;

                jobs.push({
                    title: cleanText(title),
                    company: cleanText(company),
                    location: cleanText(location),
                    salary: cleanText(salary),
                    description: cleanText(description),
                    postedAt: cleanText(postedAt),
                    url,
                    jobId: cleanText(jobId),
                    source: 'json_ld',
                    listingPageUrl: requestUrl,
                });
            }
        } catch {
            // ignore bad blocks
        }
    });

    return jobs;
};

const extractJobsFromHtml = ($, requestUrl) => {
    const jobs = [];

    // Primary selector for job listings
    $('h2 a[href*="/job/"]').each((_, anchor) => {
        const $a = $(anchor);
        let url = $a.attr('href');
        const title = cleanText($a.text());
        if (!url || !title) return;

        // Normalize URL
        url = normalizeUrl(url);
        if (!url) return;

        // Find the job card container
        const card = $a.closest('article').first().length 
            ? $a.closest('article').first()
            : $a.closest('li').first().length 
            ? $a.closest('li').first()
            : $a.closest('[class*="job"]').first().length
            ? $a.closest('[class*="job"]').first()
            : $a.closest('div').first();

        // Try to extract company name
        let company = null;
        // Look for company in specific selectors
        const companySelectors = [
            card.find('span[class*="company"]').first().text(),
            card.find('div[class*="company"]').first().text(),
            card.find('[class*="employer"]').first().text(),
            card.find('p').first().text(), // Often first paragraph after title
        ];
        
        for (const compText of companySelectors) {
            const cleaned = cleanText(compText);
            if (cleaned && cleaned.length > 2 && cleaned.length < 100) {
                company = cleaned;
                break;
            }
        }

        // Try to extract location
        let location = null;
        const locationSelectors = [
            card.find('span[class*="location"]').first().text(),
            card.find('div[class*="location"]').first().text(),
            card.find('[class*="place"]').first().text(),
        ];

        for (const locText of locationSelectors) {
            const cleaned = cleanText(locText);
            if (cleaned) {
                location = cleaned;
                break;
            }
        }

        // Fallback: parse from all text
        if (!location || !company) {
            const allText = card.text();
            const lines = allText.split('\n').map(l => cleanText(l)).filter(Boolean);
            
            // Deduplicate consecutive lines
            const uniqueLines = [];
            for (const line of lines) {
                if (!uniqueLines.length || uniqueLines[uniqueLines.length - 1] !== line) {
                    uniqueLines.push(line);
                }
            }

            const isMoneyLine = (line) =>
                /[\u00A3$€]|\bper annum\b|\bper hour\b|\bper year\b|\bUp To\b|\bp\/h\b|\bp\.a\./i.test(line);
            const isPostedLine = (line) => /\b(ago|Today|Yesterday|hours?|days?|weeks?|months?)\s+ago\b/i.test(line);
            const isLocationLine = (line) => 
                /[A-Z]{1,2}\d[\dA-Z]?\s*\d[A-Z]{2}/.test(line) || // UK postcode
                /\b(London|Manchester|Birmingham|Leeds|Liverpool|Scotland|Wales|England|UK)\b/i.test(line);

            const titleIdx = uniqueLines.findIndex(l => l === title);
            
            // Extract company (usually first line after title)
            if (!company && titleIdx !== -1) {
                for (let i = titleIdx + 1; i < uniqueLines.length; i++) {
                    const line = uniqueLines[i];
                    if (isMoneyLine(line) || isPostedLine(line) || isLocationLine(line)) continue;
                    if (line.length > 2 && line.length < 100) {
                        company = line;
                        break;
                    }
                }
            }

            // Extract location
            if (!location) {
                for (const line of uniqueLines) {
                    if (isLocationLine(line) && !isMoneyLine(line) && !isPostedLine(line)) {
                        location = line;
                        break;
                    }
                }
            }
        }

        // Try to extract salary
        let salary = null;
        const salaryText = card.text();
        const salaryMatch = salaryText.match(/[\u00A3$€][\d,]+(?:\s*-\s*[\u00A3$€]?[\d,]+)?(?:\s+per\s+(?:annum|hour|year|week|day))?|Up\s+to\s+[\u00A3$€][\d,]+/i);
        if (salaryMatch) {
            salary = cleanText(salaryMatch[0]);
        }

        // Try to extract posted date
        let postedAt = null;
        const postedMatch = card.text().match(/(\d+\s+(?:hours?|days?|weeks?|months?)\s+ago|Today|Yesterday)/i);
        if (postedMatch) {
            postedAt = cleanText(postedMatch[0]);
        }

        const jobId = getJobIdFromUrl(url);

        jobs.push({
            title,
            company,
            location,
            salary,
            description: null, // Will be populated if collectDetails is enabled
            postedAt,
            url,
            jobId,
            source: 'html',
            listingPageUrl: requestUrl,
        });
    });

    return jobs;
};

const extractFromInlineState = (html, requestUrl) => {
    const matches = html.match(/__NEXT_DATA__\"?\s*>\s*({.+?})\s*</s);
    if (!matches) return [];
    try {
        const json = JSON.parse(matches[1]);
        const jobs =
            (json?.props?.pageProps?.jobs && Object.values(json.props.pageProps.jobs)) ||
            (Array.isArray(json?.props?.pageProps?.results) && json.props.pageProps.results) ||
            [];
        return tryParseJsonApi(JSON.stringify(jobs), requestUrl);
    } catch {
        return [];
    }
};

/**
 * Fetch and extract job details from detail page
 */
const fetchJobDetails = async (jobUrl, fetchViaHttp) => {
    try {
        await randomDelay(500, 1500); // Polite delay
        
        const response = await fetchViaHttp(jobUrl, 2);
        if (!response || response.statusCode !== 200 || !response.body) {
            log.warning('Failed to fetch job details', { jobUrl, status: response?.statusCode });
            return null;
        }

        const html = response.body;
        const $ = cheerio.load(html);

        // Extract full job description
        let descriptionHtml = null;
        let descriptionText = null;

        // Try various selectors for job description
        const descSelectors = [
            '[class*="job-description"]',
            '[class*="jobDescription"]',
            '[id*="job-description"]',
            '[class*="description"]',
            'article [class*="content"]',
            '.job-details',
            '#job-details',
        ];

        for (const selector of descSelectors) {
            const elem = $(selector).first();
            if (elem.length && elem.text().trim().length > 50) {
                descriptionHtml = elem.html();
                descriptionText = cleanText(elem.text());
                break;
            }
        }

        // Extract additional metadata
        let jobType = null;
        const jobTypeMatch = $('body').text().match(/\b(Permanent|Temporary|Contract|Part[- ]?time|Full[- ]?time)\b/i);
        if (jobTypeMatch) {
            jobType = cleanText(jobTypeMatch[0]);
        }

        // Try to extract application URL
        let applyUrl = null;
        const applyButton = $('a[href*="apply"], button[onclick*="apply"]').first();
        if (applyButton.length) {
            const href = applyButton.attr('href');
            if (href) {
                applyUrl = normalizeUrl(href);
            }
        }

        return {
            descriptionHtml,
            descriptionText,
            jobType,
            applyUrl,
        };
    } catch (err) {
        log.error('Error fetching job details', { jobUrl, message: err?.message });
        return null;
    }
};

/**
 * MAIN
 */
await Actor.main(async () => {
    const input = (await Actor.getInput()) || {};

    const {
        keyword = '',
        location = '',
        startUrl: inputStartUrl = '',
        results_wanted = 50,
        max_pages = 20,
        maxConcurrency = 3,
        proxyConfiguration: proxyInput,
        strategy = 'http_first', // http_first | playwright_only
    } = input;

    const targetResults = toNumberOrDefault(results_wanted, 50);
    const maxPages = toNumberOrDefault(max_pages, 20);
    const startUrl = guessStartUrl({ keyword, location, startUrl: inputStartUrl });

    log.info('Starting Caterer.com scraper (Playwright listing + HTML/JSON parsers)', {
        keyword,
        location,
        startUrl,
        results_wanted: targetResults,
        max_pages: maxPages,
        maxConcurrency,
        strategy,
    });

    // Configure proxy with residential IPs for better success rate
    const proxyConfiguration = await Actor.createProxyConfiguration(
        proxyInput || {
            groups: ['RESIDENTIAL'],
            countryCode: 'GB', // Use UK IPs for better geo-targeting
        }
    );
    log.info('Proxy configured', { 
        usingCustom: !!proxyInput,
        groups: proxyInput?.groups || ['RESIDENTIAL'],
        countryCode: proxyInput?.countryCode || 'GB',
    });

    const requestQueue = await RequestQueue.open();
    await requestQueue.addRequest({
        url: startUrl,
        userData: {
            label: 'LIST',
            pageNum: 1,
        },
        headers: buildHeaders(startUrl),
        useExtendedUniqueKey: true, // avoid duplicate addRequest collisions
    });

    const seenJobIds = new Set();
    const seenUrls = new Set();
    let jobsSaved = 0;
    let pagesProcessed = 0;

    const fetchViaHttp = async (url, retries = 3) => {
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                const opts = {
                    url,
                    headers: {
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'accept-language': 'en-GB,en;q=0.9',
                        'referer': 'https://www.caterer.com/',
                        'upgrade-insecure-requests': '1',
                    },
                    timeout: 30000,
                    retry: {
                        limit: 0, // We handle retries manually
                    },
                };

                if (proxyConfiguration) {
                    try {
                        const proxyUrl = await proxyConfiguration.newUrl();
                        if (proxyUrl) {
                            opts.proxyUrl = proxyUrl;
                        }
                    } catch (err) {
                        log.warning('Unable to set proxy for HTTP fetch', { message: err?.message });
                    }
                }

                const response = await impit(opts);
                
                if (response.statusCode === 200 && response.body) {
                    log.debug('HTTP fetch successful', { 
                        url, 
                        attempt, 
                        statusCode: response.statusCode,
                        bodyLength: response.body.length 
                    });
                    return response;
                }

                if ([403, 429, 503].includes(response.statusCode)) {
                    log.warning(`HTTP blocked with ${response.statusCode}, attempt ${attempt}/${retries}`);
                    if (attempt < retries) {
                        await randomDelay(2000 * attempt, 4000 * attempt); // Exponential backoff
                        continue;
                    }
                }

                return response;
            } catch (err) {
                log.warning(`HTTP fetch error, attempt ${attempt}/${retries}`, { 
                    url, 
                    message: err?.message 
                });
                if (attempt < retries) {
                    await randomDelay(1500 * attempt, 3000 * attempt);
                    continue;
                }
                throw err;
            }
        }
    };

    const crawler = new PlaywrightCrawler({
        requestQueue,
        proxyConfiguration,
        maxConcurrency: Math.max(1, maxConcurrency), // At least 1 concurrent request
        headless: true,
        navigationTimeoutSecs: 35,
        maxRequestRetries: 5,
        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: {
            maxPoolSize: 50,
            sessionOptions: {
                maxUsageCount: 10, // Rotate session after 10 uses
                maxErrorScore: 3, // Retire session after 3 errors
            },
        },
        launchContext: {
            launchOptions: {
                args: [
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-web-security',
                    '--disable-setuid-sandbox',
                    '--no-sandbox',
                ],
                ignoreHTTPSErrors: true,
            },
        },
        preNavigationHooks: [
            async ({ page, request, session }, gotoOptions) => {
                // Block unnecessary resources for faster scraping
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    if (['image', 'media', 'font', 'stylesheet'].includes(type)) {
                        return route.abort();
                    }
                    return route.continue();
                });

                await page.setExtraHTTPHeaders({
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'accept-language': 'en-GB,en;q=0.9',
                    'referer': 'https://www.caterer.com/',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                });

                // Additional stealth measures
                await page.addInitScript(() => {
                    // Override navigator.webdriver
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => false,
                    });

                    // Mock permissions
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );

                    // Add chrome runtime
                    window.chrome = {
                        runtime: {},
                    };
                });

                gotoOptions.waitUntil = 'domcontentloaded';
                gotoOptions.timeout = 30000;
            },
        ],
        requestHandler: async ({ page, request, log: crawlerLog, session }) => {
            const { label, pageNum } = request.userData;
            if (label !== 'LIST') return;

            if (jobsSaved >= targetResults) {
                crawlerLog.info('Target results reached, skipping page', {
                    url: request.url,
                    jobsSaved,
                    targetResults,
                });
                return;
            }

            pagesProcessed += 1;
            let html = '';
            let status = null;
            let contentType = '';
            let usedPlaywright = false;

            if (strategy === 'http_first') {
                try {
                    const res = await fetchViaHttp(request.url);
                    status = res.statusCode || null;
                    contentType = res.headers['content-type'] || '';
                    html = res.body?.toString?.('utf8') ?? '';
                    if (status === 200 && html) {
                        crawlerLog.info('Fetched via HTTP client', { status, contentType });
                    } else {
                        crawlerLog.warning('HTTP fetch blocked or empty, will fall back to Playwright', {
                            status,
                            contentType,
                        });
                    }
                } catch (err) {
                    crawlerLog.warning('HTTP fetch failed, falling back to Playwright', {
                        url: request.url,
                        message: err?.message,
                    });
                }
            }

            if (!html) {
                let response;
                usedPlaywright = true;
                try {
                    response = await page.goto(request.url, {
                        waitUntil: 'domcontentloaded',
                        timeout: 30000,
                    });
                } catch (err) {
                    crawlerLog.warning('page.goto failed, retrying once with networkidle', {
                        url: request.url,
                        message: err?.message,
                    });
                    try {
                        response = await page.goto(request.url, {
                            waitUntil: 'networkidle',
                            timeout: 35000,
                        });
                    } catch (err2) {
                        crawlerLog.warning('Second goto failed', { url: request.url, message: err2?.message });
                        throw err2;
                    }
                }

                status = response?.status() || 0;
                if (status === 403 || status === 429) {
                    crawlerLog.warning('Blocked response, retiring session', { url: request.url, status });
                    if (session) session.retire();
                    throw new Error(`Blocked with status ${status}`);
                }

                await page.waitForTimeout(1000);
                html = await page.content();
                contentType = response?.headers()['content-type'] || '';
            }

            const isJsonLike = contentType.includes('application/json') || html.trim().startsWith('{');
            const collected = [];

            if (isJsonLike) {
                collected.push(...tryParseJsonApi(html, request.url));
            }

            collected.push(...extractJsonLdJobs(html, request.url));

            const $ = cheerio.load(html);
            collected.push(...extractJobsFromHtml($, request.url));

            collected.push(...extractFromInlineState(html, request.url));

            const deduped = [];
            for (const job of collected) {
                const uniqueKey = job.jobId || job.url;
                if (!uniqueKey) continue;
                if (seenJobIds.has(uniqueKey) || seenUrls.has(job.url)) continue;
                
                // Validate job has minimum required fields
                if (!job.title || !job.url) {
                    crawlerLog.debug('Skipping job with missing required fields', { job });
                    continue;
                }
                
                seenJobIds.add(uniqueKey);
                seenUrls.add(job.url);
                deduped.push(job);
            }

            const remaining = targetResults - jobsSaved;
            let toSave = deduped.slice(0, remaining);

            // Fetch job details if collectDetails is enabled
            if (input.collectDetails && toSave.length) {
                crawlerLog.info('Fetching job details for listings', { count: toSave.length });
                
                const detailsPromises = toSave.map(async (job) => {
                    const details = await fetchJobDetails(job.url, fetchViaHttp);
                    if (details) {
                        return {
                            ...job,
                            descriptionHtml: details.descriptionHtml,
                            descriptionText: details.descriptionText || job.description,
                            jobType: details.jobType,
                            applyUrl: details.applyUrl,
                        };
                    }
                    return job;
                });

                try {
                    toSave = await Promise.all(detailsPromises);
                    crawlerLog.info('Job details fetched successfully');
                } catch (err) {
                    crawlerLog.error('Error fetching job details batch', { message: err?.message });
                }
            }

            if (toSave.length) {
                await Dataset.pushData(
                    toSave.map((job) => ({
                        ...job,
                        searchKeyword: keyword || null,
                        searchLocation: location || null,
                        pageNum,
                        transport: usedPlaywright ? 'playwright' : 'http',
                        scrapedAt: new Date().toISOString(),
                    })),
                );
                jobsSaved += toSave.length;
                crawlerLog.info('Saved jobs from page', {
                    pageNum,
                    saved: toSave.length,
                    jobsSaved,
                    targetResults,
                    sourceMix: Array.from(new Set(toSave.map((j) => j.source))),
                    withDetails: input.collectDetails || false,
                });
            } else {
                crawlerLog.warning('No jobs extracted on page', {
                    url: request.url,
                    pageNum,
                });
            }

            if (jobsSaved >= targetResults) {
                crawlerLog.info('Target results reached, stopping pagination', {
                    jobsSaved,
                    targetResults,
                });
                return;
            }
            if (pageNum >= maxPages) {
                crawlerLog.info('Max pages reached, stopping', { pageNum, maxPages });
                return;
            }

            // Check if we got any jobs from this page - stop if empty
            if (deduped.length === 0 && pageNum > 1) {
                crawlerLog.warning('No jobs found on page, assuming end of results', { pageNum });
                return;
            }

            // Add polite delay before queuing next page
            await randomDelay(1000, 2500);

            const nextPageNum = pageNum + 1;
            const nextUrl = buildNextPageUrl(request.url, nextPageNum);

            await requestQueue.addRequest({
                url: nextUrl,
                userData: {
                    label: 'LIST',
                    pageNum: nextPageNum,
                },
                headers: {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'accept-language': 'en-GB,en;q=0.9',
                    'referer': request.url,
                    'upgrade-insecure-requests': '1',
                },
                useExtendedUniqueKey: true,
            });

            crawlerLog.info('Queued next page', { nextPageNum, nextUrl });
        },
        failedRequestHandler: async ({ request, error, log: crawlerLog }) => {
            crawlerLog.error('Request failed after retries', {
                url: request.url,
                message: error?.message || String(error),
            });
        },
    });

    await crawler.run();

    log.info('Scraping completed', {
        jobsSaved,
        targetResults,
        pagesProcessed,
        seenJobIds: seenJobIds.size,
    });
});
