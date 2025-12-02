import { Actor, Dataset, log } from 'apify';
import { BasicCrawler, RequestQueue } from 'crawlee';
import { Impit } from 'impit';
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
    return new Promise((resolve) => setTimeout(resolve, delay));
};

const buildHeaders = (url) => ({
    accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'accept-language': 'en-GB,en;q=0.9',
    referer: url || 'https://www.caterer.com/',
    'upgrade-insecure-requests': '1',
});

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
                const postedAt = item.datePosted || item.datePublished;
                const jobId =
                    item.identifier?.value ||
                    item.identifier ||
                    item.id ||
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
    } catch (err) {
        log.debug('Failed to parse JSON API response', { url: requestUrl, message: err?.message });
        return [];
    }
};

const extractJsonLdJobs = (html, requestUrl) => {
    try {
        const $ = cheerio.load(html);
        const jobs = [];

        $('script[type="application/ld+json"]').each((_, el) => {
            let jsonText = $(el).contents().toString().trim();
            if (!jsonText) return;

            try {
                const data = JSON.parse(jsonText);

                const candidates = Array.isArray(data) ? data : [data];
                for (const block of candidates) {
                    const graphItems = Array.isArray(block['@graph']) ? block['@graph'] : [block];

                    for (const item of graphItems) {
                        if (!item || typeof item !== 'object') continue;
                        const type = item['@type'];
                        if (type !== 'JobPosting' && type !== 'JobPostingDetails') continue;

                        const title = item.title || item.positionTitle || item.name;
                        const company =
                            item.hiringOrganization?.name ||
                            item.hiringOrganization ||
                            item.employer ||
                            item.companyName;
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
                        const url =
                            item.url ||
                            item.applyUrl ||
                            item.hiringOrganization?.sameAs ||
                            item.mainEntityOfPage?.['@id'] ||
                            getJobIdFromUrl(item.identifier?.value || item.identifier);

                        const jobId =
                            item.identifier?.value ||
                            item.identifier ||
                            getJobIdFromUrl(url);

                        if (!title || !url) continue;

                        jobs.push({
                            title: cleanText(title),
                            company: cleanText(company),
                            location: cleanText(location),
                            salary: cleanText(salary),
                            description: cleanText(description),
                            postedAt: cleanText(postedAt),
                            url: normalizeUrl(url),
                            jobId: cleanText(jobId),
                            source: 'json_ld',
                            listingPageUrl: requestUrl,
                        });
                    }
                }
            } catch (err) {
                log.debug('Failed to parse JSON-LD block', { url: requestUrl, message: err?.message });
            }
        });

        return jobs;
    } catch (err) {
        log.debug('Error while extracting JSON-LD', { url: requestUrl, message: err?.message });
        return [];
    }
};

const extractJobsFromHtml = ($, listingPageUrl) => {
    const jobs = [];

    // Primary selector for job listings (may need adjustment for current DOM)
    $('h2 a[href*="/job/"]').each((_, anchor) => {
        const $a = $(anchor);
        let url = $a.attr('href');
        const title = cleanText($a.text());
        if (!url || !title) return;

        url = normalizeUrl(url);
        if (!url) return;

        const card = $a.closest('article').first().length
            ? $a.closest('article').first()
            : $a.closest('li').first().length
            ? $a.closest('li').first()
            : $a.closest('[class*="job"]').first().length
            ? $a.closest('[class*="job"]').first()
            : $a.closest('div').first();

        let company = null;
        const companySelectors = [
            card.find('span[class*="company"]').first().text(),
            card.find('div[class*="company"]').first().text(),
            card.find('[class*="employer"]').first().text(),
            card.find('p').first().text(),
        ];
        for (const text of companySelectors) {
            const cleaned = cleanText(text);
            if (
                cleaned &&
                cleaned.length > 2 &&
                !cleaned.toLowerCase().includes(cleanText(title)?.toLowerCase() || '')
            ) {
                company = cleaned;
                break;
            }
        }

        const textLines = [];
        card.find('*').each((_, el) => {
            const t = cleanText($(el).text());
            if (t && !textLines.includes(t)) {
                textLines.push(t);
            }
        });

        let location = null;
        let salary = null;
        let postedAt = null;

        const lines = textLines.filter((line) => {
            if (line.toLowerCase().includes(cleanText(title)?.toLowerCase() || '')) return false;
            if (company && line.toLowerCase().includes(company.toLowerCase())) return false;
            return true;
        });

        const uniqueLines = [];
        for (const line of lines) {
            if (!uniqueLines.length || uniqueLines[uniqueLines.length - 1] !== line) {
                uniqueLines.push(line);
            }
        }

        const isMoneyLine = (line) =>
            /[\u00A3$€]|\bper annum\b|\bper hour\b|\bper year\b|\bUp To\b|\bp\/h\b|\bp\.a\./i.test(line);
        const isPostedLine = (line) =>
            /\b(ago|Today|Yesterday|hours?|days?|weeks?|months?)\s+ago\b/i.test(line);
        const isLocationLine = (line) =>
            /[A-Z]{1,2}\d[\dA-Z]?\s*\d[A-Z]{2}/.test(line) ||
            /\b(London|Manchester|Birmingham|Leeds|Liverpool|Scotland|Wales|England|UK)\b/i.test(
                line,
            );

        for (const line of uniqueLines) {
            if (!salary && isMoneyLine(line)) salary = line;
            else if (!postedAt && isPostedLine(line)) postedAt = line;
            else if (!location && isLocationLine(line)) location = line;
        }

        jobs.push({
            title,
            company,
            location,
            salary,
            postedAt,
            description: null,
            url,
            jobId: getJobIdFromUrl(url),
            source: 'html',
            listingPageUrl,
        });
    });

    return jobs;
};

const extractFromInlineState = (html, requestUrl) => {
    const jobs = [];

    const patterns = [
        /__NEXT_DATA__\s*=\s*({.*?});<\/script/s,
        /window\.__INITIAL_STATE__\s*=\s*({.*?});/s,
        /window\.__APP_STATE__\s*=\s*({.*?});/s,
    ];

    for (const pattern of patterns) {
        const match = html.match(pattern);
        if (!match) continue;

        try {
            const jsonText = match[1];
            const data = JSON.parse(jsonText);

            const findJobsInObject = (obj) => {
                if (!obj || typeof obj !== 'object') return;
                for (const key of Object.keys(obj)) {
                    const val = obj[key];
                    if (Array.isArray(val)) {
                        if (
                            val.length &&
                            typeof val[0] === 'object' &&
                            (val[0].jobTitle || val[0].title || val[0].position)
                        ) {
                            for (const item of val) {
                                const title =
                                    item.title ||
                                    item.jobTitle ||
                                    item.name ||
                                    item.position;
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
                                const salary =
                                    item.salary || item.salaryText || item.compensation;
                                const postedAt = item.datePosted || item.datePublished;
                                const url =
                                    item.url ||
                                    item.link ||
                                    item.jobUrl ||
                                    item.applyUrl;
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
                                    description: cleanText(
                                        item.description || item.summary || item.teaser,
                                    ),
                                    postedAt: cleanText(postedAt),
                                    url: normalizeUrl(url),
                                    jobId: cleanText(jobId),
                                    source: 'inline_state',
                                    listingPageUrl: requestUrl,
                                });
                            }
                        } else {
                            for (const v of val) {
                                if (typeof v === 'object') findJobsInObject(v);
                            }
                        }
                    } else if (typeof val === 'object') {
                        findJobsInObject(val);
                    }
                }
            };

            findJobsInObject(data);
        } catch (err) {
            log.debug('Failed to parse inline state JSON', { url: requestUrl, message: err?.message });
        }
    }

    return jobs;
};

const fetchJobDetails = async (jobUrl, fetchViaHttp) => {
    try {
        await randomDelay(500, 1500);

        const response = await fetchViaHttp(jobUrl, 2);
        if (!response || response.statusCode !== 200 || !response.body) {
            log.warning('Failed to fetch job details (non-200 or empty body)', {
                jobUrl,
                status: response?.statusCode,
            });
            return null;
        }

        const html = response.body;
        const $ = cheerio.load(html);

        let descriptionHtml = null;
        let descriptionText = null;

        const descriptionSelectors = [
            '[class*="job-description"]',
            '[data-test="job-description"]',
            '.job-description',
            '.description',
            '#job-description',
            'section[role="main"]',
        ];

        for (const sel of descriptionSelectors) {
            const el = $(sel).first();
            if (el.length) {
                descriptionHtml = el.html();
                descriptionText = cleanText(el.text());
                break;
            }
        }

        if (!descriptionHtml) {
            const main = $('main').first();
            if (main.length) {
                descriptionHtml = main.html();
                descriptionText = cleanText(main.text());
            }
        }

        let jobType = null;
        const typeSelectors = [
            '[class*="job-type"]',
            '[data-test="job-type"]',
            '.job-meta',
            '.job-facts',
        ];
        for (const sel of typeSelectors) {
            const text = cleanText($(sel).first().text());
            if (text && text.length < 200) {
                jobType = text;
                break;
            }
        }

        let applyUrl = null;
        const applyButton = $('a[href*="apply"], button[data-test*="apply"]').first();
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
        // Non-fatal: details are optional, listing is still saved
        log.warning('Job details fetch failed (skipping, listing is still saved)', {
            jobUrl,
            message: (err?.message || String(err)).slice(0, 300),
        });
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
        proxyConfiguration: proxyInput,
        collectDetails = false, // default OFF for stability
    } = input;

    const targetResults = toNumberOrDefault(results_wanted, 50);
    const maxPages = toNumberOrDefault(max_pages, 20);
    const startUrl = guessStartUrl({ keyword, location, startUrl: inputStartUrl });

    log.info('Starting Caterer.com scraper (HTTP + Impit + JSON/HTML parsers)', {
        keyword,
        location,
        startUrl,
        results_wanted: targetResults,
        max_pages: maxPages,
        collectDetails,
    });

    const proxyConfiguration = await Actor.createProxyConfiguration(
        proxyInput || {
            groups: ['RESIDENTIAL'],
            countryCode: 'GB',
        },
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
        useExtendedUniqueKey: true,
    });

    const seenJobIds = new Set();
    const seenUrls = new Set();
    let jobsSaved = 0;
    let pagesProcessed = 0;

    // --- IMPIT + Apify proxy HTTP client ---
    const fetchViaHttp = async (url, retries = 3) => {
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                await randomDelay(300, 900);

                let proxyUrl = null;
                if (proxyConfiguration) {
                    try {
                        proxyUrl = await proxyConfiguration.newUrl();
                    } catch (err) {
                        log.warning('Unable to get proxy URL from proxyConfiguration', {
                            message: (err?.message || String(err)).slice(0, 200),
                        });
                    }
                }

                // Fresh Impit instance per request to rotate proxy URL
                const impit = new Impit({
                    browser: 'chrome',
                    proxyUrl: proxyUrl || undefined,
                    ignoreTlsErrors: true,
                });

                const response = await impit.fetch(url, {
                    headers: buildHeaders(url),
                });

                if (!response.ok) {
                    const status = response.status;
                    if ([403, 429, 503].includes(status)) {
                        log.warning(`HTTP blocked with ${status}, attempt ${attempt}/${retries}`, {
                            url,
                        });
                        if (attempt < retries) {
                            await randomDelay(2000 * attempt, 4000 * attempt);
                            continue;
                        }
                    }
                    throw new Error(`HTTP error! status: ${status}`);
                }

                const html = await response.text();

                log.debug('HTTP fetch successful', {
                    url,
                    attempt,
                    status: response.status,
                    bodyLength: html.length,
                });

                return {
                    statusCode: response.status,
                    headers: Object.fromEntries(response.headers.entries()),
                    body: html,
                };
            } catch (err) {
                log.warning(`HTTP fetch error, attempt ${attempt}/${retries}`, {
                    url,
                    message: (err?.message || String(err)).slice(0, 200),
                });
                if (attempt < retries) {
                    await randomDelay(1500 * attempt, 3000 * attempt);
                    continue;
                }
                throw err;
            }
        }
    };

    const DEFAULT_MAX_CONCURRENCY = 3;
    const DEFAULT_MAX_RETRIES = 4;

    const crawler = new BasicCrawler({
        requestQueue,
        maxConcurrency: DEFAULT_MAX_CONCURRENCY,
        maxRequestRetries: DEFAULT_MAX_RETRIES,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 50,
            sessionOptions: {
                maxUsageCount: 50,
                maxErrorScore: 3,
            },
        },
        async requestHandler({ request, log: crawlerLog }) {
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

            try {
                const res = await fetchViaHttp(request.url);
                status = res.statusCode || null;
                contentType = res.headers['content-type'] || '';
                html = res.body?.toString?.('utf8') ?? '';
                if (status === 200 && html) {
                    crawlerLog.info('Fetched via HTTP client', { status, contentType });
                } else {
                    crawlerLog.warning('HTTP fetch blocked or empty', { status, contentType });
                }
            } catch (err) {
                crawlerLog.warning('HTTP fetch failed', {
                    url: request.url,
                    message: (err?.message || String(err)).slice(0, 200),
                });
                throw err;
            }

            const isJsonLike =
                (contentType || '').includes('application/json') || html.trim().startsWith('{');
            const collected = [];

            // 1) JSON API-style responses
            if (isJsonLike) {
                collected.push(...tryParseJsonApi(html, request.url));
            }

            // 2) Inline JSON state (__NEXT_DATA__ etc.)
            collected.push(...extractFromInlineState(html, request.url));

            // 3) JSON-LD structured data
            collected.push(...extractJsonLdJobs(html, request.url));

            // 4) HTML DOM fallback
            const $ = cheerio.load(html);
            collected.push(...extractJobsFromHtml($, request.url));

            const deduped = [];
            for (const job of collected) {
                const uniqueKey = job.jobId || job.url;
                if (!uniqueKey) continue;
                if (seenJobIds.has(uniqueKey) || seenUrls.has(job.url)) continue;

                if (!job.title || !job.url) {
                    crawlerLog.debug('Skipping job with missing required fields', { job });
                    continue;
                }

                seenJobIds.add(uniqueKey);
                seenUrls.add(job.url);
                deduped.push(job);
            }

            if (deduped.length) {
                const toSave = [];

                for (const job of deduped) {
                    if (jobsSaved >= targetResults) break;

                    const finalJob = { ...job };

                    if (collectDetails) {
                        try {
                            const details = await fetchJobDetails(job.url, fetchViaHttp);
                            if (details) {
                                finalJob.descriptionHtml =
                                    details.descriptionHtml || finalJob.description;
                                finalJob.description =
                                    details.descriptionText || finalJob.description;
                                finalJob.jobType = details.jobType || finalJob.jobType;
                                finalJob.applyUrl = details.applyUrl || finalJob.applyUrl;
                                finalJob.detailsFetched = true;
                            }
                        } catch (err) {
                            // Should not really throw due to catch inside fetchJobDetails,
                            // but we keep this as a safeguard.
                            crawlerLog.warning('Failed to fetch job details (outer)', {
                                url: job.url,
                                message: (err?.message || String(err)).slice(0, 200),
                            });
                        }
                    }

                    toSave.push(finalJob);
                    jobsSaved += 1;
                }

                if (toSave.length) {
                    await Dataset.pushData(toSave);
                    crawlerLog.info('Saved jobs from page', {
                        url: request.url,
                        pageNum,
                        jobsOnPage: deduped.length,
                        jobsSavedSoFar: jobsSaved,
                        targetResults,
                        sourceMix: Array.from(new Set(toSave.map((j) => j.source))),
                        withDetails: collectDetails,
                    });
                } else {
                    crawlerLog.warning('No jobs extracted on page after filtering', {
                        url: request.url,
                        pageNum,
                    });
                }
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

            if (deduped.length === 0 && pageNum === 1) {
                crawlerLog.error(
                    'No jobs found on first page, possible layout change or blocking',
                    {
                        url: request.url,
                        status,
                        contentType,
                        htmlSnippet: html.slice(0, 500),
                    },
                );
            }
            if (deduped.length === 0 && pageNum > 1) {
                crawlerLog.warning('No jobs found on page, assuming end of results', {
                    pageNum,
                });
                return;
            }

            await randomDelay(1000, 2500);

            const nextPageNum = pageNum + 1;
            const nextUrl = buildNextPageUrl(request.url, nextPageNum);

            await requestQueue.addRequest({
                url: nextUrl,
                userData: {
                    label: 'LIST',
                    pageNum: nextPageNum,
                },
                headers: buildHeaders(request.url),
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
