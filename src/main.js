import { Actor, Dataset, log } from 'apify';
import { PlaywrightCrawler, RequestQueue } from 'crawlee';
import { gotScraping } from 'got-scraping';
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

const buildHeaders = (refererUrl) => ({
    accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7',
    'accept-language': 'en-GB,en;q=0.9',
    'user-agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
    referer: refererUrl,
    'upgrade-insecure-requests': '1',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'sec-ch-ua': '"Chromium";v="120", "Not.A/Brand";v="24", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
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

    $('h2 a[href*="/job/"]').each((_, anchor) => {
        const $a = $(anchor);
        const url = $a.attr('href');
        const title = cleanText($a.text());
        if (!url || !title) return;

        const card =
            $a.closest('article')?.first() ||
            $a.closest('li')?.first() ||
            $a.closest('[class*="job"]')?.first() ||
            $a.parent();

        const linesRaw = card
            ? card.text().split('\n').map((l) => l.trim()).filter(Boolean)
            : [];

        const lines = [];
        for (const line of linesRaw) {
            if (!lines.length || lines[lines.length - 1] !== line) {
                lines.push(line);
            }
        }

        const titleIdx = lines.findIndex((l) => l === title);
        let company = null;
        let location = null;
        let salary = null;
        let postedAt = null;

        const isMoneyLine = (line) =>
            /[\u00A3$]|\bper annum\b|\bper hour\b|\bper year\b|\bUp To\b/i.test(line);
        const isPostedLine = (line) => /\b(ago|Today|Yesterday)\b/i.test(line);

        for (let i = titleIdx + 1; i < lines.length; i++) {
            const line = lines[i];
            if (isMoneyLine(line) || isPostedLine(line)) continue;
            company = line;
            break;
        }

        const companyIdx = company ? lines.indexOf(company, titleIdx + 1) : -1;
        for (let i = companyIdx + 1; i < lines.length; i++) {
            const line = lines[i];
            if (isMoneyLine(line) || isPostedLine(line)) continue;
            if (
                /[A-Z]{1,2}\d[\dA-Z]? ?\d[A-Z]{2}/.test(line) ||
                line.includes(',') ||
                /\b(UK|London|Manchester|Birmingham|Scotland|Wales|England)\b/i.test(line)
            ) {
                location = line;
                break;
            }
        }

        const locIdx = location ? lines.indexOf(location, (companyIdx + 1) || 0) : companyIdx;
        for (let i = locIdx + 1; i < lines.length; i++) {
            const line = lines[i];
            if (isMoneyLine(line)) {
                salary = line;
                break;
            }
        }

        for (let i = lines.length - 1; i >= 0; i--) {
            if (isPostedLine(lines[i])) {
                postedAt = lines[i];
                break;
            }
        }

        const descLines = [];
        const salaryIdx = salary ? lines.indexOf(salary) : -1;
        const stopTokens = ['more', 'NEW', 'FEATURED', 'NEWFEATURED'];

        if (salaryIdx !== -1) {
            for (let i = salaryIdx + 1; i < lines.length; i++) {
                const line = lines[i];
                if (stopTokens.includes(line.toUpperCase()) || isPostedLine(line)) break;
                if (!isMoneyLine(line)) descLines.push(line);
            }
        }

        const description = descLines.length ? descLines.join(' ') : null;
        const jobId = getJobIdFromUrl(url);

        jobs.push({
            title,
            company: cleanText(company),
            location: cleanText(location),
            salary: cleanText(salary),
            description: cleanText(description),
            postedAt: cleanText(postedAt),
            url,
            jobId: cleanText(jobId),
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

    const proxyConfiguration = await Actor.createProxyConfiguration(proxyInput || {});
    log.info('Proxy configured', { usingCustom: !!proxyInput });

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

    const fetchViaHttp = async (url) => {
        const headers = buildHeaders(url);
        const opts = {
            url,
            headers,
            timeout: { request: 25000 },
            http2: false,
            throwHttpErrors: false,
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

        const res = await gotScraping(opts);
        return res;
    };

    const crawler = new PlaywrightCrawler({
        requestQueue,
        proxyConfiguration,
        maxConcurrency,
        headless: true,
        navigationTimeoutSecs: 30,
        maxRequestRetries: 4,
        useSessionPool: true,
        persistCookiesPerSession: true,
        launchContext: {
            launchOptions: {
                args: [
                    '--disable-dev-shm-usage',
                    '--disable-http2', // force HTTP/1.1 to avoid proxy/http2 protocol errors
                    '--disable-features=NetworkService,NetworkServiceInProcess',
                ],
                ignoreHTTPSErrors: true,
            },
        },
        preNavigationHooks: [
            async ({ page, request, session }, gotoOptions) => {
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    if (['image', 'media', 'font', 'stylesheet'].includes(type)) return route.abort();
                    return route.continue();
                });
                await page.setExtraHTTPHeaders(buildHeaders(request.url));
                gotoOptions.waitUntil = 'domcontentloaded';
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
                seenJobIds.add(uniqueKey);
                seenUrls.add(job.url);
                deduped.push(job);
            }

            const remaining = targetResults - jobsSaved;
            const toSave = deduped.slice(0, remaining);

            if (toSave.length) {
                await Dataset.pushData(
                    toSave.map((job) => ({
                        ...job,
                    searchKeyword: keyword || null,
                    searchLocation: location || null,
                    pageNum,
                    transport: usedPlaywright ? 'playwright' : 'http',
                })),
            );
                jobsSaved += toSave.length;
                crawlerLog.info('Saved jobs from page', {
                    pageNum,
                    saved: toSave.length,
                    jobsSaved,
                    targetResults,
                    sourceMix: Array.from(new Set(toSave.map((j) => j.source))),
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
