import { Actor, log } from 'apify';
import { CheerioCrawler } from 'crawlee';
import { chromium } from 'playwright';

// ---------- Helpers ----------

/**
 * Normalize whitespace and strip weird CSS fragments (from inline styles leaking into text).
 */
const cleanText = (text) => {
    if (!text) return '';
    let out = text
        .replace(/\s+/g, ' ')              // collapse whitespace
        .replace(/\u00A0/g, ' ')           // non-breaking space
        .trim();

    // Strip common ".res-xxxxx{...}" / ".job-ad-display-xxx{...}" CSS blobs that sometimes leak into text.
    out = out.replace(/\.res-[^{]+{[^}]*}/g, '')
             .replace(/\.job-[^{]+{[^}]*}/g, '')
             .replace(/#no-js-image-[^:]+: [^}]+}/g, '')
             .replace(/\s+/g, ' ')
             .trim();

    return out;
};

const parsePostedDate = (text) => {
    if (!text) return null;
    const trimmed = cleanText(text.toLowerCase());
    const now = new Date();

    const justDate = (d) => d.toISOString();

    if (trimmed.includes('today')) return justDate(now);
    if (trimmed.includes('yesterday')) {
        const d = new Date(now);
        d.setDate(d.getDate() - 1);
        return justDate(d);
    }
    const m = trimmed.match(/(\d+)\s+(minute|hour|day|week|month|year)s?\s+ago/);
    if (m) {
        const value = Number(m[1]);
        const unit = m[2];
        const d = new Date(now);
        const mult = (unit === 'minute' ? 1 :
                      unit === 'hour' ? 60 :
                      unit === 'day' ? 60 * 24 :
                      unit === 'week' ? 60 * 24 * 7 :
                      unit === 'month' ? 60 * 24 * 30 :
                      60 * 24 * 365);
        d.setMinutes(d.getMinutes() - value * mult);
        return justDate(d);
    }
    return null;
};

const buildSearchUrl = (keyword, location, pageNum = 1) => {
    const base = 'https://www.caterer.com/jobs';
    const parts = [];

    if (keyword && keyword.trim()) {
        // Keyword portion
        const kw = keyword.trim().toLowerCase().replace(/\s+/g, '-');
        parts.push(encodeURIComponent(kw));
    }

    if (location && location.trim()) {
        // Location can be appended after keyword with a hyphen, Caterer is quite forgiving in URLs.
        const loc = location.trim().toLowerCase().replace(/\s+/g, '-');
        if (parts.length) {
            parts[parts.length - 1] += `-${encodeURIComponent(loc)}`;
        } else {
            parts.push(encodeURIComponent(loc));
        }
    }

    const path = parts.length ? `/${parts.join('/')}` : '';
    const url = new URL(base + path);
    if (keyword && !url.searchParams.has('keywords')) url.searchParams.set('keywords', keyword);
    if (location && !url.searchParams.has('location')) url.searchParams.set('location', location);
    if (pageNum > 1) url.searchParams.set('page', String(pageNum));

    return url.toString();
};

/**
 * Extract jobs from listing page DOM using Caterer.com structure.
 * We rely on `data-at` attributes for clean fields.
 * This only uses listing pages (no slow detail pages).
 */
const extractJobsFromListing = ($, pageUrl) => {
    const jobs = [];

    // Primary selector (this worked in your Playwright version).
    const cards = $('[data-at="job-item"]');

    // Fallback for when the data-at structure changes:
    if (cards.length === 0) {
        const headings = $('h2, h3').filter((_, el) => {
            const txt = cleanText($(el).text());
            return txt && !/filters|related jobs|locations|salaries/i.test(txt);
        });

        headings.each((_, el) => {
            const h = $(el);
            const titleLink = h.find('a[href*="/job/"]').first();
            const href = titleLink.attr('href') || '';
            if (!href) return;

            const titleRaw = cleanText(titleLink.text() || h.text());
            const url = new URL(href, pageUrl).toString();

            const company = cleanText(h.nextAll().eq(0).text());
            const location = cleanText(h.nextAll().eq(1).text());
            const salary = cleanText(h.nextAll().eq(2).text());
            const snippet = cleanText(h.nextAll().eq(3).text());
            const postedText = cleanText(
                h.nextAll('span, time, p')
                    .filter((_, el2) => /posted|minute|hour|day|week|month|year|recently/i.test($(el2).text()))
                    .first()
                    .text()
            );

            jobs.push({
                title: titleRaw || null,
                url,
                company: company || null,
                location: location || null,
                salary: salary || null,
                description_snippet: snippet || null,
                posted_text: postedText || null,
                posted_at: parsePostedDate(postedText),
                source_list_url: pageUrl,
            });
        });

        return jobs;
    }

    // Normal path with `data-at` attributes
    cards.each((_, el) => {
        const card = $(el);

        const titleEl = card.find('[data-at="job-title"], a[href*="/job/"]').first();
        const href = titleEl.attr('href') || '';
        if (!href) return;

        let titleRaw = cleanText(titleEl.text());
        if (!titleRaw) {
            titleRaw = cleanText(card.find('h2, h3').first().text());
        }

        const url = new URL(href, pageUrl).toString();

        const company = cleanText(
            card.find('[data-at="job-company-name"]').first().text()
            || card.find('a[href*="/jobs/"]').first().text()
        ) || null;

        const location = cleanText(
            card.find('[data-at="job-location"]').first().text()
            || card.find('span:contains("London"), span:contains("UK")').first().text()
        ) || null;

        let salary = cleanText(
            card.find('[data-at="job-salary"]').first().text()
        );

        // If salary accidentally contains other fields, try to trim to the currency portion.
        if (salary && !/[£€$]/.test(salary) && /(per\s+(hour|annum|year|day))/i.test(card.text())) {
            const txt = cleanText(card.text());
            const match = txt.match(/([£€$].+?(?:per\s+(?:hour|annum|year|day)|pa|hourly))/i);
            if (match) salary = cleanText(match[1]);
        }

        const snippet = cleanText(
            card.find('[data-at="job-description"], [data-at="job-snippet"]').first().text()
            || card.find('p').first().text()
        ) || null;

        const postedText = cleanText(
            card.find('[data-at="job-posted-date"], time').first().text()
        ) || null;

        jobs.push({
            title: titleRaw || null,
            url,
            company,
            location,
            salary: salary || null,
            description_snippet: snippet,
            posted_text: postedText,
            posted_at: parsePostedDate(postedText),
            source_list_url: pageUrl,
        });
    });

    return jobs;
};

// ---------- Main ----------

await Actor.main(async () => {
    const input = (await Actor.getInput()) || {};

    const {
        keyword = '',
        location = '',
        startUrl = '',
        results_wanted = 50,
        max_pages = 20,
        collectDetails = true,          // Kept for compatibility, but we only use listing description for speed.
        useBrowserHandshake = true,
        maxConcurrency = 10,            // HTTP-only, we can go fairly high.
    } = input;

    const target = Number(results_wanted) > 0 ? Number(results_wanted) : 50;

    // Decide first listing URL
    const initialUrl = startUrl && startUrl.trim()
        ? startUrl.trim()
        : buildSearchUrl(keyword, location, 1);

    log.info('Starting Caterer.com hybrid HTTP scraper (Playwright handshake + CheerioCrawler)', {
        keyword,
        location,
        startUrl: initialUrl,
        results_wanted: target,
        max_pages,
        collectDetails,
        maxConcurrency,
        useBrowserHandshake,
    });

    // Proxy configuration (reuses your existing Apify proxy settings)
    const proxyConfiguration = await Actor.createProxyConfiguration().catch((err) => {
        log.warning('Proxy not configured, continuing without Apify proxy.', { error: err?.message });
        return undefined;
    });

    // ---------- Optional Playwright handshake (1 page only) ----------
    if (useBrowserHandshake) {
        try {
            const proxyUrl = proxyConfiguration ? await proxyConfiguration.newUrl() : null;

            const browser = await chromium.launch({
                headless: true,
                args: [
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                ],
                proxy: proxyUrl ? { server: proxyUrl } : undefined,
            });

            const context = await browser.newContext({
                viewport: { width: 1366, height: 768 },
                userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36',
                locale: 'en-GB',
            });

            const page = await context.newPage();
            log.info('Playwright handshake: navigating to start URL via full browser', { url: initialUrl });

            await page.goto(initialUrl, {
                waitUntil: 'domcontentloaded',
                timeout: 20000,
            });

            // Small wait for potential anti-bot JS to run, then close. We do not keep cookies; HTTP scraper will be used afterwards.
            await page.waitForTimeout(2000);

            await browser.close();
            log.info('Playwright handshake completed, continuing with fast HTTP-only scraper.');
        } catch (err) {
            log.warning('Playwright handshake failed, continuing with HTTP-only scraper.', { error: err?.message || String(err) });
        }
    }

    // ---------- Fast HTTP scraping with CheerioCrawler ----------
    const stats = {
        pagesVisited: 0,
        pagesQueued: 0,
        jobsFromPages: 0,
        jobsSaved: 0,
    };

    const seenUrls = new Set();

    const crawler = new CheerioCrawler({
        proxyConfiguration,
        // CheerioCrawler internally uses got-scraping + browser-like headers and per-session fingerprints.
        useSessionPool: true,
        persistCookiesPerSession: true,
        maxConcurrency,
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 45,

        async requestHandler({ request, $, response, session, proxyInfo }) {
            const { pageNum = 1 } = request.userData;
            stats.pagesVisited += 1;

            const url = request.loadedUrl || request.url;
            log.info('Processing listing page', { url, pageNum });

            const jobs = extractJobsFromListing($, url);
            stats.jobsFromPages += jobs.length;

            const remaining = target - stats.jobsSaved;
            const slice = remaining < jobs.length ? jobs.slice(0, remaining) : jobs;

            for (const job of slice) {
                if (seenUrls.has(job.url)) continue;
                seenUrls.add(job.url);

                await Actor.pushData(job);
                stats.jobsSaved += 1;

                if (stats.jobsSaved >= target) {
                    log.info('Target job count reached, stopping crawler.', { target });
                    // Abort the crawler gracefully – no more pages fetched.
                    await crawler.autoscaledPool?.abort();
                    break;
                }
            }

            // Pagination: only enqueue next page if we still need more jobs
            if (stats.jobsSaved < target && pageNum < max_pages) {
                const nextPage = pageNum + 1;
                const nextUrl = startUrl && startUrl.trim()
                    ? (() => {
                        const urlObj = new URL(startUrl.trim());
                        urlObj.searchParams.set('page', String(nextPage));
                        return urlObj.toString();
                    })()
                    : buildSearchUrl(keyword, location, nextPage);

                stats.pagesQueued += 1;
                await crawler.addRequests([
                    {
                        url: nextUrl,
                        userData: { pageNum: nextPage },
                    },
                ]);

                log.info('Queued next listing page', { nextPage, nextUrl });
            }
        },

        async failedRequestHandler({ request, error, session, proxyInfo }) {
            log.warning('Listing request failed', {
                url: request.url,
                error: error?.message || String(error),
                retries: request.retryCount,
            });
        },
    });

    await crawler.run([
        { url: initialUrl, userData: { pageNum: 1 } },
    ]);

    log.info('Scraping finished', {
        jobsSaved: stats.jobsSaved,
        target,
        stats,
    });
});
