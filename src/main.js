// Caterer.com jobs scraper - CheerioCrawler implementation
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

// Single-entrypoint main
await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            keyword = '', location = '', category = '', results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 999, collectDetails = true, startUrl, startUrls, url, proxyConfiguration,
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 999;

        const toAbs = (href, base = 'https://www.caterer.com') => {
            try { return new URL(href, base).href; } catch { return null; }
        };

        const cleanText = (html) => {
            if (!html) return '';
            const $ = cheerioLoad(html);
            $('script, style, noscript, iframe').remove();
            return $.root().text().replace(/\s+/g, ' ').trim();
        };

        const buildStartUrl = (kw, loc, cat) => {
            const u = new URL('https://www.caterer.com/jobs');
            if (kw) u.searchParams.set('keywords', String(kw).trim());
            if (loc) u.searchParams.set('location', String(loc).trim());
            if (cat) u.searchParams.set('category', String(cat).trim());
            return u.href;
        };

        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) initial.push(...startUrls);
        if (startUrl) initial.push(startUrl);
        if (url) initial.push(url);
        if (!initial.length) initial.push(buildStartUrl(keyword, location, category));

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;

        let saved = 0;

        function extractFromJsonLd($) {
            const scripts = $('script[type="application/ld+json"]');
            for (let i = 0; i < scripts.length; i++) {
                try {
                    const parsed = JSON.parse($(scripts[i]).html() || '');
                    const arr = Array.isArray(parsed) ? parsed : [parsed];
                    for (const e of arr) {
                        if (!e) continue;
                        const t = e['@type'] || e.type;
                        if (t === 'JobPosting' || (Array.isArray(t) && t.includes('JobPosting'))) {
                            return {
                                title: e.title || e.name || null,
                                company: e.hiringOrganization?.name || null,
                                date_posted: e.datePosted || null,
                                description_html: e.description || null,
                                location: (e.jobLocation && e.jobLocation.address && (e.jobLocation.address.addressLocality || e.jobLocation.address.addressRegion)) || null,
                            };
                        }
                    }
                } catch (e) { /* ignore parsing errors */ }
            }
            return null;
        }

        function findJobLinks($, base) {
            const links = new Set();
            // Caterer.com job links - adjust selectors based on actual site structure
            $('.job-card a[href], .job-listing a[href], article.job a[href], .job-item a[href]').each((_, a) => {
                const href = $(a).attr('href');
                if (!href) return;
                const abs = toAbs(href, base);
                if (abs && /caterer\.com\/jobs\//i.test(abs)) links.add(abs);
            });
            // Fallback: look for any job-related links
            if (links.size === 0) {
                $('a[href]').each((_, a) => {
                    const href = $(a).attr('href');
                    if (!href) return;
                    if (/\/jobs\/|job-\d+|vacancy/i.test(href)) {
                        const abs = toAbs(href, base);
                        if (abs) links.add(abs);
                    }
                });
            }
            return [...links];
        }

        function findNextPage($, base) {
            // Try standard pagination patterns
            const rel = $('a[rel="next"]').attr('href');
            if (rel) return toAbs(rel, base);
            
            // Look for pagination with "Next" text
            const next = $('a.pagination__next, a.next, .pagination a').filter((_, el) => {
                const text = $(el).text().trim();
                return /(^|\s)(next|›|»|>)(\s|$)/i.test(text);
            }).first().attr('href');
            if (next) return toAbs(next, base);
            
            // Look for numbered pagination - find current page and get next
            const currentPage = $('.pagination .active, .pagination .current').text().trim();
            if (currentPage) {
                const nextPageNum = parseInt(currentPage) + 1;
                const nextLink = $(`.pagination a`).filter((_, el) => $(el).text().trim() === String(nextPageNum)).first().attr('href');
                if (nextLink) return toAbs(nextLink, base);
            }
            
            return null;
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 3,
            useSessionPool: true,
            maxConcurrency: 10,
            requestHandlerTimeoutSecs: 60,
            async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                if (label === 'LIST') {
                    const links = findJobLinks($, request.url);
                    crawlerLog.info(`LIST ${request.url} -> found ${links.length} links`);

                    if (collectDetails) {
                        const remaining = RESULTS_WANTED - saved;
                        const toEnqueue = links.slice(0, Math.max(0, remaining));
                        if (toEnqueue.length) await enqueueLinks({ urls: toEnqueue, userData: { label: 'DETAIL' } });
                    } else {
                        const remaining = RESULTS_WANTED - saved;
                        const toPush = links.slice(0, Math.max(0, remaining));
                        if (toPush.length) { await Dataset.pushData(toPush.map(u => ({ url: u, _source: 'caterer.com' }))); saved += toPush.length; }
                    }

                    if (saved < RESULTS_WANTED && pageNo < MAX_PAGES) {
                        const next = findNextPage($, request.url);
                        if (next) await enqueueLinks({ urls: [next], userData: { label: 'LIST', pageNo: pageNo + 1 } });
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    if (saved >= RESULTS_WANTED) return;
                    try {
                        const json = extractFromJsonLd($);
                        const data = json || {};
                        
                        // Enhanced selectors for Caterer.com job pages
                        if (!data.title) {
                            data.title = $('h1.job-title, h1[class*="job"], h1').first().text().trim() || null;
                        }
                        
                        if (!data.company) {
                            data.company = $('.company-name, [class*="company"], .employer, .recruiter-name').first().text().trim() || null;
                        }
                        
                        if (!data.description_html) {
                            const desc = $('.job-description, [class*="job-description"], .description, .job-details, .entry-content, #job-description').first();
                            data.description_html = desc && desc.length ? String(desc.html()).trim() : null;
                        }
                        data.description_text = data.description_html ? cleanText(data.description_html) : null;
                        
                        if (!data.location) {
                            data.location = $('.job-location, [class*="location"], .location, [class*="address"]').first().text().trim() || null;
                        }
                        
                        // Extract salary if available
                        const salary = $('.salary, [class*="salary"], .wage, [class*="wage"]').first().text().trim() || null;
                        
                        // Extract job type if available
                        const jobType = $('.job-type, [class*="job-type"], .employment-type').first().text().trim() || null;

                        const item = {
                            title: data.title || null,
                            company: data.company || null,
                            category: category || null,
                            location: data.location || null,
                            salary: salary || null,
                            job_type: jobType || null,
                            date_posted: data.date_posted || null,
                            description_html: data.description_html || null,
                            description_text: data.description_text || null,
                            url: request.url,
                        };

                        await Dataset.pushData(item);
                        saved++;
                    } catch (err) { crawlerLog.error(`DETAIL ${request.url} failed: ${err.message}`); }
                }
            }
        });

        await crawler.run(initial.map(u => ({ url: u, userData: { label: 'LIST', pageNo: 1 } })));
        log.info(`Finished. Saved ${saved} items`);
    } finally {
        await Actor.exit();
    }
}

main().catch(err => { console.error(err); process.exit(1); });
