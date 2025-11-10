// Caterer.com jobs scraper - Production-ready implementation
// Stack: Apify + Crawlee + CheerioCrawler + gotScraping + header-generator
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import HeaderGenerator from 'header-generator';

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
        
        log.info('Starting Caterer.com Job Scraper', { keyword, location, category, results_wanted: RESULTS_WANTED, max_pages: MAX_PAGES });

        // Dynamic header generation for anti-bot evasion
        const getHeaders = () => {
            const headerGenerator = new HeaderGenerator({
                browsers: ['chrome', 'firefox'],
                operatingSystems: ['windows', 'macos', 'linux'],
                devices: ['desktop'],
            });
            return headerGenerator.getHeaders();
        };

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
            // Caterer.com uses /jobs/search for search results
            const u = new URL('https://www.caterer.com/jobs/search');
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
            // Caterer.com job URLs follow pattern: /job/{title-slug}/{company-slug}-job{id}
            // Example: https://www.caterer.com/job/bar-staff/search-job106117278
            $('a[href]').each((_, a) => {
                const href = $(a).attr('href');
                if (!href) return;
                
                // Match actual job posting URLs, not location/category links
                // Job URLs: /job/{slug}/{company}-job{number}
                // Exclude: /jobs/search/in-{location}, /jobs/{category}, /jobs?{params}
                if (/^\/job\/[^\/]+\/[^\/]+-job\d+$|^\/job\/[a-z0-9\-]+\/[a-z0-9\-]+-job\d+$/i.test(href)) {
                    const abs = toAbs(href, base);
                    if (abs && !abs.includes('#')) {
                        links.add(abs);
                    }
                }
            });
            return [...links];
        }

        function findNextPage($, base) {
            // Caterer.com uses ?page=N pagination (seen in fetched HTML: ?page=2, ?page=3, etc.)
            const nextLink = $('a[href*="page="]').filter((_, el) => {
                const text = $(el).text().trim().toLowerCase();
                return text === 'next' || text.includes('next');
            }).first().attr('href');
            
            if (nextLink) return toAbs(nextLink, base);
            
            // Fallback: try to find current page and calculate next
            const currentUrl = new URL(base);
            const currentPage = parseInt(currentUrl.searchParams.get('page') || '1');
            
            // Check if a link to next page number exists
            const nextPageNum = currentPage + 1;
            const nextPageLink = $(`a[href*="page=${nextPageNum}"]`).first().attr('href');
            if (nextPageLink) return toAbs(nextPageLink, base);
            
            return null;
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 5,
            useSessionPool: true,
            maxConcurrency: 3,
            requestHandlerTimeoutSecs: 120,
            navigationTimeoutSecs: 90,
            
            // Stealth headers and request options
            prepareRequestFunction: async ({ request }) => {
                // Generate realistic browser headers for each request
                const headers = getHeaders();
                request.headers = {
                    ...headers,
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Referer': 'https://www.caterer.com/',
                    'Origin': 'https://www.caterer.com',
                };
                
                // Add random delays to appear human-like
                await new Promise(resolve => setTimeout(resolve, Math.random() * 2000 + 500));
                return request;
            },

            async failedRequestHandler({ request, error }, context) {
                log.error(`Request failed after ${request.retryCount} retries: ${request.url}`, { 
                    error: error.message,
                    statusCode: error.statusCode 
                });
            },
            async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                if (label === 'LIST') {
                    crawlerLog.info(`Processing LIST page ${pageNo}: ${request.url}`);
                    
                    // Extract jobs directly from listing page
                    const jobs = [];
                    
                    // Find all job links in h2 tags - more reliable selector
                    const jobElements = $('h2 a[href*="/job/"]');
                    crawlerLog.info(`Found ${jobElements.length} potential job links`);
                    
                    jobElements.each((idx, el) => {
                        try {
                            const $link = $(el);
                            const href = $link.attr('href');
                            
                            // Validate it's a proper job URL
                            if (!href || !/\/job\/[^\/]+\/[^\/]+-job\d+/i.test(href)) {
                                return;
                            }
                            
                            const jobUrl = toAbs(href, request.url);
                            const title = $link.text().trim();
                            
                            // Try to find the parent container for this job to extract other details
                            const $jobContainer = $link.closest('article, section, .vacancy, [class*="job-item"], div[role="article"]').first();
                            
                            // Extract company - usually appears after the title in the listing
                            let company = null;
                            const companyLink = $jobContainer.find('a[href*="/jobs/"]').not($link).first();
                            if (companyLink.length) {
                                company = companyLink.text().trim();
                            } else {
                                // Fallback: look for company text patterns
                                const containerText = $jobContainer.text();
                                const companyMatch = containerText.match(/(?:by|company|employer):\s*([^\n]+)/i);
                                if (companyMatch) company = companyMatch[1].trim();
                            }
                            
                            // Extract location - typically in the job container
                            const locationText = $jobContainer.text();
                            let location = null;
                            
                            // UK postcode pattern or city patterns
                            const locationMatch = locationText.match(/([A-Z]{1,2}\d{1,2}\s?[A-Z]{2}|[A-Z][a-z]+(?:\s[A-Z][a-z]+)*(?:,\s*[A-Z]{2,})?)/);
                            if (locationMatch) {
                                location = locationMatch[0].trim();
                            }
                            
                            // Extract salary - look for £ signs with better pattern
                            let salary = null;
                            const salaryMatch = locationText.match(
                                /(?:£[\d,]+(?:\.\d{2})?(?:\s*-\s*£[\d,]+(?:\.\d{2})?)?|Up to £[\d,]+(?:\.\d{2})?)\s*per\s*(?:hour|annum|day|year|week)/i
                            );
                            if (salaryMatch) {
                                salary = salaryMatch[0].trim();
                            }
                            
                            // Extract date posted with improved pattern
                            let datePosted = null;
                            const dateMatch = locationText.match(
                                /(?:posted\s)?(?:(\d+)\s*(?:hours?|days?|weeks?|months?)\s*ago|NEW|FEATURED|\d+\s*ago)/i
                            );
                            if (dateMatch) {
                                datePosted = dateMatch[0].trim();
                            }
                            
                            if (title && jobUrl && title.length > 2) {
                                jobs.push({
                                    title,
                                    company,
                                    category: category || null,
                                    location,
                                    salary,
                                    job_type: null,
                                    date_posted: datePosted,
                                    description_html: null,
                                    description_text: null,
                                    url: jobUrl,
                                });
                            }
                        } catch (err) {
                            crawlerLog.warning(`Error parsing job element: ${err.message}`);
                        }
                    });
                    
                    crawlerLog.info(`Extracted ${jobs.length} valid jobs from ${request.url}`);
                    
                    if (jobs.length > 0) {
                        const remaining = RESULTS_WANTED - saved;
                        const toPush = jobs.slice(0, Math.max(0, remaining));
                        if (toPush.length > 0) {
                            await Dataset.pushData(toPush);
                            saved += toPush.length;
                            crawlerLog.info(`✓ Saved ${toPush.length} jobs. Total: ${saved}/${RESULTS_WANTED}`);
                        }
                    }

                    // Optionally enqueue detail pages if requested
                    if (collectDetails && saved < RESULTS_WANTED) {
                        const detailUrls = findJobLinks($, request.url);
                        const remaining = RESULTS_WANTED - saved;
                        const toEnqueue = detailUrls.slice(0, Math.max(0, remaining));
                        if (toEnqueue.length > 0) {
                            crawlerLog.info(`Enqueueing ${toEnqueue.length} detail pages`);
                            await enqueueLinks({ 
                                urls: toEnqueue, 
                                userData: { label: 'DETAIL' },
                                strategy: 'NEW' 
                            });
                        }
                    }

                    // Handle pagination
                    if (saved < RESULTS_WANTED && pageNo < MAX_PAGES) {
                        const next = findNextPage($, request.url);
                        if (next) {
                            crawlerLog.info(`Enqueueing next page (${pageNo + 1}): ${next}`);
                            await enqueueLinks({ 
                                urls: [next], 
                                userData: { label: 'LIST', pageNo: pageNo + 1 },
                                strategy: 'NEW' 
                            });
                        } else {
                            crawlerLog.info('No next page found - pagination complete');
                        }
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    if (saved >= RESULTS_WANTED) {
                        crawlerLog.info('Results limit reached, skipping detail page');
                        return;
                    }
                    
                    try {
                        crawlerLog.info(`Processing DETAIL page: ${request.url}`);
                        
                        // Try JSON-LD first
                        const json = extractFromJsonLd($);
                        const data = json || {};
                        
                        // Enhanced selectors for detail pages
                        if (!data.title) {
                            data.title = $('h1, [class*="job-title"], .job-heading').first().text().trim() || null;
                        }
                        
                        if (!data.company) {
                            data.company = $('[class*="recruiter"], [class*="employer"], .company, .job-company').first().text().trim() || null;
                        }
                        
                        if (!data.description_html) {
                            const descSelectors = ['.job-description', '[class*="description"]', '.job-details', 'article', '.content'];
                            for (const sel of descSelectors) {
                                const desc = $(sel).first();
                                if (desc.length && desc.text().length > 50) {
                                    data.description_html = desc.html();
                                    break;
                                }
                            }
                        }
                        
                        if (data.description_html) {
                            data.description_text = cleanText(data.description_html);
                        }
                        
                        if (!data.location) {
                            data.location = $('[class*="location"]').first().text().trim() || null;
                        }
                        
                        const salary = $('[class*="salary"], .wage').first().text().trim() || null;
                        const jobType = $('[class*="job-type"], .employment-type').first().text().trim() || null;

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

                        if (item.title) {
                            await Dataset.pushData(item);
                            saved++;
                            crawlerLog.info(`✓ Saved detail. Total: ${saved}/${RESULTS_WANTED}`);
                        }
                    } catch (err) {
                        crawlerLog.error(`DETAIL extraction failed: ${err.message}`);
                    }
                }
            }
        });

        log.info(`Starting crawler with ${initial.length} initial URL(s):`, initial);
        await crawler.run(initial.map(u => ({ url: u, userData: { label: 'LIST', pageNo: 1 } })));
        log.info(`✓ Finished successfully. Saved ${saved} job listings`);
        
        if (saved === 0) {
            log.warning('No jobs were extracted. This might indicate selectors need updating or the site structure has changed.');
        }
    } catch (error) {
        log.error('Fatal error in main():', error);
        throw error;
    } finally {
        await Actor.exit();
    }
}

main().catch(err => { console.error(err); process.exit(1); });
