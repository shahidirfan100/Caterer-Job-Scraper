// Caterer Job Scraper - Robust production implementation using Crawlee CheerioCrawler
// Simplified approach: Use Crawlee's battle-tested infrastructure for maximum reliability
import { Actor } from 'apify';
import { CheerioCrawler, ProxyConfiguration, Dataset } from 'crawlee';
import { HeaderGenerator } from 'header-generator';

await Actor.init();

try {
    const input = await Actor.getInput() ?? {};
    const {
        keyword = '',
        location = '',
        results_wanted = 50,
        max_pages = 10,
        collectDetails = false,
    } = input;

    Actor.log.info('Starting Caterer.com job scraper', {
        keyword,
        location,
        results_wanted,
        max_pages,
        collectDetails,
    });

    // Code version log to ensure correct build is running
    Actor.log.info('Actor code version', { version: 'main_v2', date: new Date().toISOString() });

    // Use Apify's proxy configuration - with fallback
    let proxyConfiguration;
    try {
        proxyConfiguration = await Actor.createProxyConfiguration({
            groups: ['RESIDENTIAL'],
            countryCode: 'GB',
        });
        Actor.log.info('✓ Proxy configured');
    } catch (proxyError) {
        Actor.log.warning('Proxy setup failed, will try without proxy', { error: proxyError.message });
        proxyConfiguration = undefined;
    }

    let savedCount = 0;
    const savedUrls = new Set();
    const stats = {
        listPagesProcessed: 0,
        detailPagesProcessed: 0,
        jobsExtracted: 0,
        jobsSaved: 0,
    };

    // Build search URL
    const buildSearchUrl = (kw, loc, page = 1) => {
        const url = new URL('https://www.caterer.com/jobs/search');
        if (kw) url.searchParams.set('keywords', kw);
        if (loc) url.searchParams.set('location', loc);
        if (page > 1) url.searchParams.set('page', String(page));
        return url.href;
    };

    // Parse posted date
    const parsePostedDate = (text) => {
        if (!text) return null;
        const lower = text.toLowerCase();
        
        if (lower.includes('today') || lower.includes('just now')) {
            return new Date().toISOString();
        }
        
        const match = lower.match(/(\d+)\s*(hour|day|week|month)s?\s*ago/);
        if (match) {
            const num = parseInt(match[1]);
            const unit = match[2];
            const ms = {
                hour: 3600000,
                day: 86400000,
                week: 604800000,
                month: 2592000000,
            }[unit] || 0;
            return new Date(Date.now() - num * ms).toISOString();
        }
        
        return null;
    };

    Actor.log.info('Creating crawler...');
    
    // Initialize header generator for realistic browser fingerprints
    const headerGenerator = new HeaderGenerator({
        browsers: [
            { name: "chrome", minVersion: 130, maxVersion: 131 }, // Nov 2025 Chrome versions
        ],
        devices: ["desktop"],
        locales: ["en-GB", "en-US"],
        operatingSystems: ["windows"],
    });
    
    const crawler = new CheerioCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: max_pages * 30, // Limit total requests
        maxRequestRetries: 5,
        requestHandlerTimeoutSecs: 60,
        maxConcurrency: 1, // Sequential requests only for maximum stealth
        
        // Aggressive session rotation for better stealth - rotate every 3-5 requests
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 50,
            sessionOptions: {
                maxUsageCount: 4, // Reduced from 10 to 4 for aggressive rotation
                maxAgeSecs: 150, // Reduced from 300 to 150 seconds (2.5 minutes)
            },
        },
        
        // Add custom headers before each request for stealth
        preNavigationHooks: [
            async ({ request, session, log }, gotoOptions) => {
                const headers = headerGenerator.getHeaders({
                    httpVersion: '2',
                    operatingSystems: ['windows'],
                    browsers: [{ name: 'chrome', minVersion: 130, maxVersion: 131 }],
                });
                
                // Override with specific headers for better stealth
                headers['accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8';
                headers['accept-language'] = 'en-GB,en;q=0.9,en-US;q=0.8';
                headers['accept-encoding'] = 'gzip, deflate, br';
                headers['cache-control'] = 'max-age=0';
                headers['sec-fetch-dest'] = 'document';
                headers['sec-fetch-mode'] = 'navigate';
                headers['sec-fetch-site'] = 'none';
                headers['sec-fetch-user'] = '?1';
                headers['upgrade-insecure-requests'] = '1';
                
                // Apply headers to the navigation options (this applies to HTTP requests)
                gotoOptions.headers = Object.assign({}, gotoOptions.headers || {}, headers);

                // Debug log to help understand what headers and UA are applied
                const ua = headers['user-agent'] || headers['User-Agent'] || headers['userAgent'] || 'unknown';
                Actor.log.info('PreNavigationHooks - applying headers', {
                    url: request.url,
                    sessionId: session?.id,
                    ua: String(ua).substring(0, 100),
                });
            },
        ],
        
        // Add human-like delays after each request
        postNavigationHooks: [
            async ({ request }) => {
                // Random delay between 2-5 seconds to simulate human reading
                const delay = 2000 + Math.random() * 3000;
                Actor.log.debug(`Human delay: ${Math.round(delay)}ms before next request`);
                await new Promise(resolve => setTimeout(resolve, delay));
            },
        ],

        async requestHandler({ request, $, crawler: crawlerInstance, log, session, response }) {
            try {
                const { label, pageNum = 1 } = request.userData;
            
            // Get HTML content for logging and block detection
            const html = $.html();
            const htmlLength = html.length;
            const pageTitle = $('title').text().trim();
            
            log.info(`Processing ${label} page`, {
                url: request.url,
                pageNum,
                statusCode: response?.statusCode || 'unknown',
                htmlLength,
                pageTitle: pageTitle.substring(0, 80),
                sessionId: session?.id,
            });
            
            Actor.log.info('Request successful', {
                url: request.url,
                contentLength: htmlLength,
                title: pageTitle,
            });
            
            // Block detection - check for captcha or blocked content
            const htmlLower = html.toLowerCase();
            const blockIndicators = [
                'captcha',
                'access denied',
                'blocked',
                'why have i been blocked',
                'cloudflare',
                'ray id:',
                'attention required',
                'security check',
            ];
            
            const isBlocked = blockIndicators.some(indicator => htmlLower.includes(indicator));
            if (isBlocked) {
                Actor.log.error('🚫 BLOCKED! Captcha or access denied detected', {
                    url: request.url,
                    pageTitle,
                    htmlLength,
                });
                
                // Retire the session immediately
                if (session) {
                    session.retire();
                    Actor.log.warning('Session retired due to block detection');
                }
                
                // Throw error to trigger retry with new session/IP
                throw new Error('Page blocked or captcha detected');
            }

            if (label === 'LIST') {
                stats.listPagesProcessed++;
                Actor.log.info('Selector counts', {
                    h2_job_a: $('h2 a[href*="/job/"]').length,
                    a_job: $('a[href*="/job/"]').length,
                    h2_all: $('h2').length,
                });
                
                // Log a couple of samples (safe and truncated)
                const sampleH2 = $('h2 a[href*="/job/"]').slice(0, 3).map((_, el) => $(el).text().trim()).get();
                const sampleAJob = $('a[href*="/job/"]').slice(0, 3).map((_, el) => $(el).text().trim()).get();
                Actor.log.info('Selector samples', { sampleH2, sampleAJob });
                
                // Extract jobs from list page
                const jobs = [];
                
                // Primary selector: h2 links containing job URLs
                $('h2 a[href*="/job/"]').each((_, el) => {
                    const $link = $(el);
                    const href = $link.attr('href');
                    const title = $link.text().trim();
                    
                    if (!href || !title || title.length < 3) return;
                    
                    // Build full URL
                    const jobUrl = new URL(href, request.url).href;
                    
                    // Extract metadata from parent container
                    const $container = $link.closest('article, li, div, section').first();
                    const containerText = $container.text();
                    
                    // Extract company name
                    let company = null;
                    const $companyLink = $container.find('a[href*="/jobs/"]').not($link).first();
                    if ($companyLink.length) {
                        company = $companyLink.text().trim();
                    }
                    
                    // Extract location
                    let location = null;
                    // Look for UK postcode pattern or city names
                    const locMatch = containerText.match(/\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}|[A-Z][a-z]+(?:,\s*[A-Z][a-z]+)*)\b/);
                    if (locMatch) {
                        location = locMatch[1].trim();
                    }
                    
                    // Extract salary
                    let salary = null;
                    const salMatch = containerText.match(/£[\d,]+(?:\.\d{2})?(?:\s*[-–]\s*£[\d,]+(?:\.\d{2})?)?(?:\s*(?:per|\/)\s*(?:hour|annum|year|day))?/i);
                    if (salMatch) {
                        salary = salMatch[0].trim();
                    }
                    
                    // Extract date posted
                    let datePosted = null;
                    const dateMatch = containerText.match(/(\d+\s*(?:hour|day|week|month)s?\s*ago|today|just\s*now)/i);
                    if (dateMatch) {
                        datePosted = parsePostedDate(dateMatch[0]);
                    }
                    
                    // Extract job type
                    let jobType = null;
                    const types = ['full time', 'part time', 'contract', 'permanent', 'temporary'];
                    const lowerText = containerText.toLowerCase();
                    for (const type of types) {
                        if (lowerText.includes(type)) {
                            jobType = type.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
                            break;
                        }
                    }
                    
                    jobs.push({
                        title,
                        company,
                        location,
                        salary,
                        job_type: jobType,
                        date_posted: datePosted,
                        url: jobUrl,
                        description_html: null,
                        description_text: null,
                    });
                });

                log.info(`Extracted ${jobs.length} jobs from page ${pageNum}`);
                stats.jobsExtracted += jobs.length;
                
                // Fallback for empty results - detailed debugging on page 1
                if (jobs.length === 0 && pageNum === 1) {
                    Actor.log.error('⚠️ NO JOBS FOUND on page 1! Debugging...', {
                        url: request.url,
                        htmlLength,
                        pageTitle,
                        h2Links: $('h2 a').length,
                        h2JobLinks: $('h2 a[href*="/job/"]').length,
                        allJobLinks: $('a[href*="/job/"]').length,
                        anyH2: $('h2').length,
                    });
                    
                    // Try alternative selectors
                    const altSelectors = [
                        'a[href*="/job/"]',
                        '.job-title a',
                        '[class*="job"] a[href*="/job/"]',
                        'article a[href*="/job/"]',
                    ];
                    
                    for (const selector of altSelectors) {
                        const count = $(selector).length;
                        if (count > 0) {
                            Actor.log.warning(`Alternative selector found ${count} matches: ${selector}`);
                        }
                    }
                    
                    // Log sample of HTML structure
                    const sampleHTML = $.html().substring(0, 2000);
                    Actor.log.debug('HTML sample (first 2000 chars):', sampleHTML);
                }

                // Save jobs
                for (const job of jobs) {
                    if (savedCount >= results_wanted) break;
                    if (savedUrls.has(job.url)) continue;
                    
                    if (collectDetails) {
                        // Enqueue detail page
                        await crawlerInstance.addRequests([{
                            url: job.url,
                            userData: { 
                                label: 'DETAIL',
                                listData: job,
                            },
                        }]);
                    } else {
                        // Save directly
                        await Dataset.pushData(job);
                        savedUrls.add(job.url);
                        savedCount++;
                        stats.jobsSaved++;
                        log.info(`✓ Saved job ${savedCount}/${results_wanted}: ${job.title}`);
                    }
                }

                // Pagination
                if (savedCount < results_wanted && pageNum < max_pages) {
                    // Look for next page
                    const nextPageNum = pageNum + 1;
                    const hasNextPage = $(`a[href*="page=${nextPageNum}"]`).length > 0 || 
                                      $('a:contains("Next"), a:contains("next")').length > 0;
                    
                    if (hasNextPage) {
                        const nextUrl = buildSearchUrl(keyword, location, nextPageNum);
                        await crawlerInstance.addRequests([{
                            url: nextUrl,
                            userData: { label: 'LIST', pageNum: nextPageNum },
                        }]);
                        log.info(`Queued page ${nextPageNum}`);
                    } else {
                        log.info('No more pages found');
                    }
                }
            } else if (label === 'DETAIL') {
                stats.detailPagesProcessed++;
                const { listData } = request.userData;
                
                if (savedCount >= results_wanted) return;
                if (savedUrls.has(listData.url)) return;
                
                // Extract enhanced details
                const job = { ...listData };
                
                // Try JSON-LD first
                $('script[type="application/ld+json"]').each((_, el) => {
                    try {
                        const data = JSON.parse($(el).html() || '');
                        const items = Array.isArray(data) ? data : [data];
                        for (const item of items) {
                            if (item['@type'] === 'JobPosting') {
                                job.title = item.title || job.title;
                                job.company = item.hiringOrganization?.name || job.company;
                                job.location = item.jobLocation?.address?.addressLocality || job.location;
                                job.salary = item.baseSalary?.value?.value || job.salary;
                                job.job_type = item.employmentType || job.job_type;
                                job.description_html = item.description || job.description_html;
                                if (item.datePosted) {
                                    job.date_posted = new Date(item.datePosted).toISOString();
                                }
                            }
                        }
                    } catch (e) {
                        // Ignore JSON parse errors
                    }
                });
                
                // Fallback to HTML scraping
                if (!job.title) {
                    job.title = $('h1').first().text().trim() || listData.title;
                }
                
                if (!job.company) {
                    job.company = $('[class*="company"], [class*="employer"]').first().text().trim() || listData.company;
                }
                
                if (!job.description_html) {
                    const $desc = $('.job-description, [class*="description"], article').first();
                    if ($desc.length) {
                        job.description_html = $desc.html();
                        job.description_text = $desc.text().replace(/\s+/g, ' ').trim();
                    }
                }
                
                if (!job.location) {
                    job.location = $('[class*="location"]').first().text().trim() || listData.location;
                }
                
                if (!job.salary) {
                    job.salary = $('[class*="salary"]').first().text().trim() || listData.salary;
                }
                
                // Save enriched job
                await Dataset.pushData(job);
                savedUrls.add(job.url);
                savedCount++;
                stats.jobsSaved++;
                log.info(`✓ Saved job ${savedCount}/${results_wanted}: ${job.title}`);
            }
            } catch (err) {
                Actor.log.error('RequestHandler error', {
                    url: request.url,
                    message: err.message,
                    stack: err.stack,
                });
                throw err;
            }
        },

        async failedRequestHandler({ request, error }, { log, session }) {
            const retryCount = request.retryCount || 0;
            
            // Calculate exponential backoff: 2^retry * 1000ms + random jitter (0-1000ms)
            const baseDelay = Math.pow(2, retryCount) * 1000;
            const jitter = Math.random() * 1000;
            const totalDelay = baseDelay + jitter;
            
            log.error(`Request failed for ${request.url}`, {
                error: error.message,
                retries: retryCount,
                nextRetryIn: `${Math.round(totalDelay / 1000)}s`,
            });
            
            Actor.log.error('Failed request details', {
                url: request.url,
                error: error.message,
                willRetry: retryCount < 5,
            });
            
            // Apply exponential backoff delay before retry
            if (retryCount < 5) {
                Actor.log.info(`Waiting ${Math.round(totalDelay / 1000)}s before retry #${retryCount + 1}`);
                await new Promise(resolve => setTimeout(resolve, totalDelay));
            }
            
            // Retire session on failure
            if (session) {
                session.retire();
                Actor.log.info('Session retired due to failure');
            }
        },
    });
    
    Actor.log.info('✓ Crawler created successfully');

    // Start crawling
    const startUrl = buildSearchUrl(keyword, location, 1);
    Actor.log.info('Starting crawler', { startUrl });
    
    await crawler.run([{
        url: startUrl,
        userData: { label: 'LIST', pageNum: 1 },
    }]);

    // Final stats
    Actor.log.info('✅ Scraping completed', {
        jobsSaved: savedCount,
        target: results_wanted,
        stats,
    });

    await Actor.setValue('STATS', stats);

} catch (error) {
    Actor.log.error('Fatal error:', {
        message: error.message,
        stack: error.stack,
    });
    throw error;
} finally {
    await Actor.exit();
}
