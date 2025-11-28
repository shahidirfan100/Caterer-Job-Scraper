// Caterer Job Scraper - Robust production implementation using Crawlee CheerioCrawler
// Simplified approach: Use Crawlee's battle-tested infrastructure for maximum reliability
import { Actor } from 'apify';
import { CheerioCrawler, ProxyConfiguration, Dataset } from 'crawlee';

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

    // Use Apify's proxy configuration - simpler and more reliable
    const proxyConfiguration = await Actor.createProxyConfiguration({
        groups: ['RESIDENTIAL'],
        countryCode: 'GB',
    });

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

    const crawler = new CheerioCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: max_pages * 30, // Limit total requests
        maxRequestRetries: 5,
        requestHandlerTimeoutSecs: 60,
        maxConcurrency: 2, // Lower concurrency to avoid rate limiting
        minConcurrency: 1,
        
        // Session management for better success rate
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 50,
            sessionOptions: {
                maxUsageCount: 10,
                maxAgeSecs: 300,
            },
        },

        async requestHandler({ request, $, crawler: crawlerInstance, log, session }) {
            const { label, pageNum = 1 } = request.userData;

            log.info(`Processing ${label} page`, {
                url: request.url,
                pageNum,
                statusCode: 200,
            });

            if (label === 'LIST') {
                stats.listPagesProcessed++;
                
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
        },

        failedRequestHandler({ request, error }, { log, session }) {
            log.error(`Request failed for ${request.url}`, {
                error: error.message,
                retries: request.retryCount,
            });
            // Retire session on failure
            if (session) {
                session.retire();
            }
        },
    });

    // Start crawling
    const startUrl = buildSearchUrl(keyword, location, 1);
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
