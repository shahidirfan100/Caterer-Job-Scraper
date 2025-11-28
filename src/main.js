// Caterer Job Scraper - Production-grade implementation using Playwright for JS rendering
// Comprehensive stealth measures with browser fingerprinting and dynamic content handling
import { Actor } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { chromium } from 'playwright';

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
    Actor.log.info('Actor code version', { version: 'playwright_v3', crawler: 'PlaywrightCrawler', date: new Date().toISOString() });

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

    Actor.log.info('Creating Playwright crawler with stealth measures...');
    
    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: max_pages * 30,
        maxRequestRetries: 5,
        requestHandlerTimeoutSecs: 90, // Increased for JS rendering
        maxConcurrency: 1, // Sequential for maximum stealth
        
        // Aggressive session rotation
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 50,
            sessionOptions: {
                maxUsageCount: 4,
                maxAgeSecs: 150,
            },
        },
        
        // Playwright-specific: Use chromium with stealth
        launchContext: {
            launcher: chromium,
            launchOptions: {
                headless: true,
                args: [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--lang=en-GB,en-US',
                ],
            },
            // Browser context with stealth fingerprint
            contextOptions: {
                userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                viewport: { 
                    width: 1920 + Math.floor(Math.random() * 100), 
                    height: 1080 + Math.floor(Math.random() * 100) 
                },
                locale: 'en-GB',
                timezoneId: 'Europe/London',
                permissions: [],
                extraHTTPHeaders: {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Cache-Control': 'max-age=0',
                },
            },
        },
        
        // Pre-navigation: Inject stealth scripts
        preNavigationHooks: [
            async ({ request, page, session }) => {
                Actor.log.info('Pre-navigation stealth setup', {
                    url: request.url,
                    sessionId: session?.id,
                });
                
                // Stealth: Override webdriver detection
                await page.addInitScript(() => {
                    // Remove webdriver flag
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => false,
                    });
                    
                    // Mock plugins
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5],
                    });
                    
                    // Mock languages
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-GB', 'en-US', 'en'],
                    });
                    
                    // Mock chrome runtime
                    window.chrome = {
                        runtime: {},
                    };
                    
                    // Mock permissions
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );
                });
            },
        ],
        
        // Post-navigation: Wait for dynamic content and add human delays
        postNavigationHooks: [
            async ({ page, request }) => {
                // Wait for network to be idle (all XHR/fetch complete)
                try {
                    await page.waitForLoadState('networkidle', { timeout: 15000 });
                    Actor.log.debug('Network idle reached');
                } catch (e) {
                    Actor.log.warning('Network idle timeout, continuing anyway');
                }
                
                // Random human-like delay
                const delay = 2000 + Math.random() * 3000;
                Actor.log.debug(`Human delay: ${Math.round(delay)}ms`);
                await page.waitForTimeout(delay);
                
                // Random mouse movements for extra stealth
                try {
                    await page.mouse.move(
                        Math.random() * 500,
                        Math.random() * 500
                    );
                } catch (e) {
                    // Ignore mouse errors
                }
            },
        ],

        async requestHandler({ request, page, crawler: crawlerInstance, log, session }) {
            try {
                const { label, pageNum = 1 } = request.userData;
            
            // Get page content for logging and block detection
            const html = await page.content();
            const htmlLength = html.length;
            const pageTitle = await page.title();
            
            log.info(`Processing ${label} page`, {
                url: request.url,
                pageNum,
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
                'just a moment',
            ];
            
            const isBlocked = blockIndicators.some(indicator => htmlLower.includes(indicator));
            if (isBlocked) {
                Actor.log.error('🚫 BLOCKED! Captcha or access denied detected', {
                    url: request.url,
                    pageTitle,
                    htmlLength,
                });
                
                // Take screenshot for debugging
                try {
                    const screenshot = await page.screenshot({ fullPage: false });
                    await Actor.setValue('BLOCKED_SCREENSHOT', screenshot, { contentType: 'image/png' });
                    Actor.log.info('Blocked page screenshot saved to key-value store');
                } catch (e) {
                    Actor.log.warning('Failed to save screenshot', { error: e.message });
                }
                
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
                
                // Wait for job listings to load (try multiple selectors)
                const selectors = [
                    'h2 a[href*="/job/"]',
                    'a[href*="/job/"]',
                    '[class*="job"]',
                    'article',
                ];
                
                let selectorFound = false;
                for (const selector of selectors) {
                    try {
                        await page.waitForSelector(selector, { timeout: 10000, state: 'attached' });
                        Actor.log.info(`Found content with selector: ${selector}`);
                        selectorFound = true;
                        break;
                    } catch (e) {
                        Actor.log.debug(`Selector ${selector} not found, trying next...`);
                    }
                }
                
                if (!selectorFound) {
                    Actor.log.warning('No job selectors found, page may not have loaded correctly');
                }
                
                // Extract jobs using Playwright's page.evaluate for fast DOM access
                const jobs = await page.evaluate((pageUrl) => {
                    const results = [];
                    
                    // Try multiple selector strategies
                    const jobLinks = Array.from(document.querySelectorAll('h2 a[href*="/job/"], a[href*="/job/"]'));
                    const seenUrls = new Set();
                    
                    jobLinks.forEach(link => {
                        const href = link.getAttribute('href');
                        const title = link.textContent?.trim();
                        
                        if (!href || !title || title.length < 3) return;
                        
                        // Build full URL
                        const jobUrl = new URL(href, pageUrl).href;
                        
                        // Skip duplicates
                        if (seenUrls.has(jobUrl)) return;
                        seenUrls.add(jobUrl);
                        
                        // Find parent container
                        let container = link.closest('article, li, div[class*="job"], section');
                        if (!container) container = link.parentElement;
                        
                        const containerText = container?.textContent || '';
                        
                        // Extract company
                        let company = null;
                        const companyLink = container?.querySelector('a[href*="/jobs/"]:not([href*="/job/"])');
                        if (companyLink) {
                            company = companyLink.textContent?.trim();
                        }
                        
                        // Extract location (UK postcode or city)
                        let location = null;
                        const locMatch = containerText.match(/\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}|London|Manchester|Birmingham|Leeds|Glasgow|Liverpool|Edinburgh|Bristol|Sheffield|Newcastle)/i);
                        if (locMatch) {
                            location = locMatch[0].trim();
                        }
                        
                        // Extract salary
                        let salary = null;
                        const salMatch = containerText.match(/£[\d,]+(?:\.\d{2})?(?:\s*[-–]\s*£[\d,]+(?:\.\d{2})?)?(?:\s*(?:per|\/)\s*(?:hour|annum|year|day))?/i);
                        if (salMatch) {
                            salary = salMatch[0].trim();
                        }
                        
                        // Extract date posted
                        let datePosted = null;
                        const dateMatch = containerText.match(/(\d+)\s*(hour|day|week|month)s?\s*ago|today|just\s*now/i);
                        if (dateMatch) {
                            const text = dateMatch[0].toLowerCase();
                            if (text.includes('today') || text.includes('just now')) {
                                datePosted = new Date().toISOString();
                            } else {
                                const num = parseInt(dateMatch[1] || '0');
                                const unit = dateMatch[2];
                                const ms = {
                                    hour: 3600000,
                                    day: 86400000,
                                    week: 604800000,
                                    month: 2592000000,
                                }[unit] || 0;
                                datePosted = new Date(Date.now() - num * ms).toISOString();
                            }
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
                        
                        results.push({
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
                    
                    return results;
                }, request.url);
                
                Actor.log.info('Selector counts', {
                    h2_job_links: await page.locator('h2 a[href*="/job/"]').count(),
                    all_job_links: await page.locator('a[href*="/job/"]').count(),
                    articles: await page.locator('article').count(),
                });
                
                log.info(`Extracted ${jobs.length} jobs from page ${pageNum}`);
                stats.jobsExtracted += jobs.length;
                
                // Fallback for empty results - detailed debugging on page 1
                if (jobs.length === 0 && pageNum === 1) {
                    const debugInfo = await page.evaluate(() => {
                        return {
                            h2Links: document.querySelectorAll('h2 a').length,
                            h2JobLinks: document.querySelectorAll('h2 a[href*="/job/"]').length,
                            allJobLinks: document.querySelectorAll('a[href*="/job/"]').length,
                            anyH2: document.querySelectorAll('h2').length,
                            bodyText: document.body?.textContent?.substring(0, 500),
                        };
                    });
                    
                    Actor.log.error('⚠️ NO JOBS FOUND on page 1! Debugging...', {
                        url: request.url,
                        htmlLength,
                        pageTitle,
                        ...debugInfo,
                    });
                    
                    // Try alternative selectors
                    const altSelectors = [
                        'a[href*="/job/"]',
                        '.job-title a',
                        '[class*="job"] a[href*="/job/"]',
                        'article a[href*="/job/"]',
                    ];
                    
                    for (const selector of altSelectors) {
                        const count = await page.locator(selector).count();
                        if (count > 0) {
                            Actor.log.warning(`Alternative selector found ${count} matches: ${selector}`);
                        }
                    }
                    
                    // Take screenshot for debugging
                    try {
                        const screenshot = await page.screenshot({ fullPage: true });
                        await Actor.setValue('DEBUG_SCREENSHOT', screenshot, { contentType: 'image/png' });
                        Actor.log.info('Debug screenshot saved to key-value store');
                    } catch (e) {
                        Actor.log.warning('Failed to save screenshot', { error: e.message });
                    }
                    
                    // Save HTML for inspection
                    await Actor.setValue('DEBUG_HTML', html, { contentType: 'text/html' });
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
                    const nextPageNum = pageNum + 1;
                    
                    // Check for next page link in DOM
                    const hasNextPage = await page.evaluate((nextNum) => {
                        // Check for page parameter in links
                        const pageLinks = document.querySelectorAll(`a[href*="page=${nextNum}"]`);
                        if (pageLinks.length > 0) return true;
                        
                        // Check for "Next" button
                        const nextButtons = Array.from(document.querySelectorAll('a')).filter(a => {
                            const text = a.textContent?.toLowerCase() || '';
                            return text.includes('next') || text.includes('→') || text.includes('»');
                        });
                        return nextButtons.length > 0;
                    }, nextPageNum);
                    
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
                
                // Wait for content to load
                try {
                    await page.waitForSelector('h1, [class*="job"]', { timeout: 10000 });
                } catch (e) {
                    Actor.log.warning('Detail page selector timeout');
                }
                
                // Extract enhanced details using page.evaluate
                const detailData = await page.evaluate(() => {
                    const result = {};
                    
                    // Try JSON-LD first (most reliable)
                    const jsonLdScripts = document.querySelectorAll('script[type="application/ld+json"]');
                    jsonLdScripts.forEach(script => {
                        try {
                            const data = JSON.parse(script.textContent || '');
                            const items = Array.isArray(data) ? data : [data];
                            items.forEach(item => {
                                if (item['@type'] === 'JobPosting') {
                                    result.title = item.title;
                                    result.company = item.hiringOrganization?.name;
                                    result.location = item.jobLocation?.address?.addressLocality;
                                    result.salary = item.baseSalary?.value?.value;
                                    result.job_type = item.employmentType;
                                    result.description_html = item.description;
                                    result.date_posted = item.datePosted ? new Date(item.datePosted).toISOString() : null;
                                }
                            });
                        } catch (e) {
                            // Ignore parse errors
                        }
                    });
                    
                    // Fallback to HTML scraping
                    if (!result.title) {
                        const h1 = document.querySelector('h1');
                        result.title = h1?.textContent?.trim();
                    }
                    
                    if (!result.company) {
                        const companyEl = document.querySelector('[class*="company"], [class*="employer"]');
                        result.company = companyEl?.textContent?.trim();
                    }
                    
                    if (!result.description_html) {
                        const descEl = document.querySelector('.job-description, [class*="description"], article');
                        if (descEl) {
                            result.description_html = descEl.innerHTML;
                            result.description_text = descEl.textContent?.replace(/\s+/g, ' ').trim();
                        }
                    }
                    
                    if (!result.location) {
                        const locEl = document.querySelector('[class*="location"]');
                        result.location = locEl?.textContent?.trim();
                    }
                    
                    if (!result.salary) {
                        const salEl = document.querySelector('[class*="salary"]');
                        result.salary = salEl?.textContent?.trim();
                    }
                    
                    return result;
                });
                
                // Merge with list data
                const job = {
                    ...listData,
                    ...Object.fromEntries(Object.entries(detailData).filter(([_, v]) => v != null)),
                };
                
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
