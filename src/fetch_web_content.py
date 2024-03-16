import threading
import time

from serper_service import SerperClient
from web_crawler import WebScraper
from dotenv import load_dotenv




class WebContentFetcher:
    MAX_RETRIES = 1  # Maximum number of retries for each URL
    ALLOWED_FAILURES = 5  # Allow up to 5 URLs to fail

    def __init__(self, query, search_location="Vienna, Austria", search_language="German",output_language="German"):
        self.query = query
        self.web_contents = []
        self.error_urls = []
        self.web_contents_lock = threading.Lock()
        self.error_urls_lock = threading.Lock()
        self.url_status = {}  # Tracks URL crawl status: 'pending', 'success', 'failed'
        self.url_retries = {}  # Tracks the number of retries for each URL
        self.search_location = search_location
        self.search_language = search_language
        self.output_language = output_language
        self.failed_count = 0  # Tracks the number of URLs that have ultimately failed

    def _web_crawler_thread(self, url: str):
        try:
            print(f"Starting web crawler for {url}")
            start_time = time.time()
            scraper = WebScraper()
            content = scraper.scrape_url(url, 0)

            if 0 < len(content) < 800:
                content = scraper.scrape_url(url, 1)

            if len(content) > 600:
                with self.web_contents_lock:
                    self.web_contents.append({"url": url, "content": content})
                self.url_status[url] = 'success'
            else:
                raise ValueError("Content too short, considered as failed.")

            end_time = time.time()
            print(f"Crawling completed for {url}. Time consumed: {end_time - start_time:.2f}s")

        except Exception as e:
            with self.error_urls_lock:
                self.error_urls.append(url)
            self.url_status[url] = 'failed'
            self.url_retries[url] = self.url_retries.get(url, 0) + 1  # Increment retries
            print(f"Error crawling {url}: {e}")

    def _serper_launcher(self):
        serper_client = SerperClient()
        serper_results = serper_client.serper(self.query, self.search_location, self.search_language)
        return serper_client.extract_components(serper_results)

    def _crawl_threads_launcher(self, url_list):
        threads = []
        for url in url_list:
            if self.url_status.get(url) != 'success' and self.url_retries.get(url, 0) < self.MAX_RETRIES:
                self.url_status[url] = 'pending'
                thread = threading.Thread(target=self._web_crawler_thread, args=(url,))
                threads.append(thread)
                thread.start()
        for thread in threads:
            thread.join()

    def retry_failed_crawls(self):
        self.failed_count = 0  # Reset failed count before retrying
        while self.failed_count <= self.ALLOWED_FAILURES:
            failed_urls = [url for url, status in self.url_status.items() if status == 'failed' and self.url_retries[url] < self.MAX_RETRIES]
            if not failed_urls or self.failed_count >= self.ALLOWED_FAILURES:
                break  # Stop if no failed URLs left or allowed failures are reached
            print(f"Retrying failed URLs: {failed_urls}")
            self._crawl_threads_launcher(failed_urls)
            self.failed_count = len([url for url in failed_urls if self.url_status[url] == 'failed'])

    def fetch(self):
        serper_response = self._serper_launcher()
        if serper_response:
            print(f"Fetching web content for query: {self.query}")
            url_list = serper_response["links"]
            print(f"Found {len(url_list)} URLs from the search results")
            self._crawl_threads_launcher(url_list)
            ordered_contents = [next((item for item in self.web_contents if item['url'] == url), {'content': ''})['content'] for url in url_list]
            return ordered_contents, serper_response
        return [], None        

if __name__ == "__main__":
    fetcher = WebContentFetcher("What happened to Silicon Valley Bank")
    contents, serper_response = fetcher.fetch()
    fetcher.retry_failed_crawls()

    print("\nFinal results:")
    print(serper_response)
    print(contents, '\n\n')
