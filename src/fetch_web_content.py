import threading
import time
from web_crawler import WebScraper
from serper_service import SerperClient

class WebContentFetcher:
    def __init__(self, query, search_location="Vienna, Austria", search_language="German", output_language="German"):
        self.query = query
        self.web_contents = []
        self.error_urls = []
        self.web_contents_lock = threading.Lock()
        self.error_urls_lock = threading.Lock()
        self.search_location = search_location
        self.search_language = search_language

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

            end_time = time.time()
            print(f"Crawling completed for {url}. Time consumed: {end_time - start_time:.2f}s")

        except Exception as e:
            with self.error_urls_lock:
                self.error_urls.append(url)
            print(f"Error crawling {url}: {e}")

    def _serper_launcher(self):
        serper_client = SerperClient()
        serper_results = serper_client.serper(self.query, self.search_location, self.search_language)
        return serper_client.extract_components(serper_results)

    def _crawl_threads_launcher(self, url_list):
        threads = []
        for url in url_list:
            thread = threading.Thread(target=self._web_crawler_thread, args=(url,))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

    def fetch(self):
        serper_response = self._serper_launcher()
        if serper_response:
            print(f"Fetching web content for query: {self.query}")
            url_list = serper_response["links"]
            print(f"Found {len(url_list)} URLs from the search results")
            self._crawl_threads_launcher(url_list)
            ordered_contents = [next((item['content'] for item in self.web_contents if item['url'] == url), '') for url in url_list]
            return ordered_contents, serper_response
        return [], None

if __name__ == "__main__":
    fetcher = WebContentFetcher("What happened to Silicon Valley Bank")
    contents, serper_response = fetcher.fetch()

    print(serper_response)
    print(contents, '\n\n')
