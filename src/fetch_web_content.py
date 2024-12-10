import threading
import time
from web_crawler import WebScraper
from serper_service import SerperClient

class WebContentFetcher:
    def __init__(self, query, search_location="Vienna, Austria", search_language="en", output_language="en"):
        # Initialize the fetcher with a search query
        self.query = query
        self.web_contents = []  # Stores the fetched web contents
        self.error_urls = []  # Stores URLs that resulted in an error during fetching
        self.web_contents_lock = threading.Lock()  # Lock for thread-safe operations on web_contents
        self.error_urls_lock = threading.Lock()  # Lock for thread-safe operations on error_urls
        self.search_location = search_location
        self.search_language = search_language

    def _web_crawler_thread(self, thread_id: int, urls: list):
        # Thread function to crawl each URL*
        try:
    
            url = urls[thread_id]
            print(f"Starting web crawler thread {thread_id} with {url}")
            start_time = time.time()
            scraper = WebScraper()
            content = scraper.scrape_url(url, 0)

            if not content:
                raise Exception("Timeout")
            # If the scraped content is too short, try extending the crawl rules
            if 0 < len(content) < 800:
                content = scraper.scrape_url(url, 1)
            # If the content length is sufficient, add it to the shared list
            if len(content) > 600:
                with self.web_contents_lock:
                    self.web_contents.append({"url": url, "content": content})
            end_time = time.time()
            print(f"Thread {thread_id} completed! Time consumed: {end_time - start_time:.2f}s")
        except Exception as e:
            # Handle any exceptions, log the error, and store the URL
            with self.error_urls_lock:
                self.error_urls.append(url)
            print(f"Thread {thread_id}: Error crawling {url}: {e}")

    def _serper_launcher(self):
        # Function to launch the Serper client and get search results
        serper_client = SerperClient()
        serper_results = serper_client.serper(self.query, self.search_location, self.search_language)
        return serper_client.extract_components(serper_results)

    def _crawl_threads_launcher(self, url_list):
        # Create and start threads for each URL in the list
        threads = []
        for i in range(len(url_list)):
            thread = threading.Thread(target=self._web_crawler_thread, args=(i, url_list))
            threads.append(thread)
            thread.start()
        # Wait for all threads to finish execution
        for thread in threads:
            thread.join()

    def fetch(self):
        # Main method to fetch web content based on the query
        serper_response = self._serper_launcher()
        if serper_response:
            print(f"Fetching web content for query: {self.query}")
            url_list = serper_response["links"]
            if len(url_list) <= 3:
                return [], None
            print(f"Found {len(url_list)} URLs from the search results")
            self._crawl_threads_launcher(url_list)
            # Reorder the fetched content to match the order of URLs
            ordered_contents = [
                next((item['content'] for item in self.web_contents if item['url'] == url), '') 
                for url in url_list
            ]
            i = 0
            for l in ordered_contents:
                if not l or l.strip() == '':
                    i += 1
            if i>=7:
                return [], None
            return ordered_contents, serper_response
        return [], None

# Example usage
if __name__ == "__main__":
    fetcher = WebContentFetcher("505U Premium HE RH #3 S (GDI IZ 95)")
    contents, serper_response = fetcher.fetch()

    print(contents)
    