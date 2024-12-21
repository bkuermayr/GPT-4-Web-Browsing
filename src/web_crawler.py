from dotenv import load_dotenv
import grequests
from bs4 import BeautifulSoup
import os
from scrapfly import ScrapeConfig, ScrapflyClient, ScrapeApiResponse
# Load .env file
load_dotenv()

class WebScraper:


    def __init__(self, user_agent='macOS'):
        # Initialize the scraper with a user agent (default is 'macOS')
        self.headers = self._get_headers(user_agent)
        self.scrapfly = ScrapflyClient(key=os.getenv('SCRAPFLY_API_KEY'))

    def _get_headers(self, user_agent):
        # Private method to get headers for the request based on the specified user agent
        if user_agent == 'macOS':
            # Headers for macOS user agent
            return {
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
            }
        else:
            # Headers for Windows user agent
            return {
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
            }

    def get_webpage_html(self, url):
        req = grequests.head(url,headers=self.headers, timeout = 10)
        #response = requests.head(url, headers=self.headers,timeout=(10,20))
        response = grequests.map([req])[0]
        #Send the request and get the response

        if response:  # Ensure response is not None
                # Check if the Content-Type header indicates HTML content
            if not response or  not response.headers.get('Content-Type', '').startswith('text/html'):
                # Skip non-HTML content
                raise Exception('Non HTML Content')
        else:
            raise Exception(f'Access Forbidden')
        try:
            # Create a HEAD request to fetch headers only
        #request_url = 'https://api.scrapfly.io/scrape'
        #params = {
        #    'key': os.getenv('SCRAPFLY_API_KEY'),
        #    'url': url,
        #    'render_js': 'false',
        #    'cache': 'true',
        #    'asp': 'true',
        #    'retry':'false',
        #    'timeout':'30000'
        #}
            conf = ScrapeConfig(asp=True,render_js=False,
                         url=url, retry=False, timeout=30000)

            response = self.scrapfly.scrape(scrape_config=conf)
            if response and response.success:
                return response.scrape_result['content']
            else:
                raise Exception(f'Request failed with status {response.status_code}')  

        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None
        
    def convert_html_to_soup(self, html):
        # Convert the HTML string to a BeautifulSoup object for parsing
        if html:
            html_string = html
            return BeautifulSoup(html_string, "lxml")
        return None

    def extract_main_content(self, html_soup,rule=0):
        # Extract the main content from a BeautifulSoup object
        text_elements = []
        allowlist = ['p','span','li','h1','h2','h3','h4','h5','h6']
        if rule==1:
            allowlist.append('div') 
        # Iterate through specified tags and collect their text
        if html_soup:
            text_elements = [t for t in html_soup.find_all(text=True) if t.parent.name in allowlist and t.strip()]
        return "\n".join(text_elements).strip()

    def scrape_url(self, url, rule=0):
        # Public method to scrape a URL and extract its main content
        webpage_html = self.get_webpage_html(url)
        if not webpage_html:
            return None
        soup = self.convert_html_to_soup(webpage_html)
        main_content = self.extract_main_content(soup,rule)
        return main_content
    

# Example usage
if __name__ == "__main__":
    scraper = WebScraper(user_agent='macOS')
    test_url = "https://www.puetzgolf.com/24-ai-one-mld-8-t-s-putter-24-ai-one-mld-8-t-s-putter"
    main_content = scraper.scrape_url(test_url)
    print(main_content)
