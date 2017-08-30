import os
from urllib.parse import urlparse
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from urllib import parse
import threading
from queue import Queue

threadLock = threading.Lock()

DOC_ID = 0
MIN_LENGTH = 150
MAX_CORPUS_SIZE = 1200
DIR_NAME = 'sciencealert/'
HOMEPAGE = 'http://www.sciencealert.com/'
DOMAIN_NAME = 'www.sciencealert.com'
QUEUE_FILE = DIR_NAME + '/queue.txt'
CRAWLED_FILE = DIR_NAME + '/crawled.txt'
NUMBER_OF_THREADS = 10

# Assign doc id and increment counter thread safe manner
def assign_doc_id():
    global DOC_ID
    res = 0
    with threadLock:
        DOC_ID += 1 
        res = DOC_ID
    return res

# Get domain name 
def get_domain(url):
    return urlparse(url).netloc

# Prevent revisiting of URLs
def file_to_set(path):
    results = set()
    with open(path, 'rt') as f:
        for line in f:
            results.add(line.replace('\n', ''))
    return results

# Iterate over set
def set_to_file(links, path):
    with open(path,"w") as f:
        for l in sorted(links):
            f.write(l + "\n")

# Get number of words in page
def word_count(string):
    return len(string.split(' '))


# Gathers links, and parses link if article and more than 150 words.
# Links to gather
# URL, TITLE, META-KEYWORDS, DATE, DOC ID (Auto-generated, sequential), CONTENT
def parse_page(base_url, relative_or_whole_url, html_doc):
    # URL
    page_info = {'url': parse.urljoin(base_url, relative_or_whole_url), 'flag': False}

    soup = BeautifulSoup(html_doc, 'html.parser')

    # Get all links to be gathered.
    links = set()
    for link in soup.find_all('a'):
        url = parse.urljoin(base_url, link.get('href')) 

        if get_domain(url) == DOMAIN_NAME:
            links.add(url)
    page_info['links'] = links  

    # Check for article type of page beefore
    page_type = soup.find("meta",  property="og:type")
    if page_type is not None and page_type["content"] == "article":
        # Get the text.

        # Kill all script and style elements
        for script in soup(["script", "style", "a"]):
            script.extract()    # rip it out
        page_text = soup.find("div", class_="article-fulltext")

        if page_text is not None:
            page_text = page_text.get_text()
            if word_count(page_text) > MIN_LENGTH:
                # CONTENT 
                page_info['content'] = page_text

                # TITLE
                page_info['title'] = soup.title.string 

                # DATE
                date = soup.find_all("div", class_="author-name-date floatstyle")
                if date is not None and len(date) > 0:
                    page_info['date'] = date[0].find("span").string

                    # META-KEYWORDS 
                    keywords = soup.find_all("meta",  attrs={"name":"keywords"})        
                    if keywords is not None and len(keywords) > 0:
                        page_info['keywords'] = keywords[0]["content"]

                        # Turn on flag
                        page_info['flag'] = True
                        page_info['doc_id'] = assign_doc_id()

                        print("Passed DOC ID" + str(page_info['doc_id']))
                        # Write to file 
                        with open(DIR_NAME + str(page_info["doc_id"]) + '.txt', 'a+') as f:
                            f.write("URL: " + page_info['url'] + "\n")
                            f.write("TITLE: " + page_info['title'] + "\n")
                            f.write("META-KEYWORDS: " + page_info['keywords'] + "\n")
                            f.write("DATE: " + page_info['date'] + "\n")
                            f.write("DOC ID: " + str(page_info['doc_id']) + "\n")
                            f.write(page_info['content'])

    return page_info


class Crawler:
    project_name = ''   # Folder name
    base_url = ''       # Base URL to start from
    domain_name = ''    # Domain name of website
    queue_file = ''     # File to store queue
    crawled_file = ''   # File to store crawled URLs

    # Set to prevent duplicates
    queue = set()
    crawled = set()

    # Take arguments and init the name variables
    def __init__(self, project_name, base_url, domain_name):
        Crawler.project_name = project_name
        Crawler.base_url = base_url
        Crawler.domain_name = domain_name
        Crawler.queue_file = Crawler.project_name + '/queue.txt'
        Crawler.crawled_file = Crawler.project_name + '/crawled.txt'
        self.setup()
        self.crawl_page('First spider', Crawler.base_url)

    # Creates directory and files for project on first run and starts the crawler
    @staticmethod
    def setup():
        if not os.path.exists(Crawler.project_name):
            os.makedirs(Crawler.project_name)
            print('Created directory ' + Crawler.project_name)

        bfs_queue = os.path.join(Crawler.project_name, 'queue.txt')
        crawled = os.path.join(Crawler.project_name, 'crawled.txt')

        if not os.path.isfile(bfs_queue):
            with open(bfs_queue, 'w') as f:
                f.write(Crawler.base_url)

        if not os.path.isfile(crawled):
            with open(crawled, 'w') as f:
                f.write('')


        Crawler.queue = file_to_set(Crawler.queue_file)
        Crawler.crawled = file_to_set(Crawler.crawled_file)

    # Updates user display, fills queue and updates files
    @staticmethod
    def crawl_page(thread_name, page_url):
        if page_url not in Crawler.crawled:
            print(thread_name + ' now we are crawling ' + page_url)
            print('Queue size ' + str(len(Crawler.queue)) + ' | Crawled size ' + str(len(Crawler.crawled)))
            Crawler.add_links_to_queue(Crawler.gather_links(page_url))
            Crawler.queue.remove(page_url)
            Crawler.crawled.add(page_url)
            Crawler.update_files()

    # Converts raw response data into readable information and checks for html
    @staticmethod
    def gather_links(page_url):
        html_string = ''
        try:
            response = urlopen(Request(page_url, headers={'User-Agent': 'Mozilla/5.0'}))
            if 'text/html' in response.getheader('Content-Type'):
                html_bytes = response.read()
                html_string = html_bytes.decode("utf-8")
                # print(html_string)
            page_info = parse_page(Crawler.base_url, page_url, html_string)

        except Exception as e:
            print(page_url)
            print(str(e))
            return set()
        # NEED TO RETURN LINKS
        return page_info['links']

    # Saves queue data to project files
    @staticmethod
    def add_links_to_queue(links):
        for url in links:
            if (url in Crawler.queue) or (url in Crawler.crawled):
                continue
            Crawler.queue.add(url)

    @staticmethod
    def update_files():
        set_to_file(Crawler.queue, Crawler.queue_file)
        set_to_file(Crawler.crawled, Crawler.crawled_file)

if __name__ == "__main__":
    queue = Queue()
    Crawler(DIR_NAME, HOMEPAGE, DOMAIN_NAME)


    # Create multiple threads
    def create_workers():
        for _ in range(NUMBER_OF_THREADS):
            t = threading.Thread(target=work)
            t.daemon = True
            t.start()

    # Each queued item is a new job
    def create_jobs():
        for link in file_to_set(QUEUE_FILE):
            queue.put(link)
        queue.join()
        crawl()

    # Do the next queued task
    def work():
        while True:
            url = queue.get()
            Crawler.crawl_page(threading.current_thread().name, url)
            queue.task_done()


    # Crawl if items in queue and have not reached required size.
    def crawl():
        queued_links = file_to_set(QUEUE_FILE)
        if len(queued_links) > 0 and DOC_ID < MAX_CORPUS_SIZE:
            print(str(len(queued_links)) + ' links in the queue')
            create_jobs()


    create_workers()
    crawl()

