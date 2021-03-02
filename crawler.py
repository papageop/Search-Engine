from bs4 import BeautifulSoup
import nltk
from nltk.stem import PorterStemmer
import re
import threading
import time
from collections import Counter
from urllib import request
from indexer import Indexer
from mongodb import MongoDB
import sys


class Crawler:
    def __init__(self, url: str, keep: bool, max_size: int, num_threads: int):

        self.stop_words = set(nltk.corpus.stopwords.words("english"))
        self.crawled_pages = 0
        self.countLocker = threading.Lock()
        self.urls = [url]
        self.urlsLocker = threading.Lock()
        self.threads_array = []
        self.num_threads = num_threads
        self.db = MongoDB()
        self.max_size = max_size
        #If the user has selected to delete previous data and drop crawler database
        if keep == 0:
            self.db.reset_crawler()
        self.indexer = Indexer(self.num_threads)

    def crawl(self):
        '''
        This method is used to crawl documents starting from a url given by user until there are
        no more urls to crawl or we have reached the maximum number of crawled pages the user defined.
        '''
        print("Crawling...")
        t1 = time.perf_counter()
        while self.crawled_pages < self.max_size:
            # Sleep when there are no available threads or there are no urls to crawl
            while len(self.urls) == 0 or sum([1 for t in self.threads_array if t.is_alive()]) > self.num_threads:
                time.sleep(0.5)
                continue
            #if there are no urls to crawl exit
            if len(self.urls) == 0:
                break
            #Get next url
            next_url = str(self.urls.pop(0))
            #start new thread
            t = threading.Thread(target=self.parse, args=(next_url))
            t.start()
            self.threads_array.append(t)

        t2 = time.perf_counter()
        print("Crawler total execution time: " +
              "{:.2f}".format(t2 - t1) + " secs")
        self.indexer.create_index() #Build indexer after crawler finishes

        
    def parse(self, *url_parse):
        url = "".join(url_parse)
        try:  # check if the reference is valid
            html = request.urlopen(url).read().decode('utf8')
            raw = BeautifulSoup(html, 'html.parser')
            title = raw.title.string
        except Exception:
            return
        try:
            links = []
            # Find all new urls to other pages
            for link in raw.findAll('a'):
            
                if link.get('href')[:4] == 'http':
                    links.append(link.get('href'))
                else:
                    #if the link is links to the same page
                    links.append(url+link.get('href'))
            with self.urlsLocker:  # Add url links
                self.urls += links

            # If this record  exists in database return
            if self.db.crawler_db.find_one({"title": title, "url": url}) != None:
                return

            rx = re.compile("[^\W\d_]+")  # regex for words
            tokens = nltk.word_tokenize(raw.get_text())
            all_words = [
                word for word in tokens if word not in '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'] #exclude exclamation marks and special characters
            all_words = [i[0] for i in [rx.findall(
                i) for i in list(all_words)] if len(i) > 0]
            all_words = [i for i in list(all_words) if not i.startswith("wg")]

            # Remove the stop words from all_words
            filtered_words = []
            for w in all_words:
                if w not in self.stop_words:
                    filtered_words.append(w)
            st = PorterStemmer()   # Stemming to all words
            stemmed_words = [st.stem(word) for word in filtered_words]
            # Convert all words to lowercase
            lowercase_words = [word.lower() for word in stemmed_words]
            with self.countLocker:  # Save the page information to the Database as a new document
                if self.crawled_pages < self.max_size:
                    print("Crawled {counter} documents of {total}...".format(counter=self.crawled_pages + 1,
                                                                             total=self.max_size))
                    self.db.crawler_db.insert_one(
                        {"url": url, "title": title, "bag": Counter(lowercase_words)})
                    self.crawled_pages += 1
        except Exception:  # something went wrong during this phase, so we will not have any results
            return


if __name__ == "__main__":
    url = str(sys.argv[1])  # get variables from commandline
    size = int(sys.argv[2])
    keep_data = int(sys.argv[3])
    threads = int(sys.argv[4])
    if keep_data == 0:
        crawler = Crawler(url=url,
                          keep=False, max_size=size, num_threads=threads)
    else:
        crawler = Crawler(url=url, keep=True,
                          max_size=size, num_threads=threads)

    crawler.crawl()
