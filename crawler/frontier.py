import os
import shelve
import re
from urllib.parse import urlparse
from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import Scrape
import queue
from collections import defaultdict, Counter
import simhash
class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = defaultdict(set)
        self.s = Scrape(config)
        self.open_domains = set() #keeps track of domains with values in them that aren't being used by a thread
        self.taken_domains = set() #keeps track of domains being used by a thread
        self.simhashes = simhash.SimhashIndex([]) #stores simhashes of previous URLs

        self.subdomains = defaultdict(int) #keeps track of unique subdomains
        self.url_word_count = Counter() #keeps track of the words found in all URL's
        self.max_url = (" ", -1) #keeps track of the biggest URL by word count

        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)



    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and self.s.is_valid(url):
                self.add_to_tbd(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self,id):
        if id == "FREE":
            return None
        if id in self.to_be_downloaded: #uses the id of the thread to give it a url from the set associated with each key which represents a
            temp = None                    #unique domain. Ensures each domain is only called once each politeness window
            if len(self.to_be_downloaded[id]) != 0:
                temp = self.to_be_downloaded[id].pop()
            if len(self.to_be_downloaded[id]) == 0:
                self.to_be_downloaded.pop(id)
            return temp
        return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self.add_to_tbd(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()

    def add_to_tbd(self,url): #takes a url from one worker thread and adds it to the URl dictionary by domain.
        parsed = urlparse(url)
        location = parsed.netloc.lower()
        if (re.match(r".*\.ics.uci.edu.*", location)): #for this project I hardcoded the "domains" however if this was a real crawler I would
            self.to_be_downloaded["ics"].add(url)           #use the domain from the parser to make this expandable to more domains
            key = "ics"                                     #for safety we only use these 5 since they are the ones in the project spec
        elif (re.match(r".*\.cs.uci.edu.*", location)):
            self.to_be_downloaded["cs"].add(url)
            key = "cs"
        elif (re.match(r".*\.informatics.uci.edu.*", location)):
            self.to_be_downloaded["informatics"].add(url)
            key = "informatics"
        elif (re.match(r".*\.stat.uci.edu.*", location)):
            self.to_be_downloaded["stat"].add(url)
            key = "stat"
        elif (re.match(r".*\.today.uci.edu/department/information_computer_sciences.*",  location)):
            self.to_be_downloaded["today"].add(url)
            key = "today"
        else:
            self.to_be_downloaded["other_valid"].add(url) #not entirely neccesary for this project but it's here just in case
            key = "other_valid"
        self.subdomains[location] += 1
        if key not in self.taken_domains:
            self.open_domains.add(key) #adds domain to the open domains if it is the first link found in that domain.


    def stop_crawl(self):       #worker threads ask frontier if they should stop or just idle if they are free
        if len(self.taken_domains) == 0:
            if len(self.open_domains) == 0:
                return True
        return False

    def has_free_domain(self):    #to check if domains are open
        return len(self.open_domains) != 0

    def get_domain(self):   #to assign a domain to a worker thread
        if len(self.open_domains) != 0:
            temp = self.open_domains.pop()
            self.taken_domains.add(temp)
            return temp
        else:
            return "FREE"

    def release_domain(self, domain): #for worker threads to return a domain they no longer need
        if domain in self.taken_domains:
            self.taken_domains.remove(domain)

    def check_simhash(self,simh): #returns whether simh is a near dupe to any stored hashes
        return len(self.simhashes.get_near_dups(simh)) == 0

    def add_simhash(self, link,simhash): #adds a simhash to frontier simhashes, neccesary for multithread
        self.simhashes.add(link,simhash)

    def add_to_counter(self,url_word):
        self.url_word_count += url_word

    def combine_max_urls(self,url): #finds the biggest url out of the biggest each worker found
        if self.max_url[1] < url[1]:
            self.max_url = url

    def print_max_url(self):
        print("\n\nMAX_URL_LENGTH:")
        print(self.max_url)

    def print_subdomains(self): #prints the dictionary of subdomains in alphabetical order
        print("\n\nPRINTING SUBDOMAINS:")
        for url, count in sorted(self.subdomains.items(),key=lambda y: y[0]):
            print(url,count)

    def print_counter(self):
        print("\n\nMOST FREQUENT WORD COUNTER:")
        print(self.url_word_count.most_common(60))

