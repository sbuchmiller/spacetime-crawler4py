import re
from urllib.parse import urlparse
import utils.response
from bs4 import BeautifulSoup
from urllib.parse import urldefrag
from simhash import Simhash, SimhashIndex #simhash code retrieved from pip, found at https://github.com/leonsim/simhash
import requests
from utils.response import Response
import cbor
import time
from collections import defaultdict 
from collections import Counter
from string import punctuation

import urllib.request
from urllib.error import HTTPError, URLError
import socket
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

class Scrape():
    def __init__(self,config, worker = None):
        self.config = config
        self.host,self.port = config.cache_server
        #self.robots = list of banned paths
        self.robots = {}
        self.simhashes = SimhashIndex([])
        self.link = 1
        self.worker = worker
        self.maxWords = tuple("", 0)
        self.wordCounter = Counter() 
        self.stopWords = ["a", "about" ,"above", "after", "again","against","all","am","an","and","any","are","are", "aren't","as","at","be","because","been","before","being","below","between","both","but","by"
                        ,"can't", "cannot","could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from","further"
                        ,"had","hadn't", "has","hasn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","isn't","it","it's","its","itself"
                        ,"let's","me","more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only","once","or","other","ought","our","ours"
                        ,"ourselves","out","over","own","same","shan't","she","she'd","she'll","she's","shouldn't","should","so","some","such","than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd"
                        ,"they'll","they're","they've","this","those","through","to","too","under","until","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","where","weren't","what","what's","when","when's","which","while"
                        ,"who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","yourself","your","you've","yourselves"]
    def scraper(self,url:str, resp: utils.response.Response) -> list:
        links = self.extract_next_links(url,resp)
        return links


    def extract_next_links(self,url, resp) -> list:
        
        blackList = ['[document]', 'noscript', 'header', 'html', 'meta', 'head', 'input', 'script', 'style', 'b', 'button']

        links = set() # make it a set so it checks duplicates after removing the fragment
        if (200 <= resp.status <= 599)  and resp.status != 204:
            soup = BeautifulSoup(resp.raw_response.content, "lxml")

            if resp.status == 200 and soup.prettify() == '':  # avoid dead URLs that return a 200 status but no data
                return []
            
            output = " "

            text = soup.find_all(text=True)

            for t in text:
                if t.parent.name not in blackList:
                    output += '{} '.format(t)
            
            #Parses the page for amount of words
            self.wordParser(output,url)
            #Sim hashes 
            simh = Simhash(output)

            if not self.worker.check_simhash(simh):
                return []
            else:
                for link in soup.findAll('a'):
                   if self.is_valid(link.get('href')):
                        # remove the fragment here
                       unfragmented = urldefrag(link.get('href'))
                       links.add(unfragmented.url)
            if self.worker != None:
                self.worker.add_simhash(self.link,simh)
                self.link += 1
            return list(links)
        return list(links)
        
    def is_valid(self,url):
        try:
            parsed = urlparse(url)
            if parsed.scheme not in set(["http", "https"]):
                return False

            not_crawling_patterns = (r".*\.(css|js|bmp|gif|jpe?g|ico"
                                     r"|png|tiff?|mid|mp2|mp3|mp4"
                                     r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                                     r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
                                     r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                                     r"|epub|dll|cnf|tgz|sha1"
                                     r"|thmx|mso|arff|rtf|jar|csv"
                                     r"|rm|smil|wmv|swf|wma|zip|rar|gz)$")

            not_crawling_path_patterns = (r".*/?(css|js|bmp|gif|jpe?g|ico"
                                     r"|png|tiff?|mid|mp2|mp3|mp4"
                                     r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                                     r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
                                     r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                                     r"|epub|dll|cnf|tgz|sha1"
                                     r"|thmx|mso|arff|rtf|jar|csv"
                                     r"|rm|smil|wmv|swf|wma|zip|rar|gz).*$")

            if(re.match(not_crawling_patterns, parsed.path.lower()) or re.match(not_crawling_path_patterns, parsed.path.lower())):  # check if the path has the patterns
                # ex: https://www.informatics.uci.edu/files/pdf/InformaticsBrochure-March2018
                    return False

            if(re.match(not_crawling_patterns, parsed.query.lower())):  # also need to check if the query has the patterns
                # ex: http://sli.ics.uci.edu/Classes/2011W-178?action=download&upname=HW2.pdfcle
                return False

            if(re.match(
                r".*\.ics\.uci\.edu\/?.*|.*\.cs\.uci\.edu\/?.*|.*\.informatics\.uci\.edu\/?.*|.*\.stat\.uci\.edu\/?.*"
                + r"|today\.uci\.edu\/department\/information_computer_sciences\/?.*$"
                ,parsed.netloc.lower() )):

            	

                
                if (len(parsed.geturl()) <= 200):  # any links bigger than 200 will be discarded
                    '''
                        # when it is taking too long (over 10 seconds) to crawl the URL, the cralwer will not crawl
                        try:
                            # set the timeout value to 10 seconds
                            response = urllib.request.urlopen(url, timeout=10)
                        except (HTTPError, URLError) as error:
                            print('{} is not retrieved because it\'s taking too long'.format(url))
                            return False
                    '''

                    #code from utils.download to download and parse the robot
                    #assumes that the URL is a new URL
                    if(not f"{parsed.netloc}" in self.robots.keys()):
                        #print("New Robot", url)
                        resp = requests.get(
                                f"http://{self.host}:{self.port}/",
                                params=[("q", f"{parsed.scheme}://{parsed.netloc}/robots.txt"), ("u", f"{self.config.user_agent}")], timeout = 5)
                        #might have to check what type of respons we're getting.

                        if resp:
                            x = Response(cbor.loads(resp.content))
                            try:
                                user_perm = self.robot_parser(x.raw_response.content.decode())
                                #print(user_perm)
                                #adding the banned paths to the dictionary.
                                self.robots[f"{parsed.netloc}"] = user_perm
                            except:
                                #print("Dead Link")
                                time.sleep(self.config.time_delay)
                                return True
                        else:
                            time.sleep(self.config.time_delay)
                    #Checks if the path is one we're allowed in crawl
                    if (f"{parsed.path}" in self.robots[f"{parsed.netloc}"]):
                        print("Check Robot: invalid", url)
                        return False
                    else:
                        #print("Check Robot: Valid", url)
                        return True
                
                return False


        except TypeError:
            print ("TypeError for ", parsed)
            raise
            
    #Updates self.robot with domains and disallows.
    #the values of the list starts with /
    def robot_parser(self,robot:str) -> list:
        lines = robot.splitlines()
        curr_agent = self.config.user_agent
        #temporary agent - permission
        user_perm = defaultdict(list)
        for i in range(len(lines)):
            words = lines[i].split()
            if(len(words) != 0):
                if(words[0].lower() == "user-agent:"):
                    curr_agent = words[1] 
                elif(words[0].lower() == "disallow:"):
                    user_perm[curr_agent].append(words[1])
                # elif(words[0].lower() == "allow:"):
                #     user_perm[curr_agent].append("+" + words[1])
        if (self.config.user_agent in user_perm.keys()):
            return user_perm[self.config.user_agent]
        else:
            return user_perm["*"]
    
    #parses The words from the pages to get the data for the final report.
    def wordParser(self, text:str, url):
        words = 0
        for x in text.split():
            x = x.rstrip(punctuation).lower()
            if(x not in self.stopWords):
                self.wordCounter[x] += 1
            words += 1
        if (words > self.maxWords[1]):
            self.maxWords = tuple(url, words)

    def getWords(self):
        return self.maxWords
    
    def getWordCounter(self):
        return self.wordCounter
    



        

