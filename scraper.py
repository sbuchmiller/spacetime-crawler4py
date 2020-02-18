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
import re

class Scrape():
    def __init__(self,config, worker = None):
        self.config = config
        self.host,self.port = config.cache_server
        #self.robots = list of banned paths
        self.robots = {}
        self.simhashes = SimhashIndex([])
        self.link = 1
        self.worker = worker
        self.maxWords = ("",0) # maxWords[0] is the URL, maxWords[1] is the number of words in it
        self.wordCounter = Counter()  # a dictionary that keeps track of the # of words 
        self.stopWords = ['1', 'a', 'about', 'above', 'after', 'again', 'against', 'all', 'also', 'am', 'an', 'and', 'any', 'are', 'are', 
                        "aren't", 'as', 'at','b', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'can', 
                        'can', "can't", 'cannot', 'could', "couldn't", 'd', 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 
                        'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', 'has', "hasn't", "hasn't", 
                        'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'herself', 'him', 'himself', 'his', 'how', 
                        "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', 
                        "let's", "ll", 'm', 'may', 'me', 'more', 'most', "mustn't", 'my', 'myself', 'next', 'no', 'nor', 'not', 'of', 'off', 'on', 
                        'once', 'once', 'one', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 's', 
                        'same', 'say', 'says', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 
                        't', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 
                        'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'under', 'until', 
                        'until', 'up', 've', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', 
                        "what's", 'when', "when's", 'where', 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'will', 'with', "won't", 
                        'would', "wouldn't", 'x', 'y', 'you', "you'd", "you'll", "you're", "you've", 'your', 'yourself', 'yourselves']
    def scraper(self,url:str, resp: utils.response.Response) -> list:
        links = self.extract_next_links(url,resp)
        return links


    def extract_next_links(self,url, resp) -> list:
        
        # blacklist contains a list of html tags and meta tags
        blackList = ['[document]', 'noscript', 'header', 'html', 'meta', 'head', 'input', 'script', 'style', 'b', 'button', '.comment-meta',
        '.comment-content', '.comment-metadata', '.comment-body', '.comment-author', '#comment','.comment-area', 'secondary', '#main', '#site-logo',
        '#container',"#primary"]

        links = set() # make it a set so it checks duplicates after removing the fragment
        if (200 <= resp.status <= 599)  and resp.status != 204:
            soup = BeautifulSoup(resp.raw_response.content, "lxml")

            if resp.status == 200 and soup.prettify() == '':  # avoid dead URLs that return a 200 status but no data
                return []
            
            # concatenate each word to the string output
            output = " "
            
            # the content of each URL 
            text = soup.find_all(text=True)

            # check if each word in the content is meta tags or html 
            for t in text:
                if t.parent.name not in blackList: 
                    output += '{} '.format(t)
            
           
            #Sim hashes 
            simh = Simhash(output)

            if not self.worker.check_simhash(simh):
                return []
            else:
                for link in soup.findAll('a'):
                   if self.is_valid(link.get('href')):

                        #Parses the page for amount of words

                        # a list of words for the max length words with stop words
                        output_list = self.wordParser(output)

                        # a list of words with no stop words that stores the frequncies of each word (common word)
                        output_list_without_stop_words = self.remove_stopwords(output_list)

                        # update the Counter Dictionary
                        self.wordParserForCounter(output_list_without_stop_words)

                        # update self.MaxWords tuple
                        self.updateMaxWords(self.wordParser(output),url)

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
            
            # check if the path has the patterns
            if(re.match(not_crawling_patterns, parsed.path.lower()) or re.match(not_crawling_path_patterns, parsed.path.lower())):  
                # ex: https://www.informatics.uci.edu/files/pdf/InformaticsBrochure-March2018
                    return False

            # also need to check if the query has the patterns
            if(re.match(not_crawling_patterns, parsed.query.lower())):  
                # ex: http://sli.ics.uci.edu/Classes/2011W-178?action=download&upname=HW2.pdfcle
                return False

            if(re.match(
                r".*\.ics\.uci\.edu\/?.*|.*\.cs\.uci\.edu\/?.*|.*\.informatics\.uci\.edu\/?.*|.*\.stat\.uci\.edu\/?.*"
                + r"|today\.uci\.edu\/department\/information_computer_sciences\/?.*$"
                ,parsed.netloc.lower() )):

                if (len(parsed.geturl()) <= 200):  # any links bigger than 200 will be discarded

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
    
    # take a string of words and parse it into a list of tokens
    def wordParser(self, text:str):
        array = text.split()

        array1 = []
        for word in array:         
            if re.match("\w+\W+\w+", word):
                array1.extend(re.split("\W+", word))
            elif re.match("^\W+$", word):
                pass
            elif re.match("^\w+\W+$", word):
                array1.append(re.search("^(\w+)(\W+)$", word).group(1))
            elif re.match("\w*[^a-zA-Z0-9]+\w*[^a-zA-Z0-9]*", word):
                array1.extend(re.split("[^a-zA-Z0-9]+", word))
            else:
                array1.append(word)

        for i in range(len(array1)):
            if array1[i].lower() != array1[i]:
                array1[i] = array1[i].lower()

        return " ".join(array1).split()

    # remove all the stop words in the list
    def remove_stopwords(self, l:list) -> list: 
            arr2 = []
            for word in l:
                if word not in self.stopWords and len(word) != 1: # if the word is not a single character or a number 
                    arr2.append(word)
            return arr2

    # update the longest page in terms of number of words    
    def updateMaxWords(self,l:list,url):
        if (len(l) > self.maxWords[1]):
            self.maxWords = (url, len(l))
    
    # get the data from the list of words and store into a dictionary (no stopWords)
    def wordParserForCounter(self,l:list):
        for x in l :
            self.wordCounter[x] += 1


    def getWords(self):
        return self.maxWords
    
    def getWordCounter(self):
        return self.wordCounter
    



        

