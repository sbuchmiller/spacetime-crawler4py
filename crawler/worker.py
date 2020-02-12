from threading import Thread

from utils.download import download
from utils import get_logger
from scraper import Scrape
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, id_lock, add_lock, url_lock, sim_lock):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.s = Scrape(config, self)
        self.get_id_lock = id_lock
        self.add_lock = add_lock
        self.get_url_lock = url_lock
        self.sim_lock = sim_lock
        self.get_id_lock.acquire()
        self.domain = self.frontier.get_domain()
        self.get_id_lock.release()
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            self.get_url_lock.acquire()
            tbd_url = self.frontier.get_tbd_url(self.domain)
            self.get_url_lock.release()
            if not tbd_url:
                if self.frontier.stop_crawl():
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
                if self.frontier.has_free_domain():
                    self.get_id_lock.acquire()
                    self.frontier.release_domain(self.domain)
                    self.domain = self.frontier.get_domain()
                    self.get_id_lock.release()
                else:
                    if self.domain != "FREE":
                        self.frontier.release_domain(self.domain)
                        self.domain = "FREE"
                    self.logger.info("sleeping, cant find domain")
                    time.sleep(6) #sleeps the thread if theres no more domains with urls (they check again after the delay)
            else:
                resp = download(tbd_url, self.config, self.logger)
                self.logger.info(
                    f"Downloaded {tbd_url}, status <{resp.status}>, "
                    f"using cache {self.config.cache_server}.")
                scraped_urls = self.s.scraper(tbd_url, resp)
                self.add_lock.acquire()
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
                self.frontier.mark_url_complete(tbd_url)
                self.add_lock.release()
            time.sleep(self.config.time_delay)

    def check_simhash(self,simhash):
        self.sim_lock.acquire()
        ret = self.frontier.check_simhash(simhash)
        self.sim_lock.release()
        return ret


    def add_simhash(self,link,simhash):
        self.sim_lock.acquire()
        self.frontier.add_simhash(link,simhash)
        self.sim_lock.release()
