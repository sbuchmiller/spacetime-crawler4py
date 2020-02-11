from threading import Thread

from utils.download import download
from utils import get_logger
from scraper import Scrape
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, lock):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.s = Scrape()
        self.frontier_get_id_lock = lock
        self.frontier_get_id_lock.acquire()
        self.domain = self.frontier.get_domain()
        self.frontier_get_id_lock.release()
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url(self.domain)
            if not tbd_url:
                if self.frontier.stop_crawl():
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
                if self.frontier.has_free_domain():
                    self.frontier_get_id_lock.acquire()
                    self.frontier.release_domain(self.domain)
                    self.domain = self.frontier.get_domain()
                    self.frontier_get_id_lock.release()
                else:
                    if self.domain != "FREE":
                        self.frontier.release_domain(self.domain)
                        self.domain = "FREE"
                    time.sleep(3)
            else:
                resp = download(tbd_url, self.config, self.logger)
                self.logger.info(
                    f"Downloaded {tbd_url}, status <{resp.status}>, "
                    f"using cache {self.config.cache_server}.")
                scraped_urls = self.s.scraper(tbd_url, resp)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
                self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
