from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
from threading import Lock
class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.get_id_lock = Lock()
        self.add_lock = Lock()
        self.get_url_lock = Lock()
        self.sim_lock = Lock()
        self.print_results_lock = Lock()
        self.worker_factory = worker_factory

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self.get_id_lock, self.add_lock, self.get_url_lock, self.sim_lock, self.print_results_lock)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
        self.frontier.print_subdomains()
        self.frontier.print_counter()
        self.frontier.print_max_url()
