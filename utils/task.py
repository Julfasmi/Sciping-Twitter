from core.crawler import TweetCrawler
from .logging import logger
import main

from concurrent.futures import ThreadPoolExecutor
import threading, datetime as dt
import schedule, os, signal

STOP_FLAG: threading.Event = threading.Event()


def start_crawler_job():
    global STOP_FLAG
    logger.debug({"message": "starting crawler machine"})

    schedule.every(10).seconds.do(running_crawler)

    def run():
        while True:
            if STOP_FLAG.is_set():
                break
            schedule.run_pending()

    thread = threading.Thread(target=run)
    thread.start()


def shutdown_crawler_job():
    logger.debug({"message": "shutdown crawler machine"})
    STOP_FLAG.set()
    schedule.clear()
    os.kill(os.getpid(), signal.SIGKILL)


def running_crawler():
    crawler: TweetCrawler = main.app.state.crawler_api
    keywords = crawler.get_available_keywords()

    def crawl(data: dict):
        crawler: TweetCrawler = main.app.state.crawler_api
        keyword = data["keyword"]
        progress_time: dt.datetime = data["progress_time"]
        return data["id"], crawler.crawl(keyword, progress_time, gap=3600 * 6)

    with ThreadPoolExecutor(max_workers=1) as executor:
        for result in executor.map(crawl, keywords):
            (id, crawl_result) = result
            (tweets, users) = crawl_result["result"]

            logger.debug(
                {
                    "pid": threading.currentThread().native_id,
                    "message": "crawling result",
                    "details": {"tweets": len(tweets), "users": len(users)},
                }
            )

            if tweets:
                crawler.insert_bulk_tweet(tweets)

            if users:
                crawler.insert_bulk_author(users)

            crawler.update_keyword_progress(id, crawl_result["status"])
