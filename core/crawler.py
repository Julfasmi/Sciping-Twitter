from helper.request import AccountPool
from utils import logger

from psycopg_pool import ConnectionPool
import psycopg, twscrape, datetime as dt
import time, asyncio, os, pandas as pd
import re, numpy as np


class TweetCrawler:
    crawler_api: twscrape.API
    logs_dir: str

    def __init__(self, pool: ConnectionPool = None):
        self.crawler_api = twscrape.API(debug=True)
        self.logs_dir = "crawling_logs"
        self.pool = pool

    async def add_account(self, data: AccountPool):
        await self.crawler_api.pool.add_account(
            username=data.username,
            password=data.password,
            email=data.email,
            email_password=data.email_pass,
        )
        current_account = await self.crawler_api.pool.get(data.username)
        result = await self.crawler_api.pool.login(current_account)
        if result:
            return result
        else:
            await self.crawler_api.pool.delete_accounts(data.username)
            return result

    def crawl(self, keywords: str, since_time: dt.datetime, gap: int = 60, lang: str = "id"):
        tzinfo = since_time.tzinfo
        since_timestamp = time.mktime(since_time.timetuple())
        now_timestamp = time.mktime(dt.datetime.now(tzinfo).timetuple())

        assert since_timestamp + gap < now_timestamp, (
            f"{since_timestamp+gap} > {now_timestamp}" + " Current Time is Overlap with until time search"
        )

        until_time = self.format_timestamp(since_timestamp + gap, tzinfo)
        since_time = self.format_timestamp(since_timestamp, tzinfo)

        query = f'"{keywords}" since:{since_time} until:{until_time} lang:{lang}'
        logger.debug({"message": "crawling", "details": query})

        tweets: list[twscrape.Tweet] = asyncio.run(twscrape.gather(self.crawler_api.search(query)))

        if tweets:
            self.save_crawling_history(tweets, f"crawling_history/{since_time}.json")

        users_tweets = []
        cleaned_tweets = []

        for tweet in tweets:
            regex_ads = r"(ads)|(advertiser)"
            is_ads = [
                *re.findall(regex_ads, tweet.sourceUrl.lower()),
                *re.findall(regex_ads, tweet.sourceLabel.lower()),
                *re.findall(regex_ads, tweet.source.lower()),
            ]
            if not is_ads:
                pass
            else:
                logger.warning({"message": "SKIP because ADS"})
                continue
            twtype = "tweet"

            if tweet.quotedTweet:
                twtype = "retweet"
            if tweet.inReplyToUser and tweet.inReplyToTweetId:
                twtype = "replied_to"

            cleaned_tweets.append(
                [tweet.id_str, tweet.rawContent, tweet.user.id_str, tweet.sourceLabel, tweet.json(), tweet.date, twtype]
            )

            users_tweets.append(
                [tweet.user.id_str, tweet.user.username, tweet.user.displayname, tweet.user.json(), tweet.user.created]
            )

        return {
            "status": dt.datetime.fromtimestamp(since_timestamp + gap, tzinfo),
            "result": (cleaned_tweets, users_tweets),
        }

    @classmethod
    def format_timestamp(self, timestamp: float, tzinfo: dt.tzinfo = dt.timezone.utc):
        timestamp: dt.datetime = dt.datetime.fromtimestamp(timestamp, tzinfo)
        return timestamp.strftime("%Y-%m-%d_%H:%M:%S_UTC")

    def save_crawling_history(self, data: list[twscrape.Tweet], outfile: str):
        pathdir, filename = os.path.normpath(outfile).split(os.sep)
        if not os.path.exists(pathdir):
            os.makedirs(pathdir, exist_ok=True)
        pd.DataFrame(data).to_json(outfile, orient="records", indent=2)

    def get_available_keywords(self):
        QUERY = 'SELECT P.id, P.keyword, P.progress_time FROM "Project" as P WHERE P.progress_time < P.until_time;'
        try:
            with self.pool.connection() as conn:
                conn: psycopg.Connection
                with conn.cursor() as cur:
                    cur.execute(QUERY)
                    return cur.fetchall()
        except psycopg.Error as e:
            logger.error({"message": e})

    def update_keyword_progress(self, id, progress_time):
        QUERY = 'UPDATE "Project" SET progress_time = %s WHERE id = %s'
        try:
            with self.pool.connection() as conn:
                conn: psycopg.Connection
                with conn.cursor() as cur:
                    cur.execute(QUERY, (progress_time, id), prepare=False)
        except psycopg.Error as e:
            logger.error({"message": e})

    def insert_bulk_author(self, data: list):
        QUERY = (
            'INSERT INTO public."Tweet_Author"('
            + ' author_id, username, display_name, "raw", joined_at)'
            + " VALUES"
            + " (%s, %s, %s, %s, %s)," * (len(data) - 1)
            + " (%s, %s, %s, %s, %s)"
            + " ON CONFLICT (author_id) DO NOTHING;"
        )

        data = np.array(data).flatten()
        try:
            with self.pool.connection() as conn:
                conn: psycopg.Connection
                with conn.cursor() as cur:
                    cur.execute(QUERY, (*data,), prepare=False)
        except psycopg.Error as e:
            logger.error({"message": e})

    def insert_bulk_tweet(self, data: list):
        QUERY = (
            'INSERT INTO public."Tweets"('
            + " tweet_published_id, content, author_id,"
            + ' source_label, "raw", tweet_published_at, type)'
            + " VALUES"
            + " (%s, %s, %s, %s, %s, %s, %s)," * (len(data) - 1)
            + " (%s, %s, %s, %s, %s, %s, %s)"
            + " ON CONFLICT (tweet_published_id, tweet_published_at)"
            + " DO NOTHING;"
        )
        # + " DO UPDATE"
        # + " content = EXCLUDED.content, "
        # + ' "raw" = EXCLUDED."raw";'

        data = np.array(data).flatten()
        try:
            with self.pool.connection() as conn:
                conn: psycopg.Connection
                with conn.cursor() as cur:
                    cur.execute(QUERY, (*data,), prepare=False)
        except psycopg.Error as e:
            logger.error({"message": e})
