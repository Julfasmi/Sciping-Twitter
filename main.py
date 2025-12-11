from utils import logger, task
from core.crawler import TweetCrawler
from core.config import POSTGRE_DB_CONN
from routes import account_router

from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
from fastapi import FastAPI
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = connection_pool = ConnectionPool(conninfo=POSTGRE_DB_CONN, kwargs={"row_factory": dict_row})
    logger.debug({"message": "connection pool connected"})

    app.state.crawler_api = TweetCrawler(pool=app.state.pool)

    task.start_crawler_job()
    yield
    task.shutdown_crawler_job()
    connection_pool.close()


app = FastAPI(lifespan=lifespan)
app.include_router(account_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app")
