from core.crawler import TweetCrawler
from helper import request
import main

from fastapi import APIRouter
from fastapi.responses import ORJSONResponse


router = APIRouter()


@router.post("/pool/add/account")
async def add_account(data: request.AccountPool):
    crawler: TweetCrawler = main.app.state.crawler_api
    result: bool = await crawler.add_account(data)
    return ORJSONResponse({"code": 0, "content": result})
