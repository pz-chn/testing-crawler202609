# tasks
# 從 worker.py 匯入 Celery app 實例
# 所有的 task 都要透過 app 來註冊, 才能被 Celery worker 識別
from crawler.worker import app

import feedparser
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from loguru import logger

from crawler.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_ACCOUNT,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

BASE_URL = "https://www.mygopen.com/feeds/posts/default"
MAX_RESULTS = 25


def clean_html(raw_html):
    # 把 RSS 內容裡的 HTML 標籤清掉, 只留下純文字
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ", strip=True)


# @app.task() 是 Celery 的裝飾器 (decorator)
# 有了這個裝飾器, 普通的 Python 函式就會變成「可派送的任務」
@app.task()
def fetch_page(page):
    # 對應 test_mygopen_crawler-0912.py 裡「抓一頁」的邏輯
    start_index = page * MAX_RESULTS + 1
    url = f"{BASE_URL}?start-index={start_index}&max-results={MAX_RESULTS}"

    logger.info(f"page {page}: 開始抓取 {url}")
    feed = feedparser.parse(url)

    articles = []
    for entry in feed.entries:
        articles.append({
            "title": entry.title,
            "published": entry.published,
            "url": entry.link,
            "categories": ", ".join(tag.term for tag in entry.get("tags", [])),
            "content_text": clean_html(entry.content[0].value) if "content" in entry else "",
        })

    if not articles:
        logger.info(f"page {page}: 沒有抓到資料")
        return 0

    # 每個 task 各自把自己這一頁寫進 MySQL, 用 append 累加
    # 不需要 chord, 因為每個 task 寫的是自己的那批資料, 不會互相覆蓋
    df = pd.DataFrame(articles)
    engine = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset=utf8mb4"
)
    df.to_sql("mygopen_articles", con=engine, if_exists="append", index=False)

    logger.info(f"page {page}: 寫入 {len(articles)} 筆資料")
    return len(articles)
