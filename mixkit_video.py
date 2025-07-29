#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/3/13 17:36
# @Author  : CUI liuliu
# @File    : mixkit_video.py



import logging
import requests
from lxml import etree
import mysql.connector
from datetime import datetime
import time
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mixkit_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ====== 配置区（请在此处修改采集/下载/数据库等参数）======
SAVE_DIR = r"Z:\李建楠"  # 视频保存目录
KEYWORD = "推进"  # 采集关键词
START_PAGE = 1  # 起始页码
END_PAGE = 60   # 结束页码
DB_CONFIG = {
    'user': 'root',
    'password': 'zq828079',
    'host': '192.168.10.70',
    'database': 'data_sql'
}
# ====== 配置区结束 ======

# keywords = [
    
#     # "4K"
#     # "first personal view",
#     # "fixed",
#     # "Pan",
#     # "zoom out",
#     # "zoom in",
#     # "push in",
#     # "pull out",
#     # "hand-held",
#     # "low angle",
#     # "tacking",
#     # "around",
#     # "Birds-Eye-View Shot Overhead shot",
#     # "over the shoulder",
#     # "Ground camera",
#     # "macro",
#     # "translation",
#     # "360 degree lens",
#     # "time-lapse photography",
#     # "slow motion",
#     # "Tilt",
#     # "Drone perspective"
# ]
page_start=1
# ==================== 请求头配置 ====================
HEADERS = {
        'Accept': 'text/html, application/xhtml+xml',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Cookie': '__cf_bm=Drw.OSEzMf4_UL2P64Du_QQMs1ioczHzrLMgHSu4hkg-1741859330-1.0.1.1-OO5mcPnnNt7H8U_ICfSzXwVyDc8knqYGBrg3N0VFi3nVfycqHO4wlMB4ud4KST.DMKFCpYAAYZ6xH60VQDsPx9wrahUg99Rc5chkpxDsMLA; CookieConsent={stamp:%27hJEdgU/jEFEJFFhX0xA2+tJffzrTG9PBRXlVW8uhbrskpnk5ALWObw==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:1%2Cutc:1741859336134%2Cregion:%27ca%27}; algolia-user-token=7fc65b27483e47bae47139ba46db98dc; _ga=GA1.1.2020846797.1741859339; _fbp=fb.1.1741859339150.553994311304994010; _ga_HD6V8WBY2G=GS1.1.1741859339.1.1.1741859406.0.0.0',
        'Priority': 'u=1, i',
        'Referer': 'https://mixkit.co/free-stock-video/discover/zoom-in/',
        'Sec-CH-UA': '"Chromium";v="134", "Not:A-Brand";v="24", "Microsoft Edge";v="134"',
        'Sec-CH-UA-Mobile': '?0',
        'Sec-CH-UA-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0'
    }


def init_database():
    """初始化数据库表结构"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS mixkit_videos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                video_url VARCHAR(512) UNIQUE,
                title VARCHAR(255),
                download_link VARCHAR(512),
                download_state BOOLEAN DEFAULT FALSE,
                keywords VARCHAR(255) DEFAULT NULL,
                save_path VARCHAR(512) DEFAULT NULL,
                datal_id VARCHAR(64) DEFAULT NULL,
                created_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        logger.info("Database initialized successfully")
    except mysql.connector.Error as e:
        logger.error(f"Database initialization failed: {str(e)}")
        raise
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def create_mixkit_videos_two_table():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS mixkit_videos_two (
                id INT AUTO_INCREMENT PRIMARY KEY,
                video_name VARCHAR(255) NOT NULL,
                category VARCHAR(50),
                tags TEXT,
                datal_id VARCHAR(50) DEFAULT NULL,
                download_state BOOLEAN DEFAULT FALSE,
                download_link VARCHAR(255) UNIQUE,
                save_path VARCHAR(255) DEFAULT NULL
            )
        ''')
        conn.commit()
        logger.info("新表 mixkit_videos_two 创建/验证成功")
    except mysql.connector.Error as e:
        logger.error(f"新表 mixkit_videos_two 创建失败: {str(e)}")
        raise
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def process_download_link(original_src):
    """处理下载链接生成高清版本"""
    try:
        filename = original_src.split("/")[-1]
        base_part = filename.split("-")[0]
        new_filename = f"{base_part}-2160.mp4"
        return original_src.replace(filename, new_filename)
    except Exception as e:
        logger.error(f"Error processing download link: {str(e)}")
        return None


def save_to_database(data):
    """保存数据到数据库"""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO mixkit_videos 
            (video_url, title, download_link, keywords)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            title=VALUES(title), 
            download_link=VALUES(download_link),
            keywords=VALUES(keywords)
        """

        cursor.execute(insert_query, (
            data['video_url'],
            data['title'],
            data['download_link'],
            data['keywords']
        ))

        conn.commit()
        logger.info(f"Inserted/Updated record: {data['video_url']}")
        return True
    except mysql.connector.Error as e:
        logger.error(f"Database operation failed: {str(e)}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


def scrape_page(url,keyword):
    """抓取单个页面"""
    try:
        logger.info(f"Start scraping page: {url}")
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()

        tree = etree.HTML(response.text)
        video_items = tree.xpath('//div[@class="item-grid__item"]')
        logger.info(f"Found {len(video_items)} video items")

        results = []
        detail_urls = []

        for item in video_items:
            try:
                video_tag = item.xpath('.//div[3]/a')[0]
                video_url = "https://mixkit.co" + video_tag.get('href')
                title = video_tag.text.strip()

                video_element = item.xpath('.//div[2]/video')[0]
                original_src = video_element.get('src')

                download_link = process_download_link(original_src)
                if not download_link:
                    continue

                data = {
                    'video_url': video_url,
                    'title': title,
                    'download_link': download_link,
                    'keywords':keyword
                }

                if save_to_database(data):
                    results.append(data)
                    detail_urls.append(video_url)

                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error processing item: {str(e)}")
                continue

        # 并发下载所有详情页视频
        def safe_download(url):
            try:
                return download_video_from_detail(url, SAVE_DIR, logger)
            except Exception as e:
                logger.error(f"下载任务异常: {url}，原因: {e}")
                logger.error(traceback.format_exc())
                return False

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(safe_download, url) for url in detail_urls]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"下载线程异常: {e}")
                    logger.error(traceback.format_exc())

        return results

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        return []
    except etree.XPathError as e:
        logger.error(f"XPath parsing error: {str(e)}")
        return []


def download_video_from_detail(detail_url, save_dir, logger):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Referer": "https://mixkit.co/"
    }
    try:
        resp = requests.get(detail_url, headers=headers, timeout=20)
        resp.raise_for_status()
        html = resp.text
        tree = etree.HTML(html)
        mp4_url = ''
        # 1. 先找 <video src="...">
        video_srcs = tree.xpath('//video/@src')
        if video_srcs:
            mp4_url = video_srcs[0]
        # 2. 再找 <source src="...">
        if not mp4_url:
            video_srcs = tree.xpath('//source[@type="video/mp4"]/@src')
            if video_srcs:
                mp4_url = video_srcs[0]
        # 3. 再用正则找所有.mp4
        if not mp4_url:
            m = re.search(r'https?://[^"\']+\.mp4', html)
            if m:
                mp4_url = m.group(0)
        # 4. 再找js变量里.mp4
        if not mp4_url:
            m = re.search(r'"(https?://[^"]+\.mp4)"', html)
            if m:
                mp4_url = m.group(1)
        if not mp4_url:
            logger.error(f"未能解析出视频下载地址: {detail_url}")
            return False
        # 下载视频
        file_name = mp4_url.split('/')[-1].split('?')[0]
        save_path = os.path.join(save_dir, file_name)
        logger.info(f"开始下载: {mp4_url} -> {save_path}")
        with requests.get(mp4_url, headers=headers, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        logger.info(f"下载成功: {save_path}")
        return True
    except Exception as e:
        logger.error(f"采集或下载失败: {detail_url}，原因: {e}")
        return False


if __name__ == "__main__":
    init_database()
    create_mixkit_videos_two_table()
    for i in range(START_PAGE, END_PAGE + 1):
        target_url = f"https://mixkit.co/free-stock-video/{KEYWORD}/?page={i}"
        logger.info(f"Start scrapy page{i}")

        start_time = datetime.now()
        scraped_data = scrape_page(target_url, KEYWORD)
        duration = datetime.now() - start_time

        logger.info(f"Scraping completed. Total items: {len(scraped_data)}. Time taken: {duration}")