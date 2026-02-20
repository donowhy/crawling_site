import os
import time
import json
import pymysql
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from notion_client import Client

from queries import CREATE_TABLE, INSERT_QUESTION

# 경로 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(script_dir, '.env'))

class NutrientScraper:
    def __init__(self):
        # DB 설정 로드
        self.db_config = {
            'host': os.getenv("DB_HOST", "127.0.0.1"),
            'port': int(os.getenv("DB_PORT", 3307)),
            'user': os.getenv("DB_USER", "admin"),
            'password': os.getenv("DB_PASSWORD", "adminpassword1!"),
            'db': os.getenv("DB_NAME", "nutrient_analysis"),
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        
        # Notion 설정
        self.notion_token = os.getenv("NOTION_TOKEN")
        self.notion_db_id = os.getenv("NOTION_DATABASE_ID")
        # 노션 설정이 유효한지 간단히 체크
        if self.notion_token and "secret_" in self.notion_token:
            self.notion = Client(auth=self.notion_token)
        else:
            self.notion = None
            print("Notion integration not configured. Sync skipped.")

        self.driver = self._init_driver()
        self.init_db()

    def _init_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument('--ignore-certificate-errors')
        
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)

    def init_db(self):
        """데이터베이스 및 테이블 초기 생성"""
        try:
            # 초기 연결 (DB 지정 없이)
            conn = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_config['db']}")
                cursor.execute(f"USE {self.db_config['db']}")

                cursor.execute(CREATE_TABLE)
            conn.commit()
            conn.close()
            print("DB Initialized.")
        except Exception as e:
            print(f"DB Init Error: {e}")

    def scrape_page(self, q_id):
        site = os.getenv("CRAWLING_SITE")

        url = f"{site}/{q_id}"
        print(f"[{q_id}] Scraping...")
        try:
            self.driver.get(url)
            time.sleep(2)
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            title_el = soup.select_one('h2.ut08sa0')
            if not title_el: return None
            
            title = title_el.get_text(strip=True)
            content_container = soup.select_one('.wmde-markdown')
            content = content_container.get_text("\n", strip=True) if content_container else ""
            
            # 추가 학습 자료 링크 추출
            links = []
            heading = soup.find(lambda t: t.name in ['h2', 'h3'] and "추가 학습 자료" in t.text)
            if heading:
                ul = heading.find_next_sibling('ul')
                if ul:
                    for a in ul.find_all('a'):
                        links.append({'text': a.get_text(strip=True), 'url': a.get('href')})
            
            return {'question_id': q_id, 'title': title, 'content': content, 'additional_links': links}
        except Exception as e:
            print(f"Scrape Error [{q_id}]: {e}")
            return None

    def save_to_db(self, data):
        """데이터를 DB에 저장 (queries.py의 쿼리 사용)"""
        db_data = data.copy()
        db_data['additional_links'] = json.dumps(data['additional_links'], ensure_ascii=False)
        try:
            conn = pymysql.connect(**self.db_config)
            with conn.cursor() as cursor:
                cursor.execute(INSERT_QUESTION, db_data)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"DB Save Error: {e}")

    def save_to_notion(self, data):
        """데이터를 노션에 저장"""
        if not self.notion or not self.notion_db_id: return
        
        try:
            blocks = []
            content_text = data['content']
            for i in range(0, len(content_text), 2000):
                blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"text": {"content": content_text[i:i+2000]}}]}
                })
            
            if data['additional_links']:
                blocks.append({"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "추가 학습 자료"}}]}})
                for link in data['additional_links']:
                    blocks.append({
                        "object": "block", "type": "bulleted_list_item",
                        "bulleted_list_item": {"rich_text": [{"text": {"content": link['text'], "link": {"url": link['url']}}}]}
                    })

            self.notion.pages.create(
                parent={"database_id": self.notion_db_id},
                properties={
                    "Name": {"title": [{"text": {"content": f"[{data['question_id']}] {data['title']}"}}]},
                    "ID": {"number": data['question_id']}
                },
                children=blocks[:100]
            )
            print(f"[{data['question_id']}] Notion Synced.")
        except Exception as e:
            print(f"Notion Error: {e}")

    def run(self, start=49, end=300):
        try:
            for q_id in range(start, end + 1):
                data = self.scrape_page(q_id)
                if data:
                    self.save_to_db(data)
                    self.save_to_notion(data)
                time.sleep(1)
        finally:
            self.driver.quit()

if __name__ == "__main__":
    NutrientScraper().run(49, 300)
