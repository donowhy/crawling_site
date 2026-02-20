import os
import json
import asyncio
import pymysql
from dotenv import load_dotenv
from notion_client import AsyncClient

# 쿼리 임포트
from queries import SELECT_ALL

# 경로 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(script_dir, '.env'))

class NotionSyncer:
    def __init__(self):
        self.db_config = {
            'host': os.getenv("DB_HOST", "127.0.0.1"),
            'port': int(os.getenv("DB_PORT", 3307)),
            'user': os.getenv("DB_USER", "admin"),
            'password': os.getenv("DB_PASSWORD", "adminpassword1!"),
            'db': os.getenv("DB_NAME", "nutrient_analysis"),
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        
        self.notion_token = os.getenv("NOTION_TOKEN")
        self.notion_db_id = os.getenv("NOTION_DATABASE_ID")
        self.notion = AsyncClient(auth=self.notion_token)
        
        # 병렬 작업 제한
        self.semaphore = asyncio.Semaphore(3) 

    def get_data_from_db(self):
        """DB에서 데이터 가져오기 (queries.py의 SELECT_ALL 사용)"""
        try:
            conn = pymysql.connect(**self.db_config)
            with conn.cursor() as cursor:
                # 쿼리 사용
                cursor.execute(SELECT_ALL)
                rows = cursor.fetchall()
            conn.close()
            return rows
        except Exception as e:
            print(f"DB Fetch Error: {e}")
            return []

    async def upload_one_row(self, row):
        """한 행씩 노션으로 업로드"""
        async with self.semaphore:
            q_id = row['question_id']
            title = row['title']
            content_text = row['content']
            additional_links = json.loads(row['additional_links'])
            
            print(f"[{q_id}] Syncing start...")
            try:
                blocks = []
                for i in range(0, len(content_text), 2000):
                    blocks.append({"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": content_text[i:i+2000]}}]}})
                
                if additional_links:
                    blocks.append({"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "추가 학습 자료"}}]}})
                    for link in additional_links:
                        blocks.append({
                            "object": "block", "type": "bulleted_list_item",
                            "bulleted_list_item": {"rich_text": [{"text": {"content": link['text'], "link": {"url": link['url']}}}]}
                        })

                await self.notion.pages.create(
                    parent={"database_id": self.notion_db_id},
                    properties={
                        "Name": {"title": [{"text": {"content": f"[{q_id}] {title}"}}]},
                        "ID": {"number": q_id}
                    },
                    children=blocks[:100]
                )
                print(f"[{q_id}] SUCCESS: Synced to Notion.")
            except Exception as e:
                print(f"[{q_id}] FAILED: {e}")

    async def run_sync(self):
        rows = self.get_data_from_db()
        if not rows:
            print("No data found.")
            return

        print(f"Found {len(rows)} items. Syncing...")
        tasks = [self.upload_one_row(row) for row in rows]
        await asyncio.gather(*tasks)
        print("Completed.")

if __name__ == "__main__":
    asyncio.run(NotionSyncer().run_sync())
