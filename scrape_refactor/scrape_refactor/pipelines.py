# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3
import datetime as dt


# class ScrapeRefactorPipeline:

#     def __init__(self):
#         self.db_path = "/Users/jai/Documents/code/sports/db/fighter_bfs.db"
#         self.db = db_file = sqlite3.connect(self.db_path)
#         self.db_cur = self.db.cursor()

#     def process_item(self, item, spider):
#         data = [
#             (f"https://www.bestfightodds.com{_url}", dt.datetime.now().isoformat(), None, None) for _url in item["urls"]
#         ]
#         self.db_cur.executemany("INSERT OR IGNORE INTO fighter_bfs VALUES (?, ?, ?, ?)", data)
#         self.db.commit()
#         return item