from google.cloud import bigquery
from google.cloud import logging
from datetime import datetime

import progressbar
import sqlite3
import requests
import os
import json

bigquery_client = bigquery.Client()
daily_prices_table = bigquery_client.get_table(bigquery_client.dataset('markets').table('daily_prices'))

logging_client = logging.Client()
def log_text(text):
    print text
    logging_client.logger('daily_prices_job').log_text(text)

localdb = sqlite3.connect('./storage/sqlite.db')

for stock in localdb.execute('SELECT id, symbol, updated_at FROM stocks').fetchall():
    stock_id = int(stock[0])
    stock_symbol = stock[1]
    stock_updated_at = datetime.strptime(stock[2], '%Y-%m-%d 00:00:00') if stock[2] != None else None
