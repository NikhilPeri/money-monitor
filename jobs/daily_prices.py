from google.cloud import bigquery
from google.cloud import logging
from datetime import datetime

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

    params = {
        'symbol': stock_symbol,
        'outputsize': 'full' if stock_updated_at == None else 'compact',
        'interval': '15min',
        'apikey': os.environ['STOCK_API_KEY'],
        'function': 'TIME_SERIES_DAILY'
    }
    '''
        ...
        "2017-12-11 16:00:00": {
            "1. open": "85.2200",
            "2. high": "85.2600",
            "3. low": "85.2100",
            "4. close": "85.2300",
            "5. volume": "3601140"
        }
        ...
    '''

    try:
        raw_data = json.loads(requests.get('https://www.alphavantage.co/query', params=params).text)
        data = raw_data['Time Series (Daily)']
        data_refreshed_at = datetime.strptime(raw_data['Meta Data']['3. Last Refreshed'], '%Y-%m-%d')
    except:
        log_text('ERROR: could not find prices for ' + stock_symbol)
        continue

    if len(data) == 0:
        log_text('ERROR: could not find prices for ' + stock_symbol)
        continue

    new_prices = []
    for date_str in data.keys():
        date = datetime.strptime(date_str, '%Y-%m-%d')

        if stock_updated_at != None and date <= stock_updated_at:
            continue

        new_prices.append({
            'stock_id': stock_id,
            'date': date.date(),
            'volume': float(data[date_str]['5. volume']),
            'open': float(data[date_str]['1. open']),
            'low': float(data[date_str]['3. low']),
            'high': float(data[date_str]['2. high']),
            'close': float(data[date_str]['4. close']),
        })

    try:
        bigquery_client.create_rows(daily_prices_table, new_prices)
        log_text('Updated ' + str(len(new_prices)) + ' prices for ' + stock_symbol)
        localdb.execute('UPDATE stocks SET updated_at = ? WHERE id = ?', (str(data_refreshed_at), stock_id))
        localdb.commit()
    except:
        log_text('ERROR: could not write prices for ' + stock_symbol)
