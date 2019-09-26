from app.service import ws


def get():
    data = {'table': 'spot/ticker', 'data': [
        {'instrument_id': 'BCH-USDT', 'last': '407.01', 'best_bid': '406.8', 'best_ask': '407.01', 'open_24h': '401.95',
         'high_24h': '413.79', 'low_24h': '396.52', 'base_volume_24h': '100038.9681',
         'quote_volume_24h': '40611634.6443745', 'timestamp': '2019-07-08T08:09:52.647Z'}]}
    print(data['data'][0]['instrument_id'])


if __name__ == "__main__":
    ws.start()
    # get()