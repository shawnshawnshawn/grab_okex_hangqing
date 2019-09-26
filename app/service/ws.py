# coding=utf-8
import json
import logging
import ssl
import zlib
import time
import datetime
import pymongo
import websocket
import threading

from app import settings
from app.service import mongodb

logger = logging.getLogger()

collection = mongodb.get_collection("market_24h_info")
collection_name = "market_kline_info"

data = {
    "tick": {
        "goUp": "",
        "range": "",
        "source": "",
        "close": "",
        "high": "",
        "open": "",
        "low": "",
        "amount": "",
        "vol": "",
        "period": "",
        "pair": "",
        "_id": "",
        "ts": "",
        "ch": ""
    }
}


###
# 通过websocket与火币网实现通信
###
def save_data(msg):
    if settings.DATABASE_RECORD and mongodb:
        try:
            pair = msg["data"][0]["instrument_id"].replace("-", "").lower()
            if msg['table'].find("candle60s") > 0:
                data['tick']['period'] = "1min"
            elif msg['table'].find("candle300s") > 0:
                data['tick']['period'] = "5min"
            elif msg['table'].find("candle900s") > 0:
                data['tick']['period'] = "15min"
            elif msg['table'].find("candle1800s") > 0:
                data['tick']['period'] = "30min"
            elif msg['table'].find("candle3600s") > 0:
                data['tick']['period'] = "60min"
            elif msg['table'].find("candle86400s") > 0:
                data['tick']['period'] = "1day"
            data['tick']['close'] = str(msg["data"][0]['candle'][4])
            data['tick']['high'] = str(msg["data"][0]['candle'][2])
            data['tick']['open'] = str(msg["data"][0]['candle'][1])
            data['tick']['low'] = str(msg["data"][0]['candle'][3])
            data['tick']['amount'] = "0"
            data['tick']['vol'] = str(msg["data"][0]['candle'][5])
            data['tick']['period'] = data['tick']['period']
            data['tick']['pair'] = pair
            t1, t2 = msg['data'][0]['candle'][0].replace("T", " ").split(".")
            ts = int(time.mktime(time.strptime(t1, "%Y-%m-%d %H:%M:%S")))
            data['tick']['ts'] = ts
            data['tick']['_id'] = settings.SOURCE + "-" + pair + "-" + str(ts) + "-" + data['tick']['period']
            data['tick']['ch'] = msg['table']
            data['tick']['source'] = settings.SOURCE
            open_price = float(data['tick']['open'])
            close_price = float(data['tick']['close'])
            det_price = close_price - open_price
            data['tick']['goUp'] = close_price >= open_price
            data['tick']['range'] = str(round(abs(det_price) / open_price * 100, 2))
            data['tick']['time'] = datetime.datetime.utcnow()
            collection1 = mongodb.get_collection(collection_name)
            collection1.save(data['tick'])
        except Exception as exp:
            logger.error("无法保存到数据库：" + str(exp))


def save_1day_market_data(msg):
    """
    1。 保存币价保险中前端显示的交易对24小时行情数据
    2。 处理过后的行情数据，数据结构
    """
    # logger.info("保存币价1Day行情数据信息")
    pair = msg["data"][0]["instrument_id"].replace("-", "").lower()
    t1, t2 = msg['data'][0]['timestamp'].replace("T", " ").split(".")
    ts = int(time.mktime(time.strptime(t1, "%Y-%m-%d %H:%M:%S")))
    open_price = float(msg["data"][0]['open_24h'])
    close_price = float(msg["data"][0]['last'])
    det_price = close_price - open_price
    data['tick']['goUp'] = close_price >= open_price
    data['tick']['range'] = str(round(abs(det_price) / open_price * 100, 2))
    data['tick']['source'] = settings.SOURCE
    data['tick']['_id'] = settings.SOURCE + "-" + pair + "-" + str(ts)
    data['tick']['close'] = msg["data"][0]['last']
    data['tick']['high'] = msg["data"][0]['high_24h']
    data['tick']['open'] = msg["data"][0]['open_24h']
    data['tick']['low'] = msg["data"][0]['low_24h']
    data['tick']['amount'] = msg['data'][0]['base_volume_24h']
    data['tick']['vol'] = msg['data'][0]['quote_volume_24h']
    data['tick']['period'] = "1day"
    data['tick']['pair'] = pair
    data['tick']['ts'] = ts
    data['tick']['ch'] = msg['table']
    data['tick']['time'] = datetime.datetime.utcnow()
    collection.save(data['tick'])


def send_message(ws, msg_dict):
    msg = json.dumps(msg_dict).encode()
    logger.debug("发送消息:" + str(msg_dict))
    ws.send(msg)


###
# okex使用inflate压缩数据，所以需要解压
###
def inflate(message):
    decompress = zlib.decompressobj(
        -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(message)
    inflated += decompress.flush()
    return inflated


def on_message(ws, message):
    unzipped_data = inflate(message)
    msg_dict = json.loads(unzipped_data)
    logging.info(msg_dict)
    if str(msg_dict).__contains__("event"):
        logger.info("收到订阅回复消息: " + str(msg_dict))
    elif str(msg_dict).__contains__("spot/ticker"):
        save_1day_market_data(msg_dict)
        logger.debug("收到消息: " + str(msg_dict))
    else:
        save_data(msg_dict)
        logger.debug("收到消息: " + str(msg_dict))


def on_error(ws, error):
    logger.error(str(error))


def on_close(ws):
    logger.info("已断开连接")
    logger.info("等待重新尝试连接")
    # time.sleep(2)
    start()


def on_open(ws):
    logger.info("ws连接成功")
    threads = []
    t1 = threading.Thread(target=sub, args=(ws,))
    t2 = threading.Thread(target=subKline1min, args=(ws,))
    t3 = threading.Thread(target=subKline5min, args=(ws, ))
    t4 = threading.Thread(target=subKline15min, args=(ws, ))
    t5 = threading.Thread(target=subKline30min, args=(ws, ))
    t6 = threading.Thread(target=subKline1h, args=(ws, ))
    t7 = threading.Thread(target=subKline24h, args=(ws, ))
    threads.append(t1)
    threads.append(t2)
    threads.append(t3)
    threads.append(t4)
    threads.append(t5)
    threads.append(t6)
    threads.append(t7)
    for x in threads:
        x.start()


def sub(ws):
    s = []
    # 遍历settings中的货币对象
    for symbol in settings.SYMBOLS:
        for currency in settings.COINS:
            if symbol != currency:
                subscribe = "{0}:{1}-{2}".format("spot/ticker", currency, symbol)
                s.append(subscribe)
    info = {
        "op": "subscribe",
        "args": s
    }
    # 订阅K线图和24h行情
    send_message(ws, info)


def subKline1min(ws):
    s = []
    for symbol in settings.SYMBOLS:
        for currency in settings.COINS:
            if symbol != currency:
                subscribe = "{0}:{1}-{2}".format("spot/candle60s", currency, symbol)
                s.append(subscribe)
    info = {
        "op": "subscribe",
        "args": s
    }
    send_message(ws, info)


def subKline5min(ws):
    s = []
    for symbol in settings.SYMBOLS:
        for currency in settings.COINS:
            if symbol != currency:
                subscribe = "{0}:{1}-{2}".format("spot/candle300s", currency, symbol)
                s.append(subscribe)
    info = {
        "op": "subscribe",
        "args": s
    }
    send_message(ws, info)


def subKline15min(ws):
    s = []
    for symbol in settings.SYMBOLS:
        for currency in settings.COINS:
            if symbol != currency:
                subscribe = "{0}:{1}-{2}".format("spot/candle900s", currency, symbol)
                s.append(subscribe)
    info = {
        "op": "subscribe",
        "args": s
    }
    send_message(ws, info)


def subKline30min(ws):
    s = []
    for symbol in settings.SYMBOLS:
        for currency in settings.COINS:
            if symbol != currency:
                subscribe = "{0}:{1}-{2}".format("spot/candle1800s", currency, symbol)
                s.append(subscribe)
    info = {
        "op": "subscribe",
        "args": s
    }
    send_message(ws, info)


def subKline1h(ws):
    s = []
    for symbol in settings.SYMBOLS:
        for currency in settings.COINS:
            if symbol != currency:
                subscribe = "{0}:{1}-{2}".format("spot/candle3600s", currency, symbol)
                s.append(subscribe)
    info = {
        "op": "subscribe",
        "args": s
    }
    send_message(ws, info)


def subKline24h(ws):
    s = []
    for symbol in settings.SYMBOLS:
        for currency in settings.COINS:
            if symbol != currency:
                subscribe = "{0}:{1}-{2}".format("spot/candle86400s", currency, symbol)
                s.append(subscribe)
    info = {
        "op": "subscribe",
        "args": s
    }
    send_message(ws, info)


def start():
    ws = websocket.WebSocketApp(
        "wss://okexcomreal.bafang.com:8443/ws/v3",
        # "wss://real.okex.com:10442/ws/v3",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
