import logging

TEST_NET = True
ROBOT_ID = 4
TRADE_INTERVAL = 3
LOG_LEVEL = logging.DEBUG
SERVER_BASE_URL = "http://127.0.0.1:7000/api"
ROBOT_STREAM_WS_URI = "ws://127.0.0.1:7000/robots/{pk}/streams/".format(pk=ROBOT_ID)
TOKEN = ""
DINGTALK_WEBHOOK = ""
DINGTALK_SECRET = ""
ROBOT_STREAM_KEY = ""
