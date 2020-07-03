import asyncio
import json
import logging
import logging.config
import time
from asyncio.queues import Queue as AsyncioQueue
from asyncio.queues import QueueEmpty
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from queue import Queue
from typing import Optional

import websockets
from websockets.client import WebSocketClientProtocol

from bot import settings
from dingtalkchatbot.chatbot import DingtalkChatbot

from .pool import Pool


class DingTalkHandler(logging.Handler):
    """
    将日志推送钉钉
    """

    def __init__(self, webhook_url: str, secret: str, level=logging.NOTSET):
        super().__init__(level=level)
        self.ding_bot = DingtalkChatbot(webhook_url, secret=secret)

    def emit(self, record):
        try:
            msg = self.format(record)
            self.ding_bot.send_text(msg=msg)
        except Exception:
            self.handleError(record)


class WebsocketHandler(logging.Handler):
    def __init__(self, ws_uri: str, level: int = logging.NOTSET):
        super().__init__(level=level)
        self.pool: Pool = Pool(ws_uri=ws_uri)

    async def send(self, s: str):
        sock = await self.pool.acquire()
        try:
            await sock.send(s)
        except websockets.ConnectionClosedError:
            pass
        await self.pool.release(sock)

    def serialize(self, record):
        return json.dumps(self.map_log_record(record))

    def map_log_record(self, record):
        return record.__dict__

    def emit(self, record):
        pass

    async def ahandle(self, record):
        rv = self.filter(record)
        if rv:
            await self.aemit(record)
        return rv

    async def aemit(self, record):
        try:
            await self.send(self.serialize(record))
        except Exception:
            self.handleError(record)

    async def aclose(self):
        pass


class WebsocketListener(object):
    _sentinel = None

    def __init__(self, queue, *handlers, respect_handler_level=False):
        self.queue = queue
        self.handlers = handlers
        self._task = None
        self.respect_handler_level = respect_handler_level

    async def dequeue(self):
        return await self.queue.get()

    def start(self):
        self._task = asyncio.get_event_loop().create_task(self._monitor())

    def prepare(self, record):
        return record

    async def handle(self, record):
        record = self.prepare(record)
        tasks = []
        for handler in self.handlers:
            if not self.respect_handler_level:
                process = True
            else:
                process = record.levelno >= handler.level
            if process:
                tasks.append(handler.ahandle(record))
        if len(tasks) > 0:
            await asyncio.gather(*tasks)

    async def _monitor(self):
        q = self.queue
        has_task_done = hasattr(q, "task_done")
        while True:
            try:
                record = await self.dequeue()
                if record is self._sentinel:
                    if has_task_done:
                        q.task_done()
                    break
                await self.handle(record)
                if has_task_done:
                    q.task_done()
            except QueueEmpty:
                break

    async def enqueue_sentinel(self):
        self.queue.put_nowait(self._sentinel)

    async def stop(self):
        await self.enqueue_sentinel()
        self.queue.join()
        self._task.cancel()
        self._task = None
        tasks = []
        for handler in self.handlers:
            tasks.append(handler.astop())
        if len(tasks) > 0:
            await asyncio.gather(*tasks)


class YuFuRobotStreamWebsocketHandler(WebsocketHandler):
    def map_log_record(self, record):
        return {
            "topic": "log",
            "level": record.levelname,
            "timestamp": int(record.created * 1000),
            "msg": record.msg,
        }


LOG_LOCATION = "./.logs/bot.log"

ws_queue = AsyncioQueue()

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {"format": "%(message)s"},
        "default": {
            "format": "%(asctime)s %(levelname)s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "level": logging.INFO,
            "class": "logging.StreamHandler",
            "formatter": "default",
        },
        "file": {
            "level": logging.DEBUG,
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "filename": LOG_LOCATION,
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 7,
            "encoding": "utf-8",
        },
        "websocket": {
            "level": logging.INFO,
            "class": "logging.handlers.QueueHandler",
            "formatter": "simple",
            "queue": ws_queue,
        },
        "dingtalk": {
            "level": logging.INFO,
            "class": "logging.NullHandler",
            "formatter": "simple",
        },
    },
    "loggers": {
        "local": {"handlers": ["console", "file"], "level": logging.DEBUG},
        "remote": {"handlers": ["console", "file", "websocket"], "level": logging.INFO},
        "notifier": {
            "handlers": ["console", "file", "websocket"],
            "level": logging.INFO,
        },
    },
}


def config_logging():
    if settings.DINGTALK_WEBHOOK and settings.DINGTALK_SECRET:
        dingtalk_queue = Queue()
        DEFAULT_LOGGING["handlers"]["dingtalk"] = {
            "level": logging.INFO,
            "class": "logging.handlers.QueueHandler",
            "queue": dingtalk_queue,
            "formatter": "simple",
        }
        DEFAULT_LOGGING["loggers"]["notifier"]["handlers"] = [
            "console",
            "file",
            "websocket",
            "dingtalk",
        ]
        dingtalk_handler = DingTalkHandler(
            webhook_url=settings.DINGTALK_WEBHOOK, secret=settings.DINGTALK_SECRET
        )
        dingtalk_listener = QueueListener(dingtalk_queue, dingtalk_handler)
        dingtalk_listener.start()

    Path(LOG_LOCATION).parent.mkdir(parents=True, exist_ok=True)
    logging.config.dictConfig(DEFAULT_LOGGING)
    ws_handler = YuFuRobotStreamWebsocketHandler(ws_uri=settings.WS_ROBOT_STREAM_URI)
    ws_listener = WebsocketListener(ws_queue, ws_handler)
    ws_listener.start()


if __name__ == "__main__":
    config_logging()
    logger = logging.getLogger("remote")
    dingtalk_logger = logging.getLogger("dingtalk")

    async def main():
        while True:
            dingtalk_logger.warning("test")
            await asyncio.sleep(10)

    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
