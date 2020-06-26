import asyncio
import json
import logging
import logging.config
import time
from asyncio.queues import Queue as AsyncioQueue
from asyncio.queues import QueueEmpty
from logging.handlers import QueueListener
from pathlib import Path
from queue import Queue
from typing import Optional

import websockets
from websockets.client import WebSocketClientProtocol

from bot import settings
from dingtalkchatbot.chatbot import DingtalkChatbot


class DingTalkHandler(logging.Handler):
    def __init__(self, webhook, secret, level=logging.NOTSET):
        super().__init__(level=level)
        self.ding_bot = DingtalkChatbot(webhook, secret=secret)

    def emit(self, record):
        try:
            msg = self.format(record)
            self.ding_bot.send_text(msg=msg)
        except Exception:
            self.handleError(record)


class WebsocketHandler(logging.Handler):
    def __init__(
        self, ws_uri: str, retry_interval: int = 5, level: int = logging.NOTSET
    ):
        super().__init__(level=level)
        self.ws_uri: str = ws_uri
        self.sock: Optional[WebSocketClientProtocol] = None
        self.retry_interval: int = retry_interval
        self.retry_time: Optional[int] = None

    def get_ws_uri(self):
        return self.ws_uri

    async def connect(self, timeout: int = 1) -> WebSocketClientProtocol:
        result = await websockets.connect(self.get_ws_uri(), timeout=timeout)
        return result

    async def create_sock(self):
        now = time.time()
        if self.retry_time is None:
            attempt = True
        else:
            attempt = now >= self.retry_time

        if attempt:
            try:
                self.sock = await self.connect()
                self.retry_time = None
            except (websockets.InvalidURI, websockets.InvalidHandshake, OSError):
                self.retry_time = now + self.retry_interval

    async def send(self, s: str):
        if self.sock is None:
            await self.create_sock()

        if self.sock:
            try:
                await self.sock.send(s)
            except websockets.ConnectionClosedError:
                self.sock = None

    def serialize(self, record):
        return json.dumps(self.map_log_record(record))

    def map_log_record(self, record):
        return record.__dict__

    async def ahandle(self, record):
        rv = self.filter(record)
        if rv:
            await self.aemit(record)
        return rv

    def emit(self, record):
        pass

    async def aemit(self, record):
        try:
            await self.send(self.serialize(record))
        except Exception:
            self.handleError(record)

    async def aclose(self):
        if self.sock:
            await self.sock.close()
            self.sock = None


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
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._monitor())

    def prepare(self, record):
        return record

    async def handle(self, record):
        record = self.prepare(record)
        for handler in self.handlers:
            if not self.respect_handler_level:
                process = True
            else:
                process = record.levelno >= handler.level
            if process:
                await handler.ahandle(record)

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
        for handler in self.handlers:
            await handler.astop()


class FisherRobotStreamWebsocketHandler(WebsocketHandler):
    def __init__(self, **kwargs):
        stream_key = kwargs.pop("stream_key")
        self.stream_key = stream_key
        super().__init__(**kwargs)

    def get_ws_uri(self):
        ws_uri = super().get_ws_uri()
        return ws_uri + "?" + "stream_key=" + self.stream_key

    def map_log_record(self, record):
        return {
            "topic": "log",
            "level": record.levelname,
            "timestamp": int(record.created * 1000),
            "msg": record.msg,
        }


log_location = "./.logs/fisher.log"
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
            "level": settings.LOG_LEVEL,
            "class": "logging.StreamHandler",
            "formatter": "default",
        },
        "file": {
            "level": logging.DEBUG,
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "filename": log_location,
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 7,
            "encoding": "utf-8",
        },
        "ws": {
            "level": settings.LOG_LEVEL,
            "class": "logging.handlers.QueueHandler",
            "formatter": "simple",
            "queue": ws_queue,
        },
        "dingtalk": {
            "level": settings.LOG_LEVEL,
            "class": "logging.NullHandler",
            "formatter": "simple",
        },
    },
    "loggers": {
        "local": {"handlers": ["console", "file"], "level": logging.DEBUG},
        "remote": {"handlers": ["console", "file", "ws"], "level": logging.INFO},
        "notifier": {"handlers": ["console", "file", "ws"], "level": logging.INFO},
    },
}


def config_logging():
    if settings.DINGTALK_WEBHOOK and settings.DINGTALK_SECRET:
        dingtalk_queue = Queue()
        DEFAULT_LOGGING["handlers"]["dingtalk"] = {
            "level": settings.LOG_LEVEL,
            "class": "logging.handlers.QueueHandler",
            "queue": dingtalk_queue,
            "formatter": "simple",
        }
        DEFAULT_LOGGING["loggers"]["notifier"]["handlers"].append("dingtalk")
        dingtalk_handler = DingTalkHandler(
            webhook=settings.DINGTALK_WEBHOOK, secret=settings.DINGTALK_SECRET
        )
        dingtalk_listener = QueueListener(dingtalk_queue, dingtalk_handler)
        dingtalk_listener.start()

    Path(log_location).parent.mkdir(parents=True, exist_ok=True)
    logging.config.dictConfig(DEFAULT_LOGGING)
    ws_handler = FisherRobotStreamWebsocketHandler(
        ws_uri=settings.ROBOT_STREAM_WS_URI, stream_key=settings.ROBOT_STREAM_KEY,
    )
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
