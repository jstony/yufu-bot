import json
import logging

import httpx
import websockets

from bot import const as c
from bot import settings

local_logger = logging.getLogger("local")


class FisherRobotStreamWebsocketClient(object):
    connect_kwargs = {
        "timeout": 5,
        "close_timeout": 5,
        "ping_timeout": None,
    }

    def __init__(self):
        self.ws_uri = (
            settings.ROBOT_STREAM_WS_URI
            + "?"
            + "stream_key="
            + settings.ROBOT_STREAM_KEY
        )
        self.ws = None
        self._task = None

    def get_connect_kwargs(self):
        return self.connect_kwargs

    async def connect(self):
        connect_kwargs = self.get_connect_kwargs()
        self.ws = await websockets.connect(self.ws_uri, **connect_kwargs)
        local_logger.info("Connected!")

    async def send(self, s):
        # fixme: 处理断线重连
        if self.ws.closed:
            await self.connect()
        await self.ws.send(s)
        local_logger.debug(">>> %s", s)

    async def send_position(self, data):
        message = {"topic": "position", "data": data}
        await self.send(self.encode(message))

    async def send_asset(self, data):
        message = {"topic": "asset", "data": data}
        await self.send(self.encode(message))

    async def send_grids(self, data):
        message = {"topic": "grid", "data": data}
        await self.send(self.encode(message))

    async def send_ping(self, data):
        message = {"topic": "ping", "data": data}
        await self.send(self.encode(message))

    async def start(self):
        await self.connect()

    async def stop(self):
        await self.ws.close()

    @staticmethod
    def encode(message):
        return json.dumps(message)

    @staticmethod
    def decode(s):
        return json.loads(s)


class FisherHttpClient(object):
    headers = {"Authorization": f"Token {settings.TOKEN}"}

    def __init__(self):
        self.base_url: str = settings.SERVER_BASE_URL.rstrip("/")

    async def fetch_grid_strategy_parameter(self):
        """获取网格数据"""
        parameter = await self._request(
            "GET", c.ROBOT_GRID_STRATEGY_PARAMETER_REQ_PATH.format(pk=settings.ROBOT_ID)
        )
        return parameter

    async def fetch_grids(self):
        """获取网格数据"""
        grids = await self._request(
            "GET", c.ROBOT_GRIDS_REQ_PATH.format(pk=settings.ROBOT_ID)
        )
        return grids

    async def fetch_robot_config(self):
        """获取机器人配置信息"""
        cfg = await self._request(
            "GET", c.ROBOT_CONFIG_REQ_PATH.format(pk=settings.ROBOT_ID)
        )
        return cfg

    async def update_grids(self, grids):
        """更新网格数据"""
        await self._request("PATCH", c.GRIDS_REQ_PATH, json=grids)

    async def update_robot(self, data):
        """更新机器人数据"""
        await self._request(
            "PATCH", c.ROBOT_REQ_PATH.format(pk=settings.ROBOT_ID), json=data
        )

    async def update_asset(self, data):
        """更新资产数据"""
        await self._request(
            "PATCH", c.ASSETS_REQ_PATH.format(pk=settings.ASSET_ID), json=data
        )

    async def update_position(self, data):
        """更新仓位数据"""
        await self._request(
            "PATCH", c.POSITIONS_REQ_PATH.format(pk=settings.POSITION_ID), json=data
        )

    async def ping(self):
        """Ping 服务器"""
        await self._request("POST", c.ROBOT_PING_REQ_PATH.format(pk=settings.ROBOT_ID))

    async def _request(
        self, method, req_path, headers=None, params=None, data=None, json=None
    ):
        url = self.base_url + req_path
        local_logger.debug(
            "%s %s, request: headers=%s params=%s data=%s json=%s",
            method,
            url,
            headers,
            params,
            data,
            json,
        )
        async with httpx.AsyncClient() as client:
            res = await client.request(
                method,
                url,
                headers=headers or self.headers,
                params=params,
                data=data,
                json=json,
                timeout=None,
            )
        http_text = res.text
        local_logger.debug(
            "%s %s, response: status_code=%s headers=%s http_text=%s",
            method,
            url,
            res.status_code,
            headers,
            http_text,
        )
        res.raise_for_status()
        if res.status_code == "204":
            return None
        return res.json()
