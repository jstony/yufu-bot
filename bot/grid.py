import asyncio
import logging.config
import random
import string
import time
from datetime import datetime

from ccxt import async_support as ccxt

from bot import settings
from bot.client import FisherHttpClient, FisherRobotStreamWebsocketClient
from bot.utils.log import config_logging

# fixme: 更优雅的实现
local_logger = logging.getLogger("local")
remote_logger = logging.getLogger("remote")
notifier_logger = logging.getLogger("notifier")


class BybitGridBot(object):
    id = "bybit"
    name = "Bybit"
    ccxt_symbols = {
        "BTCUSD": "BTC/USD",
        "ETHUSD": "ETH/USD",
        "EOSUSD": "EOS/USD",
    }
    ccxt_exchange_class = ccxt.bybit
    order_cid_prefix = "fisher:"

    def __init__(self):
        # ccxt
        self._ccxt_symbol: str = ""
        self.ccxt_exchange = self.ccxt_exchange_class(
            {
                "enableRateLimit": True,
                "options": {"adjustForTimeDifference": True, "recvWindow": 10 * 1000},
            }
        )

        # 交易所账户数据
        self.pos = {
            "dir": 0,
            "qty": 0.0,
            "avg_price": 0.0,
            "liq_price": 0.0,
            "unrealised_pnl": 0.0,
            "leverage": 0.0,
        }
        self.balance = 0.0
        self._principal = 0.0

        # 机器人配置
        self.enabled = False
        self.test_net = False
        self._grids = []
        self._robot_id = -1
        self._username = ""
        self._exchange_name_zh = ""
        self._pair = ""
        self._margin_currency = ""
        self._api_key = ""
        self._secret = ""
        self._asset_id = -1
        self._position_id = -1
        self._synced_order_set = set()
        self._ignored_order_set = set()
        self._order_sync_ts = None
        self._grid_id_index_map = {}
        now = self.timestamp_millisecond()
        self._timestamp_markers = {
            "start": now,
        }
        self.http_client = FisherHttpClient()
        self.ws_client = FisherRobotStreamWebsocketClient()

    async def sync_config(self):
        global asset_id
        global position_id

        result = await self.http_client.fetch_robot_config()
        self.enabled = result["enabled"]
        self._robot_id = result["id"]
        self._pair = result["pair"]
        self._margin_currency = result["margin_currency"]
        # fixme: Find a more elegant way
        settings.ASSET_ID = self._asset_id = result["asset_id"]
        settings.POSITION_ID = self._position_id = result["position_id"]
        ccxt_symbol = self.ccxt_symbols.get(self._pair)
        if not ccxt_symbol:
            # fixme
            raise
        self._ccxt_symbol = ccxt_symbol
        self._api_key = result["credential_keys"]["api_key"]
        self._secret = result["credential_keys"]["secret"]
        self._username = result["user"]["username"]
        self._exchange_name_zh = result["exchange"]["name_zh"]
        self.test_net = result["test_net"]
        if self._order_sync_ts is None:
            self._order_sync_ts = (
                result["order_sync_ts"] or self.timestamp_millisecond()
            )

    async def load_grids(self):
        grids = await self.http_client.fetch_grids()
        self._grids = grids
        for i, grid in enumerate(grids):
            self._grid_id_index_map[grid["id"]] = i

    async def place_order(self, *, order_dir, qty, price, cid=""):
        dir_mapping = {
            -1: "sell",
            1: "buy",
        }
        await self.ccxt_exchange.create_order(
            symbol=self._ccxt_symbol,
            type="limit",
            side=dir_mapping[order_dir],
            amount=qty,
            price=price,
            params={"time_in_force": "PostOnly", "order_link_id": cid},
        )

    def enable_test_net(self):
        self.ccxt_exchange.set_sandbox_mode(enabled=True)

    async def place_orders_batch(self, orders):
        tasks = []
        for order_args in orders:
            tasks.append(self.place_order(**order_args))
        # Todo: 处理异常
        await asyncio.gather(*tasks)

    def _find_grid_order(self, orders, grid_id):
        for order in orders:
            cid: str = order["info"]["order_link_id"]
            if not cid.startswith(self.order_cid_prefix):
                local_logger.warning(
                    "检测到非网格订单[cid=%s, price=%f, size=%d]",
                    cid,
                    order["price"],
                    order["amount"],
                )
                continue

            cid = cid.strip(self.order_cid_prefix)
            if "-" not in cid:
                continue

            if grid_id == int(cid.split("-", 1)[0]):
                return order

    async def _preload_ignored_orders(self):
        orders = await self.ccxt_exchange.fetch_closed_orders(
            symbol=self._ccxt_symbol, limit=50, params={"order_status": "Filled"},
        )
        orders = sorted(orders, key=lambda x: x["timestamp"])
        grid_orders = self._filter_grid_orders(orders)
        for order in grid_orders:
            # fixme 机器人启动前的订单，跳过
            if (
                self.ccxt_exchange.parse8601(order["info"]["updated_at"])
                < self._order_sync_ts
            ):
                local_logger.info(
                    "忽略已同步的订单（id: %s, cid: %s）",
                    order["id"],
                    order["info"]["order_link_id"],
                )
                self._ignored_order_set.add(order["id"])
                continue

    def _filter_grid_orders(self, orders):
        grid_orders = []
        not_grid_orders = []
        for order in orders:
            cid: str = order["info"]["order_link_id"]
            if cid.startswith(self.order_cid_prefix) and "-" in cid:
                grid_orders.append(order)
            else:
                not_grid_orders.append(order)
        return grid_orders

    async def ensure_order(self):
        last_price = await self.get_last_price()
        bid, ask = await self.get_best_price()
        open_orders = await self.ccxt_exchange.fetch_open_orders(
            symbol=self._ccxt_symbol, limit=50
        )
        order_args_list = []
        for grid in reversed(self._grids):
            grid_id = grid["id"]
            if self._find_grid_order(open_orders, grid_id):
                continue

            if grid["holding"]:
                order_args = {
                    "order_dir": 1,
                    "qty": grid["entry_qty"],
                    "price": min(grid["exit_price"], ask - 0.5, self.pos["avg_price"]),
                }
            else:
                if grid["entry_price"] <= last_price:
                    continue

                order_args = {
                    "order_dir": -1,
                    "qty": grid["entry_qty"],
                    "price": grid["entry_price"],
                }
            suffix = self._get_random_string(k=10)
            order_args["cid"] = f"{self.order_cid_prefix}{grid_id}-{suffix}"
            order_args_list.append(order_args)

        await self.place_orders_batch(order_args_list)

    async def sync_balance(self):
        result = await self.ccxt_exchange.fetch_balance()
        self.balance = result[self._margin_currency]["total"]

    async def sync_position(self):
        res = await self.ccxt_exchange.private_get_position_list({"symbol": self._pair})
        pos = {
            "dir": 0,
            "qty": 0.0,
            "avg_price": 0.0,
            "liq_price": 0.0,
            "unrealised_pnl": 0.0,
            "leverage": 0.0,
        }
        if res["result"]["side"] == "None":
            self.pos.update(pos)
            return

        qty = res["result"]["size"]
        dir_mapping = {
            "buy": 1,
            "sell": -1,
        }
        pos_dir = dir_mapping[res["result"]["side"].lower()]
        avg_price = round(float(res["result"]["entry_price"]), 2)  # $
        liq_price = round(float(res["result"]["liq_price"]), 2)
        unrealised_pnl = res["result"]["unrealised_pnl"]
        leverage = res["result"]["effective_leverage"]
        self.pos.update(
            {
                "dir": pos_dir,
                "qty": qty,
                "avg_price": avg_price,
                "liq_price": liq_price,
                "unrealised_pnl": unrealised_pnl,
                "leverage": leverage,
            }
        )

    async def sync_grids(self):
        orders = await self.ccxt_exchange.fetch_closed_orders(
            symbol=self._ccxt_symbol, limit=50, params={"order_status": "Filled"},
        )
        orders = sorted(orders, key=lambda x: x["timestamp"])
        grid_orders = self._filter_grid_orders(orders)
        updated_grids = []
        for order in grid_orders:
            # 已同步，跳过
            if order["id"] in self._synced_order_set:
                continue

            if order["id"] in self._ignored_order_set:
                continue

            cid: str = order["info"]["order_link_id"]
            grid_id = int(cid.strip(self.order_cid_prefix).split("-", 1)[0])
            grid_index = self._grid_id_index_map.get(grid_id, -1)
            if grid_index < 0:
                local_logger.info("未找到 %s 订单关联的网格", cid)
                self._ignored_order_set.add(order["id"])
                continue

            grid = self._grids[grid_index]
            if order["side"] == "sell":
                grid["filled_qty"] = order["cost"]
                grid["holding"] = True
                notifier_logger.info(
                    "网格（层%s, 入%.2f, 出%.2f, 量%d）开，关联订单：%s %d@%.2f",
                    grid["index"],
                    grid["entry_price"],
                    grid["exit_price"],
                    grid["entry_qty"],
                    order["side"],
                    order["amount"],
                    order["price"],
                )
                updated_grids.append(grid)

            if order["side"] == "buy":
                grid["filled_qty"] = 0
                grid["holding"] = False
                notifier_logger.info(
                    "网格（层%s, 入%.2f, 出%.2f, 量%d）平，关联订单：%s %d@%.2f",
                    grid["index"],
                    grid["entry_price"],
                    grid["exit_price"],
                    grid["entry_qty"],
                    order["side"],
                    order["amount"],
                    order["price"],
                )
                updated_grids.append(grid)

            self._synced_order_set.add(order["id"])
            remote_logger.info("已同步 %d 个网格订单", len(self._synced_order_set))

        if len(updated_grids) > 0:
            # fixme 失败重传，否则可能导致网格状态不一致
            await self.http_client.update_grids(updated_grids)
            # Todo: 优化为消息队列，减少阻塞
            await self.ws_client.send_grids(updated_grids)

        # Todo: 优化回传频率
        self._order_sync_ts = self.timestamp_millisecond()
        await self.http_client.update_robot(data={"order_sync_ts": self._order_sync_ts})

    async def get_last_price(self):
        ticker = await self.ccxt_exchange.fetch_ticker(symbol=self._ccxt_symbol)
        last_price = ticker["last"]
        return last_price

    async def get_best_price(self):
        order_book = await self.ccxt_exchange.fetch_l2_order_book(
            symbol=self._ccxt_symbol, limit=5
        )
        best_bid = order_book["bids"][0][0]
        best_ask = order_book["asks"][0][0]
        return best_ask, best_bid

    async def pre_trade(self):
        await self.sync_config()
        if self.test_net:
            self.enable_test_net()
        self.auth()
        await self.ccxt_exchange.load_markets()
        tasks = [
            self.ccxt_exchange.cancel_all_orders(symbol=self._ccxt_symbol),
            self.sync_position(),
            self.sync_balance(),
            self.load_grids(),
            self._preload_ignored_orders(),
        ]
        await asyncio.gather(*tasks)

    async def wait_enable(self):
        """
        等待机器人开启
        """
        while not self.enabled:
            remote_logger.info("机器人处于关闭状态...")
            await self.ccxt_exchange.cancel_all_orders(symbol=self._ccxt_symbol)
            await asyncio.sleep(5)
            await self.sync_config()

    async def trade(self):
        remote_logger.info("正在设置交易环境")
        await self.pre_trade()
        cnt = 1
        while True:
            try:
                local_logger.info("检测轮次: %d", cnt)
                await self.sync_config()
                await self.wait_enable()
                await asyncio.gather(
                    self.sync_position(), self.sync_balance(), self.sync_grids()
                )
                await self.ensure_order()
                time.sleep(settings.TRADE_INTERVAL)
                cnt += 1
            except Exception as e:
                notifier_logger.exception(e, exc_info=True)
                time.sleep(settings.TRADE_INTERVAL)

    def auth(self):
        self.ccxt_exchange.apiKey = self._api_key
        self.ccxt_exchange.secret = self._secret

    async def feedback_position(self):
        data = {
            "direction": self.pos["dir"],
            "qty": self.pos["qty"],
            "avg_price": self.pos["avg_price"],
            "unrealized_pnl": self.pos["unrealised_pnl"],
            "liq_price": self.pos["liq_price"],
            "leverage": self.pos["leverage"],
        }
        await self.http_client.update_position(data)
        await self.ws_client.send_position(data=data)

    async def feedback_asset(self):
        await self.http_client.update_asset({"balance": self.balance})

    async def ping_task(self):
        while True:
            try:
                await self.http_client.ping()
                await self.ws_client.send_ping(
                    data={"timestamp": self.timestamp_millisecond()}
                )
                local_logger.debug("Ping to server")
            except Exception as e:
                # fixme 更加细致的异常处理
                remote_logger.exception(e, exc_info=True)
            await asyncio.sleep(180)

    async def feedback_task(self):
        while True:
            try:
                await asyncio.gather(self.feedback_position(), self.feedback_asset())
                local_logger.debug("Feedback to server")
            except Exception as e:
                # fixme 更加细致的异常处理
                remote_logger.exception(e, exc_info=True)
            await asyncio.sleep(30)

    async def start(self):
        # fixme: more elegant way
        await self.ws_client.start()
        await self.sync_config()
        await asyncio.gather(self.trade(), self.ping_task(), self.feedback_task())

    def run(self):
        config_logging()
        loop = asyncio.get_event_loop()
        loop.create_task(self.start())
        loop.run_forever()
        loop.stop()
        loop.close()

    # 输出日志美化
    def log_pos(self):
        msg = "仓位状态: {}@{:.2f}, 强平={:.2f}".format(
            self.pos["dir"] * self.pos["size"],
            self.pos["avg_price"],
            self.pos["liq_price"],
        )
        remote_logger.info(msg)

    @staticmethod
    def _get_random_string(k=10):
        letters = string.ascii_letters
        return "".join(random.choices(letters, k=k))

    @staticmethod
    def timestamp_millisecond(dt=None, delta=None):
        if not dt:
            dt = datetime.now()

        if delta is not None:
            dt = dt + delta

        return int(dt.timestamp() * 1000)


if __name__ == "__main__":
    bot = BybitGridBot()
    bot.run()
