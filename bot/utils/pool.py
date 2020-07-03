import asyncio
import logging

import websockets

logger = logging.getLogger("yufu")


class Pool:
    def __init__(self, ws_uri: str, initsize: int = 5) -> None:
        self.ws_uri = ws_uri
        self.initsize = initsize
        self._freepool = set()
        asyncio.get_event_loop().create_task(self.init(initsize))
        self.timed_task()

    async def init(self, size: int) -> None:
        await asyncio.gather(*[self._create() for _ in range(size)])

    def timed_task(self) -> None:
        async def _timed_task() -> None:
            while True:
                await asyncio.sleep(7)

                for sock in tuple(self._freepool):
                    if sock.closed:
                        self._freepool.remove(sock)

                while len(self._freepool) > self.initsize * 2:
                    sock = self._freepool.pop()
                    await sock.close()

        asyncio.get_event_loop().create_task(_timed_task())

    async def acquire(self) -> websockets.WebSocketClientProtocol:
        while True:
            try:
                sock = self._freepool.pop()
                if sock.closed:
                    continue
                if self.initsize > len(self._freepool):
                    asyncio.create_task(self._create())
                return sock
            except KeyError:
                await self._create()

    async def release(self, sock: websockets.WebSocketClientProtocol) -> None:
        if isinstance(sock, websockets.WebSocketClientProtocol):
            if sock.closed:
                return
            self._freepool.add(sock)

    async def _create(self):
        sock = await websockets.connect(self.ws_uri)
        self._freepool.add(sock)

    @property
    def size(self):
        return len(self._freepool)


if __name__ == "__main__":
    from datetime import datetime
    import json

    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    async def main():
        pool = Pool(
            ws_uri="ws://13.229.239.117:7000/robots/2/streams/?stream_key=eyJyb2JvdF9pZCI6Mn0:1jpuEr:qCRypWzY8E9wsBVs70fPLqwkN6c"
        )
        while True:
            print("当前连接池大小: ", pool.size)
            sock = await pool.acquire()
            try:
                await sock.send(
                    json.dumps(
                        {
                            "topic": "log",
                            "level": "INFO",
                            "timestamp": int(datetime.now().timestamp() * 1000),
                            "msg": "test",
                        }
                    )
                )
            except websockets.exceptions.ConnectionClosedError:
                print("sock 连接已断开")
            await pool.release(sock)
            await asyncio.sleep(0.1)

    asyncio.run(main())
