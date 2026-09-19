from bot import LOGGER
from bot.core.tg_client import TgClient


class _Db:
    async def log_stream_stats(self, stats: dict) -> None:
        LOGGER.debug("TgStream telemetry: %s", stats)


db = _Db()


def sync_clients():
    clients = dict(TgClient.stream_bots or TgClient.helper_bots)
    loads = TgClient.stream_loads if TgClient.stream_bots else {}
    return clients, loads
