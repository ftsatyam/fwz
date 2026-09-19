from bot.core.tg_client import TgClient


multi_clients = {}
work_loads = {}
client_dc_map = {}
client_failures = {}
client_avg_mbps = {}


def sync():
    clients = dict(TgClient.stream_bots or TgClient.helper_bots)
    loads = TgClient.stream_loads if TgClient.stream_bots else {}
    multi_clients.clear()
    multi_clients.update(clients)
    work_loads.clear()
    work_loads.update(loads)
    for index in clients:
        client_dc_map.setdefault(index, None)
        client_failures.setdefault(index, 0)
        client_avg_mbps.setdefault(index, 0.0)
    return multi_clients
