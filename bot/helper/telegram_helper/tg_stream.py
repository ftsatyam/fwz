import asyncio
import math
from dataclasses import dataclass
from time import monotonic

from ... import LOGGER
from ...core.config_manager import Config
from ...core.tg_client import TgClient
from ...helper.ext_utils.links_utils import is_telegram_link
from Backend.helper.custom_dl import ACTIVE_STREAMS, ByteStreamer
from Backend.pyrofork import bot as tgstream_clients


class StreamGone(Exception):
    pass


class StreamAbort(Exception):
    pass


class NoClientAvailable(Exception):
    pass


FULL = object()


def parse_range(header, size):
    if not header:
        return FULL
    header = header.strip()
    if not header.lower().startswith("bytes=") or size <= 0:
        return FULL if not header.lower().startswith("bytes=") else None
    spec = header[6:].split(",", 1)[0].strip()
    if spec.startswith("-"):
        try:
            length = int(spec[1:])
        except ValueError:
            return None
        return None if length <= 0 else (max(0, size - length), size - 1)
    first, _, last = spec.partition("-")
    try:
        start = int(first)
    except ValueError:
        return None
    if start < 0 or start >= size:
        return None
    if not last:
        return start, size - 1
    try:
        end = int(last)
    except ValueError:
        return None
    return None if end < start else (start, min(end, size - 1))


def purge_fid(chat_id, msg_id):
    for streamer in list(ByteStreamer._instances.values()):
        streamer._file_id_cache.pop((int(chat_id), int(msg_id)), None)


@dataclass(frozen=True)
class StreamProfile:
    name: str
    chunk_size: int
    parallelism: int
    inline: bool


def _sync_clients():
    tgstream_clients.sync()
    return tgstream_clients.multi_clients, tgstream_clients.work_loads


def _parallelism(count):
    return min(max(math.ceil(max(1, count) / 5), 1), 5)


def profile(name):
    clients, _ = _sync_clients()
    chunk = max(64 * 1024, min(1024 * 1024, int(Config.STREAM_CHUNK or 1024 * 1024)))
    return StreamProfile(name, chunk, _parallelism(len(clients)), name == "playback")


def _pick_client(clients, loads):
    if not clients:
        raise NoClientAvailable("no stream or helper bots are running")
    return min(clients, key=lambda i: (loads.get(i, 0), i))


class TgStreamHandle:
    def __init__(self, chat_id, msg_id, prof, viewer=None):
        self.chat_id = int(chat_id)
        self.msg_id = int(msg_id)
        self.prof = prof
        self.viewer = viewer
        self._released = False
        self._streamer = None
        self._file_id = None
        self._extra = []
        self.size = 0
        self.name = ""
        self.mime = ""
        self.unique_id = ""
        self.stream_id = None

    async def open(self):
        clients, loads = _sync_clients()
        index = _pick_client(clients, loads)
        self._streamer = ByteStreamer(clients[index], index)
        try:
            self._file_id = await self._streamer.get_file_properties(self.chat_id, self.msg_id)
        except Exception as exc:
            raise StreamGone(str(exc)) from exc
        self.size = int(getattr(self._file_id, "file_size", 0) or 0)
        self.name = getattr(self._file_id, "file_name", "") or "File"
        self.mime = getattr(self._file_id, "mime_type", "") or "application/octet-stream"
        self.unique_id = getattr(self._file_id, "unique_id", "") or ""
        self.client_index = index
        self._primary = index
        if not self.size:
            raise StreamGone("media has no size")

        others = [i for i in clients if i != index]
        others.sort(key=lambda i: (loads.get(i, 0), i))
        for extra_index in others[: max(0, self.prof.parallelism - 1)]:
            try:
                extra_streamer = ByteStreamer(clients[extra_index], extra_index)
                extra_file_id = await extra_streamer.get_file_properties(self.chat_id, self.msg_id)
                self._extra.append((extra_index, extra_streamer, extra_file_id))
            except Exception as exc:
                LOGGER.debug("TgStream extra client %s unavailable: %s", extra_index, exc)
        return self

    async def _release(self):
        self._released = True

    async def iter_range(self, start, end):
        if end < start:
            return
        chunk = self.prof.chunk_size
        offset = start - (start % chunk)
        first_cut = start - offset
        last_cut = (end % chunk) + 1
        part_count = math.ceil((end + 1) / chunk) - math.floor(offset / chunk)
        prefetch = max(1, self.prof.parallelism)
        generator = await self._streamer.prefetch_stream(
            file_id=self._file_id,
            client_index=self._primary,
            offset=offset,
            first_part_cut=first_cut,
            last_part_cut=last_cut,
            part_count=part_count,
            chunk_size=chunk,
            prefetch=prefetch,
            parallelism=self.prof.parallelism,
            stream_id=None,
            meta={"title": self.name, "source": "fwz-tgstream"},
            chat_id=self.chat_id,
            message_id=self.msg_id,
            extra_clients=self._extra,
        )
        self.stream_id = next((key for key, value in ACTIVE_STREAMS.items() if value.get("chat_id") == self.chat_id and value.get("msg_id") == self.msg_id), None)
        try:
            async for piece in generator:
                yield piece
        finally:
            await generator.aclose()
            await self._release()


async def open_stream(chat_id, msg_id, kind, viewer=None):
    return await TgStreamHandle(chat_id, msg_id, profile(kind), viewer).open()


async def probe(chat_id, msg_id):
    clients, loads = _sync_clients()
    index = _pick_client(clients, loads)
    streamer = ByteStreamer(clients[index], index)
    try:
        fid = await streamer.get_file_properties(int(chat_id), int(msg_id))
    except Exception as exc:
        raise StreamGone(str(exc)) from exc
    return {
        "name": getattr(fid, "file_name", "") or "",
        "size": int(getattr(fid, "file_size", 0) or 0),
        "mime": getattr(fid, "mime_type", "") or "",
        "unique_id": getattr(fid, "unique_id", "") or "",
    }


async def poster_bytes(chat_id, msg_id):
    clients, loads = _sync_clients()
    index = _pick_client(clients, loads)
    message = await clients[index].get_messages(int(chat_id), int(msg_id))
    media = getattr(message, "photo", None) or getattr(message, "video", None) or getattr(message, "document", None)
    if media is None:
        raise StreamGone("media has no artwork")
    buf = await clients[index].download_media(media, in_memory=True)
    if buf is None:
        raise StreamGone("artwork unavailable")
    return buf.getvalue() if hasattr(buf, "getvalue") else bytes(buf)


async def shutdown():
    return None
