from typing import Optional

from pyrogram import Client
from pyrogram.file_id import FileId

from .exceptions import FileNotFound


def _media(message):
    for attr in ("document", "photo", "video", "audio", "voice", "video_note", "sticker", "animation"):
        value = getattr(message, attr, None)
        if value:
            return value
    return None


async def get_file_ids(client: Client, chat_id: int, message_id: int) -> Optional[FileId]:
    message = await client.get_messages(chat_id, message_id)
    if message is None or getattr(message, "empty", False):
        raise FileNotFound("Message not found or empty")
    media = _media(message)
    if media is None:
        raise FileNotFound("No supported media found in message")
    file_id = FileId.decode(media.file_id)
    file_id.file_name = getattr(media, "file_name", "") or ""
    file_id.file_size = getattr(media, "file_size", 0) or 0
    file_id.mime_type = getattr(media, "mime_type", "") or ""
    file_id.unique_id = getattr(media, "file_unique_id", "") or ""
    return file_id
