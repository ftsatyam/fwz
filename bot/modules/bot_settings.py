from asyncio import (
    gather,
    sleep,
)
from ast import literal_eval
from pyrogram import raw
from pyrogram.enums import ButtonStyle
from pyrogram.types import (
    InputRichBlockDetails,
    InputRichBlockDivider,
    InputRichBlockFooter,
    InputRichBlockList,
    InputRichBlockListItem,
    InputRichBlockParagraph,
    InputRichBlockSectionHeading,
    InputRichBlockTable,
    InputRichBlockTableCell,
    InputRichMessage,
)
from functools import partial
from io import BytesIO
from os import getcwd, getenv
from shlex import quote as shlex_quote
from time import time

from aiofiles import open as aiopen
from aiofiles.os import path as aiopath
from aiofiles.os import remove, rename
from aioshutil import rmtree
from pyrogram.filters import create
from pyrogram.handlers import MessageHandler

from .. import (
    LOGGER,
    bot_loop,
    categories_dict,
    drives_ids,
    drives_names,
    index_urls,
    intervals,
    scheduler,
    task_dict,
    shortener_dict,
    excluded_extensions,
    auth_chats,
    sudo_users,
    var_list,
)
from ..helper.ext_utils.bot_utils import (
    SetInterval,
    cmd_exec,
    new_task,
    parse_dest,
)
from ..core.config_manager import Config, DEFAULT_CONFIG
from ..core.tg_client import TgClient, db_partition_id
from ..helper.ext_utils.db_handler import database
from ..helper.ext_utils.task_manager import start_from_queued
from ..helper.mirror_leech_utils.rclone_utils.serve import rclone_serve_booter
from ..helper.telegram_helper.button_build import ButtonMaker
from ..helper.telegram_helper.bot_commands import BotCommands
from ..helper.telegram_helper.message_utils import (
    delete_message,
    edit_message,
    send_file,
    send_message,
    update_status_message,
)

start = 0
state = "view"
handler_dict = {}
DEFAULT_VALUES = {
    "LEECH_SPLIT_SIZE": TgClient.MAX_SPLIT_SIZE,
    "STATUS_UPDATE_INTERVAL": 15,
    "UPSTREAM_BRANCH": "wzv3",
    "DEFAULT_UPLOAD": "rc",
    "BOT_MAX_TASKS": 0,
    "QUEUE_ALL": 0,
    "QUEUE_DOWNLOAD": 0,
    "QUEUE_UPLOAD": 0,
    "USER_MAX_TASKS": 0,
}

BOOL_VARS = [
    "AS_DOCUMENT",
    "AUTO_THUMBNAIL",
    "BOT_PM",
    "DELETE_LINKS",
    "DRIVE_CATEGORY_MODE",
    "DISABLE_BULK",
    "DISABLE_FF_MODE",
    "DISABLE_LEECH",
    "DISABLE_MIRROR",
    "DISABLE_MULTI",
    "DISABLE_STREAM",
    "DISABLE_PLUGINS",
    "ENABLE_TELEMETRY",
    "EQUAL_SPLITS",
    "GOFILE_AUTO_CREATE_FOLDER",
    "INC_TASK_NOTIFY",
    "INC_TASK_RESUME",
    "IS_TEAM_DRIVE",
    "MEDIA_GROUP",
    "MEDIA_STORE",
    "MEM_DEEP_STATS",
    "SET_COMMANDS",
    "SHOW_CLOUD_LINK",
    "STOP_DUPLICATE",
    "USE_HYPER",
    "USE_IMAGES",
    "USE_SERVICE_ACCOUNTS",
    "WEB_PINCODE",
]

DEFAULT_DESP = {
    "AS_DOCUMENT": "Send files as document instead of media. Default: False.",
    "AUTHORIZED_CHATS": "User/Chat IDs authorized to use the bot. Space-separated. Supports thread IDs with | separator.",
    "BASE_URL": "Public URL for the streaming web UI. Format: http://ip or http://ip:port.",
    "BOT_TOKEN": "Telegram Bot Token from @BotFather.",
    "HELPER_TOKENS": "Additional bot tokens for parallel task handling.",
    "HELPER_STRINGS": "Extra user session strings for parallel task handling. Space-separated.",
    "HELPER_BOT_PROXIES": "One proxy dict per line, matching HELPER_TOKENS order. Empty line = no proxy for that bot.",
    "HELPER_USER_PROXIES": "One proxy dict per line, matching HELPER_STRINGS order. Empty line = no proxy for that user.",
    "STREAM_TOKENS": "Bot tokens dedicated to /stream and /dl. If set, streaming uses these and is isolated from mirror/leech load. Falls back to HELPER_TOKENS.",
    "BOT_MAX_TASKS": "Max tasks (including queued) the bot runs in parallel. 0 = unlimited.",
    "BOT_PM": "Send files/links to bot owner PM. Default: False.",
    "CMD_SUFFIX": "Text appended to all bot commands. Useful for running multiple bot instances.",
    "DEFAULT_LANG": "Default bot language code. Default: en.",
    "DATABASE_URL": "MongoDB connection string for persistent storage.",
    "DEFAULT_UPLOAD": "Default upload destination: gd (Google Drive) or rc (rclone). Default: rc.",
    "DELETE_LINKS": "Auto-delete source links/messages on task start. Default: False.",
    "DISABLE_LEECH": "Disable all leech (download to Telegram) tasks. Default: False.",
    "DISABLE_MIRROR": "Disable all mirror (upload to cloud) tasks. Default: False.",
    "DISABLE_BULK": "Disable bulk (zip/unzip) operations. Default: False.",
    "DISABLE_MULTI": "Disable multi-part splits. Default: False.",
    "DISABLE_FF_MODE": "Disable FFmpeg processing mode. Default: False.",
    "DISABLE_PLUGINS": "Disable the plugin system. Unloads every plugin and stops loading them at boot. Default: False.",
    "DISABLE_STREAM": "Disable streaming. Stops /stream and the stream server. Default: False.",
    "EQUAL_SPLITS": "Split files into equal parts of LEECH_SPLIT_SIZE. Default: False.",
    "EXCLUDED_EXTENSIONS": "File extensions to exclude from upload/clone. Space-separated.",
    "FFMPEG_CMDS": "Custom FFmpeg command presets. Dict format.",
    "FILELION_API": "FileLion.cc API key for direct download support.",
    "MEDIA_STORE": "Store media metadata for re-upload. Default: True.",
    "FORCE_SUB_IDS": "Channel/Group IDs for force subscription. Space-separated.",
    "GOFILE_API": "Gofile.io API token for file uploads.",
    "GOFILE_FOLDER_ID": "Gofile.io folder ID for uploads.",
    "GOFILE_AUTO_CREATE_FOLDER": "With no GOFILE_FOLDER_ID, make a folder per upload instead of using the account root. Default: False.",
    "PIXELDRAIN_KEY": "PixelDrain API key for uploads.",
    "PROTECTED_API": "ProtectedFiles.cc API key.",
    "BUZZHEAVIER_API": "BuzzHeavier API key for uploads.",
    "DEVUPLOADS_KEY": "DevUploads API key.",
    "DEVUPLOADS_FOLDER": "DevUploads folder ID.",
    "VIKINGFILE_HASH": "VikingFile.to hash for uploads.",
    "VIKINGFILE_FOLDER": "VikingFile.to folder ID.",
    "GDRIVE_ID": "Google Drive folder/TeamDrive ID for uploads.",
    "DRIVE_CATEGORY_MODE": "Let users set their own Drive upload categories in /usettings. Default: False.",
    "DRIVE_CATEGORY_SA": "Email given reader access on uploads that go outside GDRIVE_ID. Empty = skip.",
    "GD_DESP": "Description for Google Drive uploads. Default: Uploaded with WZ Bot.",
    "AUTHOR_NAME": "Author name shown on Telegraph pages.",
    "AUTHOR_URL": "Author URL for Telegraph pages. Use channel URL for join button.",
    "IMDB_TEMPLATE": "Optional HTML template for IMDB results. If empty, uses Rich Messages.",
    "IMAGES": "List of image URLs or file_ids for the gallery. Managed via /addimage command.",
    "IMG_SEARCH": "Comma-separated keywords to auto-fetch wallpaper images on startup. e.g. anime, nature, space",
    "IMG_PAGE": "Number of pages to search for each keyword in IMG_SEARCH. Each page has ~70 images. Default: 1",
    "USE_IMAGES": "Enable random photo backgrounds on bot messages. Requires IMAGES list. Default: False",
    "IMG_SOURCES": "List of image sources to fetch from. Options: wallpaperflare, peapix, wallhaven. Default: wallpaperflare",
    "INC_TASK_NOTIFY": "Notify about incomplete tasks after restart. Default: False.",
    "INC_TASK_RESUME": "Auto-resume incomplete tasks on restart. Default: False.",
    "INDEX_URL": "Google Drive Index URL for direct links.",
    "IS_TEAM_DRIVE": "Set True for TeamDrive uploads. Default: False.",
    "GD_DL_LIMIT": "Google Drive download size limit in GB. 0 = unlimited.",
    "RC_DL_LIMIT": "Rclone download size limit in GB. 0 = unlimited.",
    "CLONE_LIMIT": "Google Drive clone size limit in GB. 0 = unlimited.",
    "PLAYLIST_LIMIT": "Max items to download from a playlist. 0 = unlimited.",
    "LEECH_LIMIT": "Leech (Telegram upload) size limit in GB. 0 = unlimited.",
    "EXTRACT_LIMIT": "Extracted file size limit in GB. 0 = unlimited.",
    "ARCHIVE_LIMIT": "Archive (zip) size limit in GB. 0 = unlimited.",
    "STORAGE_LIMIT": "Minimum free storage to maintain in GB. Downloads cancelled if exceeded.",
    "LEECH_LOG_CHAT": "Chat ID to dump all leeched files, or chat_id|topic_id for a forum topic. Leave empty to disable.",
    "LEECH_DUMP_CHATS": 'Named leech dump chats selectable per task via -ud flag. Dict format: {"name": chat_id}. Example: {"A": -100123}.',
    "LINKS_LOG_ID": "Chat ID for link logging.",
    "MIRROR_LOG_ID": "Chat ID(s) for mirror logs. Space-separated for multiple.",
    "LEECH_PREFIX": "Prefix added to leeched file names.",
    "TMDB_ACCESS_TOKEN": "TMDb API key (v3) or Read Access Token (v4), used by AUTO_THUMBNAIL.",
    "AUTO_THUMBNAIL": "Fetch a poster from TMDb as thumbnail when no other thumbnail exists. Default: False.",
    "LEECH_CAPTION": "Custom caption for leeched files. Supports HTML.",
    "LEECH_SUFFIX": "Suffix added to leeched file names.",
    "LEECH_FONT": "Font style for captions: b, i, u, s, code, spoiler.",
    "LEECH_SPLIT_SIZE": "Split size for Telegram uploads in bytes. Default: 2GB (4GB for premium).",
    "MEDIA_GROUP": "Upload split parts as media group. Default: False.",
    "USE_HYPER": "Enable HyperDL/HyperUP for faster Telegram transfers. Default: True.",
    "HYPER_THREADS": "Number of parallel download parts (clients). 0 = auto.",
    "HYPER_PIPELINE": "Concurrent GetFile requests per HyperDL part. Default: 4.",
    "HYPER_CHUNK": "HyperDL working chunk size in bytes. Default: 512 * 1024 (512KB).",
    "STREAM_PIPELINE": "Concurrent GetFile requests for /dl downloads. Default: 8.",
    "STREAM_CHUNK": "Streaming chunk size in bytes, capped at 1 MiB. Default: 1048576.",
    "STREAM_PER_CLIENT": "Concurrent playback streams allowed per bot. Raise for more simultaneous viewers, lower if Telegram floods. Default: 6.",
    "STREAM_GATE": "Process-wide ceiling on concurrent GetFile calls. Default: 96.",
    "MEM_BUDGET": "Memory ceiling for transfer buffers in MB. 0 = auto (15% of the container limit).",
    "MEM_DEEP_STATS": "Add object counts to /memory. Costs a full GC scan per call. Default: False.",
    "CPU_LIMIT": "CPU limit percentage for bot processes. Default: 20.",
    "FFMPEG_CORES": "CPUs given to FFmpeg. auto = 60% of them, all/0 = every CPU, a count (5), a percentage (75%), or a taskset list (0-4). Services take the rest.",
    "THROTTLE_SERVICES": "Pause services during heavy ops (FFmpeg). auto=low-end only, always, never.",
    "NAME_SWAP": "Rename files using pattern. Format: old:new|old2:new2.",
    "OWNER_ID": "Telegram User ID of the bot owner.",
    "QUEUE_ALL": "Max parallel download+upload tasks. 0 = unlimited.",
    "QUEUE_DOWNLOAD": "Max parallel downloading tasks. 0 = unlimited.",
    "QUEUE_UPLOAD": "Max parallel uploading tasks. 0 = unlimited.",
    "RCLONE_FLAGS": "Rclone flags. Format: key:value|key|key:value.",
    "RCLONE_PATH": "Default rclone remote path for uploads.",
    "RCLONE_SERVE_URL": "Public URL for rclone serve. Format: http://ip.",
    "SHOW_CLOUD_LINK": "Show cloud link button on leeched files. Default: True.",
    "RCLONE_SERVE_USER": "Username for rclone serve authentication.",
    "RCLONE_SERVE_PASS": "Password for rclone serve authentication.",
    "RCLONE_SERVE_PORT": "Port for rclone serve. Default: 8081.",
    "SET_COMMANDS": "Auto-set bot commands on start. Default: True.",
    "STATUS_LIMIT": "Number of status messages to show. Default: 10.",
    "STATUS_UPDATE_INTERVAL": "Status message refresh interval in seconds. Default: 15.",
    "STOP_DUPLICATE": "Stop if file/folder exists in GDrive. Default: False.",
    "STREAMWISH_API": "StreamWish API key for uploads.",
    "SUDO_USERS": "User IDs with sudo access. Space-separated.",
    "TELEGRAM_API": "Telegram API ID from my.telegram.org.",
    "TELEGRAM_HASH": "Telegram API Hash from my.telegram.org.",
    "TG_PROXY": "SOCKS5 proxy for Telegram connection. Format: socks5://user:pass@ip:port.",
    "THUMBNAIL_LAYOUT": "Thumbnail layout for uploads. Format: WxH (e.g., 1280x720).",
    "VERIFY_TIMEOUT": "Verification timeout in seconds. 0 = disabled.",
    "LOGIN_PASS": "Password to skip token system. Leave empty to disable.",
    "TIMEZONE": "Timezone for messages. Default: Asia/Kolkata.",
    "USER_MAX_TASKS": "Max concurrent tasks per user. 0 = unlimited.",
    "USER_TIME_INTERVAL": "Cooldown between tasks per user in seconds. 0 = disabled.",
    "UPLOAD_PATHS": "Custom upload paths per extension. Dict format.",
    "UPSTREAM_REPO": "GitHub repo URL for bot updates.",
    "UPSTREAM_BRANCH": "Branch for updates. Default: wzv3.",
    "USER_SESSION_STRING": "Pyrogram session string for user account tasks.",
    "TRANSMISSION_MODE": "Transmission mode: bot, user, or both. Default: both.",
    "USE_SERVICE_ACCOUNTS": "Use Google Service Accounts. Default: False.",
    "WEB_ACCESS_PASSWORD": "Secret for deriving proxy passwords. Set once, use derived passwords in browser. Empty = auto-generated.",
    "WEB_PINCODE": "Ask for pincode in web file selection. Default: True.",
    "YT_DESP": "Description for YouTube uploads. Default: Uploaded with WZML-X bot.",
    "YT_TAGS": "Tags for YouTube uploads. List format.",
    "YT_CATEGORY_ID": "YouTube video category ID. Default: 22 (People & Blogs).",
    "PLUGIN_INDEXES": "Extra plugin index URLs on top of the official one. Each must be a JSON file holding a plugins list.",
    "ENABLE_TELEMETRY": "Send crash reports to telemetry.wzmlx.com to help fix bugs. Default: True.",
    "YT_PRIVACY_STATUS": "YouTube upload privacy: public, unlisted, or private.",
}

PROTECTED_VARS = {
    "TELEGRAM_HASH",
    "TELEGRAM_API",
    "OWNER_ID",
    "BOT_TOKEN",
    "DATABASE_URL",
}
RESTART_VARS = {
    "STREAM_TOKENS",
    "CMD_SUFFIX",
    "OWNER_ID",
    "USER_SESSION_STRING",
    "TELEGRAM_HASH",
    "TELEGRAM_API",
    "BOT_TOKEN",
    "TG_PROXY",
    "AUTHORIZED_CHATS",
    "DATABASE_URL",
}

ONOFF_VARS = [
    "DISABLE_LEECH",
    "DISABLE_MIRROR",
    "DISABLE_BULK",
    "DISABLE_MULTI",
    "DISABLE_FF_MODE",
    "DISABLE_PLUGINS",
    "DISABLE_STREAM",
]

LIMIT_VARS = [
    "GD_DL_LIMIT",
    "RC_DL_LIMIT",
    "CLONE_LIMIT",
    "PLAYLIST_LIMIT",
    "LEECH_LIMIT",
    "EXTRACT_LIMIT",
    "ARCHIVE_LIMIT",
    "STORAGE_LIMIT",
    "CPU_LIMIT",
    "STATUS_LIMIT",
]

LIMIT_UNITS = {
    "CPU_LIMIT": "%",
    "PLAYLIST_LIMIT": " items",
    "STATUS_LIMIT": " msgs",
}

RICH_STYLES = {
    "b": raw.types.TextBold,
    "i": raw.types.TextItalic,
    "u": raw.types.TextUnderline,
    "c": raw.types.TextFixed,
    "m": raw.types.TextMarked,
    "s": raw.types.TextStrike,
}


def rich_text(*parts):
    texts = []
    for part in parts:
        if isinstance(part, str):
            texts.append(raw.types.TextPlain(text=part))
        else:
            style, value = part
            texts.append(
                RICH_STYLES[style](text=raw.types.TextPlain(text=value))
            )
    return raw.types.TextConcat(texts=texts)


async def get_buttons(key=None, edit_type=None, edit_mode=False):
    buttons = ButtonMaker()
    if key is None:
        buttons.data_button("Config Variables", "botset var")
        buttons.data_button("Module Settings", "botset setonoff")
        buttons.data_button("Private Files", "botset private open")
        buttons.data_button("Close", "botset close", style=ButtonStyle.DANGER)
        msg = "Bot Settings:"
    elif edit_type is not None:
        if edit_type == "editvar":
            msg = f"<b>Variable:</b> <code>{key}</code>\n\n"
            msg += f"<b>Description:</b> {DEFAULT_DESP.get(key, 'No Description Provided')}\n\n"
            value = Config.get(key)
            if value == "":
                value = "None"
            msg += f"<b>Current Value:</b> <code>{value}</code>\n\n"
            buttons.data_button(
                "View Value", f"botset showvar {key}", position="header"
            )
            buttons.data_button(
                "Back",
                f"botset back {'setlimit' if key in LIMIT_VARS else 'var'}",
                position="footer",
            )
            if key not in BOOL_VARS:
                if not edit_mode:
                    buttons.data_button(
                        "Edit Value",
                        f"botset editvar {key} edit",
                        style=ButtonStyle.PRIMARY,
                    )
                else:
                    buttons.data_button("Stop Edit", f"botset editvar {key}")
            else:
                msg += "<i>Choose a valid value for the above Var</i>\n\n"
                buttons.data_button("True", f"botset boolvar {key} on")
                buttons.data_button("False", f"botset boolvar {key} off")
            if key not in BOOL_VARS and key not in PROTECTED_VARS:
                buttons.data_button("Reset", f"botset resetvar {key}")
            buttons.data_button(
                "Close", "botset close", position="footer", style=ButtonStyle.DANGER
            )
            if edit_mode and key in RESTART_VARS:
                msg += (
                    "\n<b>Note:</b> Restart required for this edit to take effect!\n\n"
                )
            if edit_mode and key not in BOOL_VARS:
                msg += "<i>Send a valid value for the above Var.</i>\n┖ <b>Time Left :</b> <code>60 sec</code>"
    elif key == "var":
        conf_dict = {
            k: v
            for k, v in Config.get_all().items()
            if not k.startswith("DISABLE_") and k not in LIMIT_VARS
        }
        all_keys = list(conf_dict.keys())
        for k in all_keys[start : 10 + start]:
            buttons.data_button(k, f"botset editvar {k}")
        buttons.data_button("Back", "botset back")
        buttons.data_button("Close", "botset close", style=ButtonStyle.DANGER)
        for x in range(0, len(all_keys), 10):
            buttons.data_button(
                f"{int(x / 10) + 1}", f"botset start var {x}", position="footer"
            )
        msg = f"⌬ <b><u>Config Variables</u></b> | <b><u>Page: {int(start / 10) + 1}</b></u>"
    elif key == "setonoff":
        buttons.data_button("On/Off Settings", "botset settoggle")
        buttons.data_button("Limit Settings", "botset setlimit")
        buttons.data_button("Back", "botset back", position="footer")
        buttons.data_button(
            "Close", "botset close", position="footer", style=ButtonStyle.DANGER
        )
        msg = "⌬ <b><u>Module Settings</u></b>"
    elif key == "settoggle":
        for k in ONOFF_VARS:
            val = Config.get(k)
            label = k.removeprefix("DISABLE_")
            if not val:
                buttons.data_button(
                    f"✓ {label}",
                    f"botset toggleonoff {k} on",
                    style=ButtonStyle.PRIMARY,
                )
            else:
                buttons.data_button(
                    label,
                    f"botset toggleonoff {k} off",
                    style=ButtonStyle.DANGER,
                )
        buttons.data_button("Back", "botset back setonoff", position="footer")
        buttons.data_button(
            "Close", "botset close", position="footer", style=ButtonStyle.DANGER
        )
        msg = "⌬ <b><u>On/Off Settings</u></b>"
    elif key == "setlimit":
        page_vars = LIMIT_VARS[start : 10 + start]
        for k in page_vars:
            buttons.data_button(
                k.removesuffix("_LIMIT").replace("_", " "),
                f"botset editvar {k}",
                style=ButtonStyle.SUCCESS if Config.get(k) else None,
            )
        buttons.data_button("Back", "botset back setonoff", position="footer")
        buttons.data_button(
            "Close", "botset close", position="footer", style=ButtonStyle.DANGER
        )
        if start:
            buttons.data_button(
                "⫷", f"botset start setlimit {start - 10}", position="l_body"
            )
        if start + 10 < len(LIMIT_VARS):
            buttons.data_button(
                "⫸", f"botset start setlimit {start + 10}", position="l_body"
            )
        rows = [
            [
                InputRichBlockTableCell(
                    rich_text(k.removesuffix("_LIMIT").replace("_", " "))
                ),
                InputRichBlockTableCell(
                    rich_text(("c", f"{v}{LIMIT_UNITS.get(k, ' GB')}")),
                    align_right=True,
                ),
            ]
            for k in LIMIT_VARS
            if (v := Config.get(k))
        ]
        msg = InputRichMessage(
            blocks=[
                InputRichBlockSectionHeading(
                    text=rich_text(("u", "Limit Settings")), size=3
                ),
                InputRichBlockParagraph(
                    text=rich_text(
                        ("m", f" {len(rows)} of {len(LIMIT_VARS)} active "),
                    )
                ),
                InputRichBlockTable(
                    title=rich_text(("b", "Active limits")),
                    rows=rows,
                    bordered=True,
                    striped=True,
                    compact=True,
                )
                if rows
                else InputRichBlockParagraph(
                    text=rich_text(("i", "No limits set, everything is unlimited."))
                ),
                InputRichBlockDivider(),
                InputRichBlockDetails(
                    summary=rich_text(("b", "How this works")),
                    blocks=[
                        InputRichBlockList(
                            items=[
                                InputRichBlockListItem(
                                    text=rich_text(
                                        "Send ",
                                        ("c", "0"),
                                        " to clear a limit and make it ",
                                        ("i", "unlimited"),
                                        ".",
                                    )
                                ),
                                InputRichBlockListItem(
                                    text=rich_text(
                                        "Sizes are in ",
                                        ("b", "GB"),
                                        " unless the table shows another unit.",
                                    )
                                ),
                                InputRichBlockListItem(
                                    text=rich_text(
                                        ("c", "CPU_LIMIT"),
                                        " is a percentage, ",
                                        ("c", "STATUS_LIMIT"),
                                        " counts messages.",
                                    )
                                ),
                                InputRichBlockListItem(
                                    text=rich_text(
                                        "Limits apply per task, not per user."
                                    )
                                ),
                            ],
                            ordered=False,
                        )
                    ],
                ),
                InputRichBlockFooter(
                    text=rich_text(
                        ("i", "Page "),
                        ("b", f"{int(start / 10) + 1}"),
                        ("i", f" of {-(-len(LIMIT_VARS) // 10)}"),
                    )
                ),
            ]
        )
    elif key == "private":
        if edit_mode:
            buttons.data_button("Stop Invoke File", "botset private stop", "header")
        else:
            buttons.data_button("Create New File", "botset private new")
            buttons.data_button("Add/Delete File", "botset private edit")
        buttons.data_button("Back", "botset back", position="footer")
        buttons.data_button(
            "Close", "botset close", position="footer", style=ButtonStyle.DANGER
        )
        txt = "\n┠ ".join(
            [
                f"<code>{fn}</code> → <b>{'Exists' if await aiopath.isfile(fn) else 'Not Exists'}</b>"
                for fn in [
                    "config.py",
                    "token.pickle",
                    "rclone.conf",
                    "accounts.zip",
                    "list_drives.txt",
                    "shortener.txt",
                    "categories.txt",
                    "cookies.txt",
                    "terabox.txt",
                    ".netrc",
                ]
            ]
        )
        msg = f"""⌬ <b>Private File Settings</b>
┠ <b>Dashboard :</b> 
┃
┠ {txt}
┃
┠ <b>Delete File</b> → Send the file name as text message, Like <code>rclone.conf</code>.
┃
┖ <b>Note:</b> Changes to private files take effect after restart."""
        if edit_mode:
            msg += "\n\n<i>Send the file name to delete the file, file to save the file & for new file create, follow below format.</i> \n\n<b>Format:</b> \n<code>file_name\n\ncontents of file</code></i>\n┖ <b>Time Left :</b> <code>60 sec</code>"
    else:
        msg = "Unknown option"

    return msg, buttons.build_menu(1 if key is None else 2, lb_cols=8)


async def update_buttons(message, key=None, edit_type=None, edit_mode=False):
    msg, button = await get_buttons(key, edit_type, edit_mode)
    await edit_message(message, msg, button)


@new_task
async def edit_variable(_, message, pre_message, key):
    handler_dict[message.chat.id] = False
    value = message.text
    if value.lower() == "true":
        value = True
    elif value.lower() == "false":
        value = False
        if key in ("INC_TASK_NOTIFY", "INC_TASK_RESUME") and Config.DATABASE_URL:
            await database.trunc_table("tasks")
    elif key == "STATUS_UPDATE_INTERVAL":
        value = int(value)
        if len(task_dict) != 0 and (st := intervals["status"]):
            for cid, intvl in list(st.items()):
                intvl.cancel()
                intervals["status"][cid] = SetInterval(
                    value, update_status_message, cid
                )
    elif key == "LEECH_SPLIT_SIZE":
        value = min(int(value), TgClient.MAX_SPLIT_SIZE)
    elif key == "EXCLUDED_EXTENSIONS":
        fx = value.split()
        excluded_extensions.clear()
        for x in fx:
            x = x.lstrip(".")
            excluded_extensions.append(x.strip().lower())
    elif key == "GDRIVE_ID":
        if drives_names and drives_names[0] == "Main":
            drives_ids[0] = value
        else:
            drives_ids.insert(0, value)
    elif key == "INDEX_URL":
        if drives_names and drives_names[0] == "Main":
            index_urls[0] = value
        else:
            index_urls.insert(0, value)
    elif key == "LINKS_LOG_ID":
        if value.strip():
            try:
                value = int(value.strip())
            except ValueError:
                await send_message(
                    message,
                    "Invalid value! LINKS_LOG_ID must be a valid integer chat ID.",
                )
                return await update_buttons(pre_message, "var")
    elif key == "MIRROR_LOG_ID":
        if value.strip():
            try:
                value = int(value.strip())
            except ValueError:
                await send_message(
                    message,
                    "Invalid value! MIRROR_LOG_ID must be a valid integer chat ID.",
                )
                return await update_buttons(pre_message, "var")
    elif key == "LEECH_LOG_CHAT":
        if value.strip():
            chat, thread = parse_dest(value.strip())
            if not isinstance(chat, int) or ("|" in value and thread is None):
                await send_message(
                    message,
                    "Invalid value! LEECH_LOG_CHAT must be a chat ID, "
                    "optionally with a topic id as chat_id|topic_id.",
                )
                return await update_buttons(pre_message, "var")
            value = value.strip() if thread else chat
    elif key == "LEECH_DUMP_CHATS":
        if isinstance(value, str):
            if value.startswith("{") and value.endswith("}"):
                try:
                    value = literal_eval(value)
                except Exception:
                    await send_message(message, "Invalid dict format!")
                    return await update_buttons(pre_message, "var")
            else:
                await send_message(
                    message,
                    'LEECH_DUMP_CHATS must be a dict. Format: {"A": -100123456}',
                )
                return await update_buttons(pre_message, "var")
        if not isinstance(value, dict):
            await send_message(
                message, 'LEECH_DUMP_CHATS must be a dict. Format: {"A": -100123456}'
            )
            return await update_buttons(pre_message, "var")
    elif key == "AUTHORIZED_CHATS":
        aid = value.split()
        auth_chats.clear()
        for id_ in aid:
            chat_id, *thread_ids = id_.split("|")
            chat_id = int(chat_id.strip())
            if thread_ids:
                thread_ids = list(map(lambda x: int(x.strip()), thread_ids))
                auth_chats[chat_id] = thread_ids
            else:
                auth_chats[chat_id] = []
    elif key == "SUDO_USERS":
        sudo_users.clear()
        aid = value.split()
        for id_ in aid:
            sudo_users.append(int(id_.strip()))
    elif key == "LOGIN_PASS":
        value = str(value)
    elif value.isdigit():
        value = int(value)
    elif value.startswith("[") and value.endswith("]"):
        try:
            value = literal_eval(value)
        except Exception:
            await send_message(message, "Invalid list/dict format!")
            return
    elif value.startswith("{") and value.endswith("}"):
        try:
            value = literal_eval(value)
        except Exception:
            await send_message(message, "Invalid dict format!")
            return
    if not isinstance(value, (str, int, float, bool, list, dict, type(None))):
        value = str(value)
    Config.set(key, value)
    if key == "CMD_SUFFIX":
        BotCommands.refresh_commands()
    await update_buttons(pre_message, key, "editvar", False)
    await delete_message(message)
    await database.update_config({key: value})
    if key in ["QUEUE_ALL", "QUEUE_DOWNLOAD", "QUEUE_UPLOAD"]:
        await start_from_queued()
    elif key in [
        "RCLONE_SERVE_URL",
        "RCLONE_SERVE_PORT",
        "RCLONE_SERVE_USER",
        "RCLONE_SERVE_PASS",
    ]:
        await rclone_serve_booter()


@new_task
async def toggle_bool_var(_, query, pre_message, key, value):
    handler_dict[query.message.chat.id] = False
    bool_value = value == "on"
    Config.set(key, bool_value)
    await update_buttons(pre_message, key, "editvar", False)
    await database.update_config({key: bool_value})
    if (
        key in ("INC_TASK_NOTIFY", "INC_TASK_RESUME")
        and not bool_value
        and Config.DATABASE_URL
    ):
        await database.trunc_table("tasks")
    elif key in ["QUEUE_ALL", "QUEUE_DOWNLOAD", "QUEUE_UPLOAD"]:
        await start_from_queued()


@new_task
async def toggle_onoff_var(_, query, pre_message, key, value):
    handler_dict[query.message.chat.id] = False
    bool_value = value == "on"
    Config.set(key, bool_value)
    await database.update_config({key: bool_value})
    await _handle_service_toggle(key, bool_value)
    await update_buttons(pre_message, "settoggle")


async def _handle_service_toggle(key, disabled):
    if key == "DISABLE_STREAM":
        from ..core.stream_server import spawn_stream_server, stop_stream_server
        if disabled:
            await stop_stream_server()
        else:
            spawn_stream_server()
    elif key == "DISABLE_PLUGINS":
        from ..core.plugin_manager import get_plugin_manager
        manager = get_plugin_manager()
        if disabled:
            await manager.unload_all()
        else:
            await manager.boot()


@new_task
async def show_var_value(_, query, key):
    value = f"{Config.get(key)}"
    if value == "":
        value = "None"
    if len(value) > 200:
        await query.answer()
        with BytesIO(str.encode(value)) as out_file:
            out_file.name = f"{key}.txt"
            await send_file(query.message, out_file)
    else:
        await query.answer(value, show_alert=True)


@new_task





async def update_private_file(_, message, pre_message, key, new_file=False):
    handler_dict[message.chat.id] = False
    if not message.media and (file_name := message.text):
        if new_file:
            file_name, content = file_name.split("\n", 1)
            file_name = file_name.strip()
            async with aiopen(file_name, "w") as f:
                await f.write(content.strip())
        else:
            if await aiopath.isfile(file_name) and file_name != "config.py":
                await remove(file_name)
            if file_name == "accounts.zip":
                if await aiopath.exists("accounts"):
                    await rmtree("accounts", ignore_errors=True)
                if await aiopath.exists("rclone_sa"):
                    await rmtree("rclone_sa", ignore_errors=True)
                Config.USE_SERVICE_ACCOUNTS = False
                await database.update_config({"USE_SERVICE_ACCOUNTS": False})
            elif file_name in [".netrc", "netrc"]:
                await cmd_exec(["touch", ".netrc"])
                await cmd_exec(["chmod", "600", ".netrc"])
                await cmd_exec(["cp", ".netrc", "/root/.netrc"])
        await delete_message(message)
    elif doc := message.document:
        file_name = doc.file_name
        fpath = f"{getcwd()}/{file_name}"
        if await aiopath.exists(fpath):
            await remove(fpath)
        await message.download(file_name=fpath)
        if file_name == "accounts.zip":
            if await aiopath.exists("accounts"):
                await rmtree("accounts", ignore_errors=True)
            if await aiopath.exists("rclone_sa"):
                await rmtree("rclone_sa", ignore_errors=True)
            await cmd_exec(
                ["7z", "x", "-o.", "-aoa", "accounts.zip", "accounts/*.json"]
            )
            await cmd_exec(["chmod", "-R", "777", "accounts"])
            if await aiopath.exists("accounts"):
                Config.USE_SERVICE_ACCOUNTS = True
                await database.update_config({"USE_SERVICE_ACCOUNTS": True})
        elif file_name in [".netrc", "netrc"]:
            if file_name == "netrc":
                await rename("netrc", ".netrc")
                file_name = ".netrc"
            await cmd_exec(["chmod", "600", ".netrc"])
            await cmd_exec(["cp", ".netrc", "/root/.netrc"])
        elif file_name == "config.py":
            await load_config()
        if "@github.com" in Config.UPSTREAM_REPO:
            buttons = ButtonMaker()
            msg = "Push to UPSTREAM_REPO ?"
            buttons.data_button(
                "Yes!", f"botset push {file_name}", style=ButtonStyle.SUCCESS
            )
            buttons.data_button("No", "botset close", style=ButtonStyle.DANGER)
            await send_message(message, msg, buttons.build_menu(2))
        else:
            await delete_message(message)
    if file_name == "rclone.conf":
        await rclone_serve_booter()
    elif file_name == "list_drives.txt" and await aiopath.exists("list_drives.txt"):
        drives_ids.clear()
        drives_names.clear()
        index_urls.clear()
        if Config.GDRIVE_ID:
            drives_names.append("Main")
            drives_ids.append(Config.GDRIVE_ID)
            index_urls.append(Config.INDEX_URL)
        async with aiopen("list_drives.txt", "r+") as f:
            lines = await f.readlines()
            for line in lines:
                temp = line.strip().split()
                drives_ids.append(temp[1])
                drives_names.append(temp[0].replace("_", " "))
                if len(temp) > 2:
                    index_urls.append(temp[2])
                else:
                    index_urls.append("")
    elif file_name == "shortener.txt" and await aiopath.exists("shortener.txt"):
        async with aiopen("shortener.txt", "r+") as f:
            lines = await f.readlines()
            for line in lines:
                temp = line.strip().split()
                if len(temp) == 2:
                    shortener_dict[temp[0]] = temp[1]
    elif file_name == "categories.txt" and await aiopath.exists("categories.txt"):
        categories_dict.clear()
        if Config.GDRIVE_ID:
            categories_dict["Root"] = {
                "drive_id": Config.GDRIVE_ID,
                "index_link": Config.INDEX_URL,
            }
        async with aiopen("categories.txt", "r+") as f:
            lines = await f.readlines()
            for line in lines:
                sep = 2 if line.strip().split()[-1].startswith("http") else 1
                temp = line.strip().rsplit(maxsplit=sep)
                name = "Root Custom" if temp[0].casefold() == "Root" else temp[0]
                categories_dict[name] = {
                    "drive_id": temp[1],
                    "index_link": (temp[2] if sep == 2 else ""),
                }
    await update_buttons(pre_message, key)
    await database.update_private_file(file_name)


async def event_handler(client, query, pfunc, rfunc, document=False):
    chat_id = query.message.chat.id
    handler_dict[chat_id] = True
    start_time = update_time = time()

    async def event_filter(_, __, event):
        user = event.from_user or event.sender_chat
        return bool(
            user.id == query.from_user.id
            and event.chat.id == chat_id
            and (event.text or event.document and document)
        )

    handler = client.add_handler(
        MessageHandler(pfunc, filters=create(event_filter)), group=-1
    )
    while handler_dict[chat_id]:
        await sleep(0.5)

        if time() - start_time > 60:
            handler_dict[chat_id] = False
            await rfunc()
        elif time() - update_time > 8 and handler_dict[chat_id]:
            update_time = time()
            msg = await client.get_messages(chat_id, query.message.id)
            text = msg.text.split("\n")
            text[-1] = (
                f"┖ <b>Time Left :</b> <code>{round(60 - (time() - start_time), 2)} sec</code>"
            )
            await edit_message(msg, "\n".join(text), msg.reply_markup)
    client.remove_handler(*handler)


@new_task
async def edit_bot_settings(client, query):
    data = query.data.split()
    message = query.message
    handler_dict[message.chat.id] = False
    if data[1] == "close":
        await query.answer()
        await delete_message(message.reply_to_message)
        await delete_message(message)
    elif data[1] == "back":
        await query.answer()
        key = data[2] if len(data) == 3 else None
        if key is None:
            globals()["start"] = 0
        await update_buttons(message, key)
    elif data[1] in [
        "var",
        "setonoff",
        "settoggle",
        "setlimit",
    ] or data[
        1
]:
        if data[1] in ("setlimit",):
            globals()["start"] = 0
        await query.answer()
        await update_buttons(message, data[1])
    elif data[1] == "resetvar":
        await query.answer()
        value = DEFAULT_CONFIG.get(data[2], "")
        if data[2] in (
            "IMAGES",
            "YT_TAGS",
            "IMG_SOURCES",
        ):
            value = []
        elif data[2] in DEFAULT_VALUES:
            value = DEFAULT_VALUES[data[2]]
            if (
                data[2] == "STATUS_UPDATE_INTERVAL"
                and len(task_dict) != 0
                and (st := intervals["status"])
            ):
                for key, intvl in list(st.items()):
                    intvl.cancel()
                    intervals["status"][key] = SetInterval(
                        value, update_status_message, key
                    )
        elif data[2] == "EXCLUDED_EXTENSIONS":
            excluded_extensions.clear()
        elif data[2] in ("BASE_URL", "WEB_ACCESS_PASSWORD"):
            await cmd_exec(["pkill", "-9", "-f", "gunicorn"])
        elif data[2] == "GDRIVE_ID":
            if drives_names and drives_names[0] == "Main":
                drives_names.pop(0)
                drives_ids.pop(0)
                index_urls.pop(0)
        elif data[2] == "INDEX_URL":
            if drives_names and drives_names[0] == "Main":
                index_urls[0] = ""
        elif data[2] in ("INC_TASK_NOTIFY", "INC_TASK_RESUME"):
            await database.trunc_table("tasks")
        elif data[2] == "AUTHORIZED_CHATS":
            auth_chats.clear()
        elif data[2] == "SUDO_USERS":
            sudo_users.clear()
        Config.set(data[2], value)
        await update_buttons(message, data[2], "editvar", False)
        if data[2] == "DATABASE_URL":
            await database.disconnect()
        await database.update_config({data[2]: value})
        if data[2] in ("QUEUE_ALL", "QUEUE_DOWNLOAD", "QUEUE_UPLOAD"):
            await start_from_queued()
        elif data[2] in (
            "RCLONE_SERVE_URL",
            "RCLONE_SERVE_PORT",
            "RCLONE_SERVE_USER",
            "RCLONE_SERVE_PASS",
        ):
            await rclone_serve_booter()
    elif data[1] == "private":
        await query.answer()
        if data[2] in ("open", "stop"):
            await update_buttons(message, data[1])
        elif data[2] in ("edit", "new"):
            await update_buttons(message, data[1], edit_mode=True)
            pfunc = partial(
                update_private_file,
                pre_message=message,
                key=data[1],
                new_file=data[2] == "new",
            )
            rfunc = partial(update_buttons, message, data[1])
            await event_handler(client, query, pfunc, rfunc, True)
    elif data[1] == "editvar":
        await query.answer()
        key = data[2]
        edit_mode = len(data) > 3 and data[3] == "edit"
        if edit_mode:
            await update_buttons(message, key, "editvar", True)
            pfunc = partial(edit_variable, pre_message=message, key=key)
            rfunc = partial(update_buttons, message, key, "editvar", False)
            await event_handler(client, query, pfunc, rfunc)
        else:
            await update_buttons(message, key, "editvar", False)
    elif data[1] == "boolvar":
        await query.answer()
        key = data[2]
        value = data[3]
        await toggle_bool_var(client, query, message, key, value)
    elif data[1] == "toggleonoff":
        await query.answer()
        key = data[2]
        value = data[3]
        await toggle_onoff_var(client, query, message, key, value)
    elif data[1] == "showvar":
        key = data[2]
        await show_var_value(client, query, key)


@new_task
async def send_bot_settings(_, message):
    handler_dict[message.chat.id] = False
    msg, button = await get_buttons()
    globals()["start"] = 0
    await send_message(message, msg, button)
