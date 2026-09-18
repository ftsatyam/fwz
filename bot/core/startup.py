from asyncio import gather
from importlib import import_module
from os import environ, path as ospath, getenv

from aiofiles import open as aiopen
from aiofiles.os import makedirs, remove, path as aiopath
from aioshutil import rmtree


from .. import (
    LOGGER,
    bot_loop,
    auth_chats,
    categories_dict,
    drives_ids,
    drives_names,
    index_urls,
    list_drives_dict,
    shortener_dict,
    var_list,
    user_data,
    excluded_extensions,
    sudo_users,
)
from ..helper.ext_utils.bot_utils import cmd_exec, derive_service_password
from ..helper.ext_utils.db_handler import database
from .config_manager import Config
from .tg_client import TgClient, db_partition_id


async def load_settings():
    if not Config.DATABASE_URL:
        return
    for p in ["thumbnails", "tokens", "rclone"]:
        if await aiopath.exists(p):
            await rmtree(p, ignore_errors=True)
    await database.connect()
    if database.db is not None:
        if TgClient.PARTITION:
            PART = str(TgClient.PARTITION)
        else:
            BOT_ID = Config.BOT_TOKEN.split(":", 1)[0]
            PART = db_partition_id(BOT_ID)
            TgClient.PARTITION = PART
        deploy_filter = {"_id": PART}
        try:
            settings = import_module("config")
            config_file = {
                key: value.strip() if isinstance(value, str) else value
                for key, value in vars(settings).items()
                if not key.startswith("__")
            }
        except ModuleNotFoundError:
            config_file = {}
        config_file.update(
            {
                key: value.strip() if isinstance(value, str) else value
                for key, value in environ.items()
                if key in var_list
            }
        )

        old_config = await database.db.settings.deployConfig.find_one(
            deploy_filter, {"_id": 0}
        )

        results = await gather(
            database.db.settings.config.find_one(deploy_filter, {"_id": 0}),
            database.db.settings.files.find_one(deploy_filter, {"_id": 0}),
            database.db.users[PART].find_one(),
        )

        (
            config_dict,
            pf_dict,
            user_exists,
        ) = results

        if old_config is None:
            await database.db.settings.deployConfig.replace_one(
                deploy_filter, config_file, upsert=True
            )
            config_dict = config_dict or {}
            for k, v in config_file.items():
                if v is not None:
                    config_dict.setdefault(k, v)
        elif old_config != config_file:
            LOGGER.info(
                "Updating.. Deploy Config changed, merging new config.py values"
            )
            config_dict = config_dict or {}
            for k, v in config_file.items():
                if k not in old_config or old_config.get(k) != v:
                    if v is not None:
                        config_dict[k] = v
            await database.db.settings.deployConfig.replace_one(
                deploy_filter, config_file, upsert=True
            )
        else:
            LOGGER.info("Updating.. Saved Config imported from MongoDB")
            config_dict = config_dict or {}

        if config_dict:
            Config.load_dict(config_dict)

        if pf_dict:
            for key, value in pf_dict.items():
                if value:
                    file_ = key.replace("__", ".")
                    async with aiopen(file_, "wb+") as f:
                        await f.write(value)



async def save_settings():
    if database.db is None:
        return
    config_file = Config.get_all()
    if TgClient.PARTITION:
        PART = str(TgClient.PARTITION)
    else:
        PART = db_partition_id(TgClient.ID)
        TgClient.PARTITION = PART
    deploy_filter = {"_id": PART}
    await database.db.settings.config.update_one(
        deploy_filter, {"$set": config_file}, upsert=True
    )


async def update_variables():
    if (
        Config.LEECH_SPLIT_SIZE > TgClient.MAX_SPLIT_SIZE
        or Config.LEECH_SPLIT_SIZE == 2097152000
        or not Config.LEECH_SPLIT_SIZE
    ):
        Config.LEECH_SPLIT_SIZE = TgClient.MAX_SPLIT_SIZE

    if Config.AUTHORIZED_CHATS:
        aid = Config.AUTHORIZED_CHATS.split()
        for id_ in aid:
            chat_id, *thread_ids = id_.split("|")
            chat_id = int(chat_id.strip())
            if thread_ids:
                thread_ids = list(map(lambda x: int(x.strip()), thread_ids))
                auth_chats[chat_id] = thread_ids
            else:
                auth_chats[chat_id] = []

    if Config.SUDO_USERS:
        aid = Config.SUDO_USERS.split()
        for id_ in aid:
            sudo_users.append(int(id_.strip()))

    if Config.EXCLUDED_EXTENSIONS:
        fx = Config.EXCLUDED_EXTENSIONS.split()
        for x in fx:
            x = x.lstrip(".")
            excluded_extensions.append(x.strip().lower())

    if Config.GDRIVE_ID:
        drives_names.append("Main")
        drives_ids.append(Config.GDRIVE_ID)
        index_urls.append(Config.INDEX_URL)
        list_drives_dict["Main"] = {
            "drive_id": Config.GDRIVE_ID,
            "index_link": Config.INDEX_URL,
        }
        categories_dict["Root"] = {
            "drive_id": Config.GDRIVE_ID,
            "index_link": Config.INDEX_URL,
        }

    if await aiopath.exists("list_drives.txt"):
        async with aiopen("list_drives.txt", "r+") as f:
            lines = await f.readlines()
            for line in lines:
                temp = line.split()
                drives_ids.append(temp[1])
                drives_names.append(temp[0].replace("_", " "))
                if len(temp) > 2:
                    index_urls.append(temp[2])
                else:
                    index_urls.append("")

                sep = 2 if temp[-1].startswith("http") else 1
                tmp = line.strip().rsplit(maxsplit=sep)
                name = "Main Custom" if tmp[0].casefold() == "Main" else tmp[0]
                list_drives_dict[name] = {
                    "drive_id": tmp[1],
                    "index_link": (tmp[2] if sep == 2 else ""),
                }

    if await aiopath.exists("shortener.txt"):
        async with aiopen("shortener.txt", "r+") as f:
            lines = await f.readlines()
            for line in lines:
                temp = line.strip().split()
                if len(temp) == 2:
                    shortener_dict[temp[0]] = temp[1]

    if await aiopath.exists("categories.txt"):
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


async def load_configurations():
    if not await aiopath.exists(".netrc"):
        async with aiopen(".netrc", "w"):
            pass

    if await aiopath.exists("accounts.zip"):
        if await aiopath.exists("accounts"):
            await rmtree("accounts", ignore_errors=True)
        await cmd_exec(["7z", "x", "-o.", "-aoa", "accounts.zip", "accounts/*.json"])
        await cmd_exec(["chmod", "-R", "777", "accounts"])
        await remove("accounts.zip")

    if not await aiopath.exists("accounts"):
        Config.USE_SERVICE_ACCOUNTS = False

    await apply_tunnel_url_once()
