from ...core.config_manager import Config


class BotCommands:
    StartCommand = "start"
    LoginCommand = "login"

    _static_commands = {
        "Mirror": ["mirror", "m"],
        "UpHoster": ["uphoster", "up"],
        "Leech": ["leech", "l"],
        "Clone": ["clone", "cl"],
        "Count": "count",
        "Delete": "del",
        "List": "list",
        "Users": "users",
        "CancelTask": ["cancel", "c"],
        "CancelAll": ["cancelall", "call"],
        "ForceStart": ["forcestart", "fs"],
        "Status": ["status", "s", "statusall"],
        "Stream": ["stream", "sl"],
        "Ping": "ping",
        "Restart": ["restart", "r", "restartall"],
        "RestartSessions": ["restartses", "rses"],
        "Broadcast": ["broadcast", "bc"],
        "Stats": ["stats", "st"],
        "Help": ["help", "h"],
        "Log": "log",
        "Shell": "shell",
        "AExec": "aexec",
        "Exec": "exec",
        "ClearLocals": "clearlocals",
        "AddImage": ["addimage", "ai"],
        "Images": ["images", "img"],
        "Authorize": ["authorize", "a"],
        "UnAuthorize": ["unauthorize", "ua"],
        "AddSudo": ["addsudo", "as"],
        "RmSudo": ["rmsudo", "rs"],
        "BlackList": ["blacklist", "bl"],
        "RmBlackList": ["rmblacklist", "rbl"],
        "BotSet": ["bsetting", "bs"],
        "UserSet": ["usetting", "us"],
        "CategorySelect": ["category", "ctsel"],
        "GDClean": ["gdclean", "gdc"],
        "Memory": ["memory", "mem"],
    }

    @classmethod
    def get_commands(cls):
        commands = {
            key: (list(value) if isinstance(value, list) else value)
            for key, value in cls._static_commands.items()
        }

        return commands

    @classmethod
    def _build_command_vars(cls):
        commands = cls.get_commands()

        for key, cmds in commands.items():
            setattr(
                cls,
                f"{key}Command",
                (
                    [
                        (
                            f"{cmd}{Config.CMD_SUFFIX}"
                            if cmd not in ["restartall", "statusall"]
                            else cmd
                        )
                        for cmd in cmds
                    ]
                    if isinstance(cmds, list)
                    else f"{cmds}{Config.CMD_SUFFIX}"
                ),
            )

    @classmethod
    def refresh_commands(cls):
        cls._build_command_vars()


BotCommands._build_command_vars()
