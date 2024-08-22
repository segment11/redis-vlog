package script

import redis.command.SGroup
import redis.command.SentinelCommand

def sGroup = super.binding.getProperty('sGroup') as SGroup

def sentinelCommand = new SentinelCommand(sGroup)
sentinelCommand.from(sGroup)
sentinelCommand.handle()