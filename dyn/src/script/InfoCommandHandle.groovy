package script

import redis.command.IGroup
import redis.command.InfoCommand

def iGroup = super.binding.getProperty('iGroup') as IGroup

def infoCommand = new InfoCommand(iGroup)
infoCommand.from(iGroup)
infoCommand.handle()