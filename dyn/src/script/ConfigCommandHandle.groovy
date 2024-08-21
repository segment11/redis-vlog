package script

import redis.command.CGroup
import redis.command.ConfigCommand

def cGroup = super.binding.getProperty('cGroup') as CGroup

def configCommand = new ConfigCommand(cGroup)
configCommand.from(cGroup)
configCommand.handle()