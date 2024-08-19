package script

import redis.command.MGroup
import redis.command.ManageCommand

def mGroup = super.binding.getProperty('mGroup') as MGroup

def manageCommand = new ManageCommand(mGroup)
manageCommand.from(mGroup)
manageCommand.handle()