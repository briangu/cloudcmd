package cloudcmd.cld.commands

import jpbetz.cli.{CommandContext, Command}
import cloudcmd.common.engine.NotificationCenter._
import cloudcmd.cld.Notifications._

abstract class CloudCommand extends Command {

  def nothingToDo() {
    msg("nothing to do.")
  }

  def exec(p1: CommandContext) {

    doCommand()

    defaultCenter.removeObserver(this, None, None)
  }

  def doCommand()
}
