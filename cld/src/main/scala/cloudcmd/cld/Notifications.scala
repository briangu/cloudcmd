package cloudcmd.cld

import cloudcmd.common.engine.NotificationCenter

object Notifications {

  val ConsoleMessage = "ConsoleMessage"

  val AdapterNotFound = "AdapterNotFound"
  val AdapterTierRange = "AdapterTierRange"
  val AdapterMatched = "AdapterMatched"

  val AdapterURI = "AdapterURI"

  val NothingToDo = "NothingToDo"

  def msg(msg: String) {
    NotificationCenter.defaultCenter.postNotification(ConsoleMessage, msg)
  }


}
