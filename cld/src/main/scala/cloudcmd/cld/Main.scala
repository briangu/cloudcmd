package cloudcmd.cld

import cloudcmd.cld.commands._
import cloudcmd.common.FileUtil
import jpbetz.cli.CommandSet
import java.io.File
import cloudcmd.common.engine.NotificationCenter

object Main {

  class ConsoleObserver {

    def apply(userInfo: Option[Map[String, Any]]) {

    }

  }

  @SuppressWarnings(Array("unchecked"))
  def main(args: Array[String]) {

    var configRoot: String = FileUtil.findConfigDir(FileUtil.getCurrentWorkingDirectory, ".cld")
    if (configRoot == null) {
      configRoot = System.getenv("HOME") + File.separator + ".cld"
      new File(configRoot).mkdir
    }

    NotificationCenter.start()

    val consoleObserver = new ConsoleObserver

    NotificationCenter.defaultCenter.addObserverForName(consoleObserver, "", None, (userInfo: Option[Map[String, Any]]) => {})
    NotificationCenter.defaultCenter.addObserverForName(consoleObserver, "", None, consoleObserver.apply)

    try {
      CloudServices.setConfigRoot(configRoot)

      val app: CommandSet = new CommandSet("cld")
      app.addSubCommands(classOf[Adapter])
      app.addSubCommands(classOf[Find])
      app.addSubCommands(classOf[Get])
      app.addSubCommands(classOf[Add])
      app.addSubCommands(classOf[Init])
      app.addSubCommands(classOf[Listing])
      app.addSubCommands(classOf[Print])
      app.addSubCommands(classOf[Reindex])
      app.addSubCommands(classOf[Remove])
      app.addSubCommands(classOf[Ensure])
      app.addSubCommands(classOf[Tag])
      app.invoke(args)
    }
    finally {
      CloudServices.shutdown()
      NotificationCenter.shutdown()
    }
  }
}