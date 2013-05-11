package cloudcmd.cld

import cloudcmd.cld.commands._
import cloudcmd.common.FileUtil
import cloudcmd.common.engine.EngineEventListener
import jpbetz.cli.CommandSet
import java.io.File
import java.util.concurrent.BlockingQueue
import java.util.concurrent.SynchronousQueue

object Main {
  @SuppressWarnings(Array("unchecked"))
  def main(args: Array[String]) {

    var configRoot: String = FileUtil.findConfigDir(FileUtil.getCurrentWorkingDirectory, ".cld")
    if (configRoot == null) {
      configRoot = System.getenv("HOME") + File.separator + ".cld"
      new File(configRoot).mkdir
    }

    try {
      CloudServices.setListener(new Main.Listener(queue))
      CloudServices.setConfigRoot(configRoot)

      msgPump.start()
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
      event(0) = true
      msgPump.interrupt()
      msgPump.join()
    }
  }

  private class Listener extends EngineEventListener {
    def this(queue: BlockingQueue[String]) {
      this()
      _queue = queue
    }

    def onMessage(msg: String) {
      _queue.offer(msg)
    }

    private var _queue: BlockingQueue[String] = null
  }

}