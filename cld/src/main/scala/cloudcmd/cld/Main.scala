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

    val event: Array[Boolean] = new Array[Boolean](1)
    event(0) = false

    val queue = new SynchronousQueue[String]

    val msgPump: Thread = new Thread(new Runnable {
      def run {
        while (!event(0)) {
          try {
            val msg: String = queue.take
            System.err.println(msg)
          }
          catch {
            case e: InterruptedException => {
            }
          }
        }
      }
    })

    val startTime: Long = System.currentTimeMillis

    try {
      val listener: Main.Listener = new Main.Listener(queue)
      CloudServices.ConfigService.getReplicationStrategy.registerListener(listener)
      CloudServices.CloudEngine.registerListener(listener)
      CloudServices.IndexStorage.registerListener(listener)
      CloudServices.FileProcessor.registerListener(listener)

      CloudServices.ConfigService.init(configRoot)
      CloudServices.CloudEngine.init
      CloudServices.IndexStorage.init(configRoot)

      msgPump.start
      CloudServices.CloudEngine.run
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
      app.addSubCommands(classOf[Update])
      app.invoke(args)
    }
    finally {
      CloudServices.shutdown
      event(0) = true
      msgPump.interrupt
    }

    println("took %6d ms to run".format(((System.currentTimeMillis - startTime))))
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