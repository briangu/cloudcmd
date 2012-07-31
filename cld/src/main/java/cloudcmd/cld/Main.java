package cloudcmd.cld;

import cloudcmd.cld.commands.*;
import cloudcmd.common.FileUtil;
import cloudcmd.common.engine.CloudEngineListener;
import jpbetz.cli.CommandSet;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.SynchronousQueue;

public class Main
{

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception
  {
    String configRoot = FileUtil.findConfigDir(FileUtil.getCurrentWorkingDirectory(), ".cld");
    if (configRoot == null)
    {
      configRoot = System.getenv("HOME") + File.separator + ".cld";
      new File(configRoot).mkdir();
    }

    final Boolean[] event = new Boolean[1];
    final BlockingQueue<String> queue = new SynchronousQueue<String>();
    Thread msgPump = new Thread(new Runnable() {
      @Override
      public void run() {
        while(!event[0]) {
          String msg = queue.poll();
          System.err.println(msg);
        }
      }
    });

    try
    {
      ConfigStorageService.instance().init(configRoot);
      IndexStorageService.instance().init(configRoot);
      CloudEngineService.instance().init(ConfigStorageService.instance(), IndexStorageService.instance());
      CloudEngineService.instance().registerListener(new CloudEngineListener() {
        @Override
        public void onMessage(String msg) {
          synchronized (queue) {
            queue.offer(msg);
          }
        }
      });

      msgPump.start();
      CloudEngineService.instance().run();

      CommandSet app = new CommandSet("cld");
      app.addSubCommands(Adapter.class);
      app.addSubCommands(Find.class);
      app.addSubCommands(Get.class);
      app.addSubCommands(Index.class);
      app.addSubCommands(Init.class);
      app.addSubCommands(Listing.class);
      app.addSubCommands(Print.class);
      app.addSubCommands(Reindex.class);
      app.addSubCommands(Remove.class);
      app.addSubCommands(Sync.class);
      app.addSubCommands(Tag.class);
      app.addSubCommands(Update.class);
      app.addSubCommands(Verify.class);
      app.invoke(args);
    }
    finally
    {
      CloudEngineService.instance().shutdown();
      IndexStorageService.instance().shutdown();
      ConfigStorageService.instance().shutdown();

      event[0] = true;
      queue.offer("done.");
      msgPump.interrupt();
    }
  }
}
