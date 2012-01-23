package cloudcmd.common.engine.commands;


import cloudcmd.common.adapters.Adapter;
import ops.AsyncCommand;
import ops.CommandContext;
import ops.MemoryElement;
import org.apache.log4j.Logger;


public class remove_block implements AsyncCommand
{
  static Logger log = Logger.getLogger(remove_block.class);

  @Override
  public void exec(CommandContext context, Object[] args)
    throws Exception
  {
    String hash = (String) args[0];
    Adapter adapter = (Adapter) args[1];

    try
    {
      context.make(new MemoryElement("msg", "body", String.format("deleting block %s found on adapter %s", hash, adapter.URI)));
      boolean deleteSuccess = adapter.remove(hash);
      if (deleteSuccess)
      {
        context.make(new MemoryElement("msg", "body", String.format("successfully deleted block %s found on adapter %s", hash, adapter.URI)));
      }
      else
      {
        context.make(new MemoryElement("msg", "body", String.format("failed to delete block %s found on adapter %s", hash, adapter.URI)));
      }
    }
    catch (Exception e)
    {
      context.make(new MemoryElement("msg", "body", String.format("failed to delete block %s on adapter %s", hash, adapter.URI)));
      log.error(hash, e);
    }
  }
}
