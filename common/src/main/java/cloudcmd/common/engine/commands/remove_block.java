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
    if (hash == null)
    {
      throw new IllegalArgumentException("hash is null");
    }

    Adapter adapter = (Adapter) args[1];
    if (adapter == null)
    {
      throw new IllegalArgumentException("adapter is null");
    }

    try
    {
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
