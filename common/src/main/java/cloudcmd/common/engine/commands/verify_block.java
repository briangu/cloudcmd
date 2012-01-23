package cloudcmd.common.engine.commands;


import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.BlockCacheService;
import ops.AsyncCommand;
import ops.CommandContext;
import ops.MemoryElement;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;


public class verify_block implements AsyncCommand
{
  static Logger log = Logger.getLogger(verify_block.class);

  @Override
  public void exec(CommandContext context, Object[] args)
    throws Exception
  {
    String hash = (String) args[0];
    Boolean deleteOnInvalid = (Boolean) args[1];
    
    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    if (!hashProviders.containsKey(hash))
    {
      context.make("msg", "body", String.format("could not find block %s in existing storage!", hash));
      return;
    }

    List<Adapter> blockProviders = hashProviders.get(hash);

    for (Adapter adapter : blockProviders)
    {
      try
      {
        boolean isValid = adapter.verify(hash);
        if (isValid)
        {
          context.make(new MemoryElement("msg", "body", String.format("successfully validated block %s is on adapter %s", hash, adapter.URI)));
        }
        else
        {
          context.make(new MemoryElement("msg", "body", String.format("bad block %s found on adapter %s", hash, adapter.URI)));
          if (deleteOnInvalid)
          {
            context.make(new MemoryElement("remove_block", "hash", hash, "adapterURI", adapter.URI));
          }
        }
      }
      catch (Exception e)
      {
        context.make(new MemoryElement("msg", "body", String.format("failed to verify block %s on adapter %s", hash, adapter.URI)));
        log.error(hash, e);
      }
    }
  }
}
