package cloudcmd.common.engine.commands;


import cloudcmd.common.FileUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.BlockCacheService;
import ops.Command;
import ops.CommandContext;
import ops.MemoryElement;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.*;


public class push_block implements Command
{
  static Logger log = Logger.getLogger(push_block.class);

  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    Adapter dest = (Adapter)args[0];
    String hash = (String) args[1];

    if (dest.contains(hash))
    {
      context.make(new MemoryElement("msg", "body", String.format("adapter %s already has block %s", dest.URI, hash)));
      return;
    }
    if (dest.IsFull())
    {
      context.make(new MemoryElement("msg", "body", String.format("adapter %s is full and cannot write %s", dest.URI, hash)));
      return;
    }
    if (!dest.IsOnLine())
    {
      context.make(new MemoryElement("msg", "body", String.format("adapter %s is offline and cannot write %s", dest.URI, hash)));
      return;
    }

    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    if (!hashProviders.containsKey(hash))
    {
      System.err.println();
      System.err.println(String.format("push_block: could not find block %s in existing storage!", hash));
      System.err.println();
      return;
    }

    List<Adapter> blockProviders = new ArrayList<Adapter>(hashProviders.get(hash));
    Collections.shuffle(blockProviders);
    Collections.sort(blockProviders, new Comparator<Adapter>()
    {
      @Override
      public int compare(Adapter o1, Adapter o2)
      {
        return o1.Tier.compareTo(o2.Tier);
      }
    });

    for (Adapter src : blockProviders)
    {
      if (!src.IsOnLine()) continue;

      InputStream is = null;

      try
      {
        is = src.load(hash);
        dest.store(is, hash);
        break;
      }
      catch (Exception e)
      {
        context.make("error_push_block", "hash", hash);
        log.error(hash, e);
      }
      finally
      {
        FileUtil.SafeClose(is);
      }
    }
  }
}
