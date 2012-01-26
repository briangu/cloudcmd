package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.FileUtil;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.MetaUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.BlockCacheService;
import cloudcmd.common.index.IndexStorageService;
import java.io.InputStream;
import java.util.*;

import ops.AsyncCommand;
import ops.CommandContext;
import ops.MemoryElement;
import org.apache.log4j.Logger;
import org.json.JSONArray;


public class pull_block implements AsyncCommand
{
  static Logger log = Logger.getLogger(pull_block.class);

  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    String hash = (String) args[0];

    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    if (!hashProviders.containsKey(hash))
    {
      System.err.println();
      System.err.println(String.format("unexpected: could not find block %s in existing storage!", hash));
      System.err.println();
      return;
    }

    List<Adapter> blockProviders = hashProviders.get(hash);

    Collections.sort(blockProviders, new Comparator<Adapter>()
    {
      @Override
      public int compare(Adapter o1, Adapter o2)
      {
        return o1.Tier.compareTo(o2.Tier);
      }
    });

    boolean success;

    if (hash.endsWith(".meta"))
    {
      Boolean retrieveBlocks = (Boolean)args[1];
      success = pullMetaBlock(context, blockProviders, retrieveBlocks, hash);
    }
    else
    {
      success = pullFileBlock(context, blockProviders, hash);
    }

    if (success)
    {
      context.make(new MemoryElement("msg", "body", String.format("successfully pulled block %s", hash)));
    }
    else
    {
      context.make(new MemoryElement("msg", "body", String.format("failed to pull block %s", hash)));
      context.make(new MemoryElement("recover_block", "hash", hash));
    }
  }

  private boolean pullMetaBlock(
      CommandContext context,
      List<Adapter> blockProviders,
      boolean retrieveBlocks,
      String hash)
  {
    boolean success = false;

    for (Adapter adapter : blockProviders)
    {
      InputStream remoteData = null;
      try
      {
        remoteData = adapter.load(hash);

        BlockCacheService.instance().getBlockCache().store(remoteData, hash);

        FileMetaData fmd = MetaUtil.loadMeta(hash, JsonUtil.loadJson(BlockCacheService.instance().getBlockCache().load(hash)));

        // if localcache has block continue
        IndexStorageService.instance().add(fmd);

        if (retrieveBlocks)
        {
          JSONArray blocks = fmd.getBlockHashes();

          for (int i = 0; i < blocks.length(); i++)
          {
            String blockHash = blocks.getString(i);
            if (BlockCacheService.instance().getBlockCache().contains(blockHash)) continue;
            context.make(new MemoryElement("pull_block", "hash", blockHash));
          }
        }
        success = true;
        break;
      }
      catch (Exception e)
      {
        context.make("error_pull_block", "hash", hash);
        log.error(hash, e);
      }
      finally
      {
        FileUtil.SafeClose(remoteData);
      }
    }

    return success;
  }

  private boolean pullFileBlock(
      CommandContext context,
      List<Adapter> blockProviders,
      String hash)
  {
    boolean success = false;

    for (Adapter adapter : blockProviders)
    {
      InputStream remoteData = null;
      try
      {
        remoteData = adapter.load(hash);
        BlockCacheService.instance().getBlockCache().store(remoteData, hash);
        success = true;
        break;
      }
      catch (Exception e)
      {
        context.make("error_pull_block", "hash", hash);
        log.error(hash, e);
      }
      finally
      {
        FileUtil.SafeClose(remoteData);
      }
    }

    return success;
  }
}
