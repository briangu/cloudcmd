package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.engine.BlockCacheService;
import cloudcmd.common.index.IndexStorageService;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import ops.AsyncCommand;
import ops.CommandContext;
import ops.MemoryElement;
import org.json.JSONArray;


public class pull_block implements AsyncCommand
{
  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    String hash = (String) args[0];

    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    if (!hashProviders.containsKey(hash))
    {
      System.err.println("unexpected: could not find block in existing storage!");
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

    if (hash.startsWith(".meta"))
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
      try
      {
        FileMetaData fmd = new FileMetaData();

        InputStream remoteData = adapter.load(hash);

        BlockCacheService.instance().getBlockCache().store(remoteData, hash);

        fmd.Meta = JsonUtil.loadJson(BlockCacheService.instance().getBlockCache().load(hash));
        fmd.MetaHash = hash;
        fmd.BlockHashes = fmd.Meta.getJSONArray("blocks");
        fmd.Tags = adapter.loadTags(hash);

        // if localcache has block continue
        IndexStorageService.instance().add(fmd);

        if (retrieveBlocks)
        {
          JSONArray blocks = fmd.BlockHashes;

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
        e.printStackTrace();
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
      try
      {
        InputStream remoteData = adapter.load(hash);
        BlockCacheService.instance().getBlockCache().store(remoteData, hash);
        success = true;
        break;
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }

    return success;
  }
}
