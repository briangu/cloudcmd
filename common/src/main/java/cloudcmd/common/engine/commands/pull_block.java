package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.engine.LocalCacheService;
import cloudcmd.common.index.IndexStorageService;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import ops.AsyncCommand;
import ops.Command;
import ops.CommandContext;
import ops.MemoryElement;
import org.json.JSONArray;


public class pull_block implements AsyncCommand
{
  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    String hash = (String) args[1];

    Map<String, List<Adapter>> hashProviders = CloudEngineService.instance().getHashProviders();

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

    if (hash.endsWith(".meta"))
    {
      Boolean retrieveBlocks = (Boolean)args[2];
      success = pullMetaBlock(context, blockProviders, retrieveBlocks, hash);
    }
    else
    {
      success = pullFileBlock(context, blockProviders, hash);
    }

    if (success)
    {
      context.make(new MemoryElement("msg", "successfully pulled block {0}", hash));
    }
    else
    {
      context.make(new MemoryElement("msg", "failed to pull block {0}", hash));
      context.make(new MemoryElement("recover_block", hash));
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

        LocalCacheService.instance().store(remoteData, hash);

        fmd.Meta = JsonUtil.loadJson(LocalCacheService.instance().load(hash));
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
            if (LocalCacheService.instance().contains(blockHash)) continue;
            context.make(new MemoryElement("pull_block", blockHash));
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
        LocalCacheService.instance().store(remoteData, hash);
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
