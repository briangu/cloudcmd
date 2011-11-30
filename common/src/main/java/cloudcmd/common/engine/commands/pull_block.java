package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.LocalCacheService;
import cloudcmd.common.index.IndexStorageService;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import ops.Command;
import ops.CommandContext;
import ops.MemoryElement;
import org.json.JSONArray;


public class pull_block implements Command
{
  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    Map<String, List<Adapter>> hashProviders = (Map<String, List<Adapter>>)args[0];
    String hash = (String) args[1];

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

    for (Adapter adapter : blockProviders)
    {
      if (hash.endsWith(".meta"))
      {
        try
        {
          Boolean retrieveBlocks = (Boolean)args[2];

          FileMetaData fmd = new FileMetaData();

          LocalCacheService.instance().store(adapter.load(hash), hash);

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
              context.make(new MemoryElement("pull_block", hashProviders, blockHash));
            }
          }
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
      else
      {
        LocalCacheService.instance().store(adapter.load(hash), hash);
      }

      break;
    }
  }
}
