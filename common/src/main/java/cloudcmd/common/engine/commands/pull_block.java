package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.index.IndexStorageService;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
    List<Adapter> blockProviders = (List<Adapter>)args[0];
    String hash = (String) args[1];

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
      try
      {
        FileMetaData fmd = new FileMetaData();

        fmd.Meta = JsonUtil.loadJson(adapter.load(hash));
        fmd.MetaHash = hash;
        fmd.BlockHashes = fmd.Meta.getJSONArray("blocks");
        fmd.Tags = adapter.loadTags(hash);

        if (!localDescription.contains(hash))
        {
          _ops.make(new MemoryElement("push_block", localCache, adapter, hash));
        }

        IndexStorageService.instance().add(fmd);

        if (retrieveBlocks)
        {
          JSONArray blocks = fmd.BlockHashes;

          for (int i = 0; i < blocks.length(); i++)
          {
            String blockHash = blocks.getString(i);
            if (localDescription.contains(blockHash)) continue;
            _ops.make(new MemoryElement("push_block", localCache, adapter, blockHash));
          }
        }
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
  }
}
