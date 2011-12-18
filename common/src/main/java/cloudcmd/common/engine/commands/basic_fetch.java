package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.FileUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.BlockCacheService;
import ops.AsyncCommand;
import ops.CommandContext;
import ops.MemoryElement;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;


/*
  This is a basic fetcher that just does simple attempt to build a file from walking the sub-blocks
  What we really want to do is to fan out the sub-block requests to sub-fetchers and have each sub-fetcher write into
  the appropriate target file region.
  We also want to enable the ability to recover a block if it can't be found (e.g. through the recover OPS production)
 */
public class basic_fetch implements AsyncCommand
{
  Logger log = Logger.getLogger(basic_fetch.class);

  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    FileMetaData meta = (FileMetaData) args[0];

    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    JSONArray blockHashes = meta.BlockHashes;

    for (int i = 0; i < blockHashes.length(); i++)
    {
      if (!hashProviders.containsKey(blockHashes.getString(i)))
      {
        context.make("msg", "body", String.format("could not find block %s in existing storage!", blockHashes.getString(i)));
        return;
      }
    }

    for (int i = 0; i < blockHashes.length(); i++)
    {
      String hash = blockHashes.getString(i);

      List<Adapter> blockProviders = hashProviders.get(hash);

      Collections.sort(blockProviders, new Comparator<Adapter>()
      {
        @Override
        public int compare(Adapter o1, Adapter o2)
        {
          return o1.Tier.compareTo(o2.Tier);
        }
      });

      boolean success = pullSubBlock(meta.Meta.getString("path"), blockProviders, hash);
      if (success)
      {
        context.make(new MemoryElement("msg", "body", String.format("%s pulled block %s", meta.Meta.getString("filename"), hash)));
      }
      else
      {
        context.make(new MemoryElement("msg", "body", String.format("%s failed to pull block %s", meta.Meta.getString("filename"), hash)));

        // attempt to rever the block and write it in the correct target file region
        // TODO: this is currently a NOP
        context.make(new MemoryElement("recover_block", "hash", hash, "meta", meta));
      }
    }
  }

  private boolean pullSubBlock(
    String path,
    List<Adapter> blockProviders,
    String hash)
  {
    boolean success = false;

    for (Adapter adapter : blockProviders)
    {
      try
      {
        InputStream remoteData = adapter.load(hash);
        FileUtil.writeFile(remoteData, path);
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
