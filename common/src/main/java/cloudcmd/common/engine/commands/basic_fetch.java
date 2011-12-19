package cloudcmd.common.engine.commands;


import cloudcmd.common.CryptoUtil;
import cloudcmd.common.FileMetaData;
import cloudcmd.common.FileUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.BlockCacheService;
import ops.AsyncCommand;
import ops.CommandContext;
import ops.MemoryElement;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import java.io.File;
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

      boolean success = pullSubBlock(context, meta.Meta.getString("path"), blockProviders, hash);
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
    CommandContext context,
    String path,
    List<Adapter> blockProviders,
    String hash)
  {
    boolean success = false;

    for (Adapter adapter : blockProviders)
    {
      InputStream remoteData = null;
      try
      {
        // TODO: only read the file size bytes back (if the file is one block)
        // TODO: support writing to an offset of the existing file to allow for sub-blocks
        remoteData = adapter.load(hash);
        File destFile = new File(path);
        destFile.getParentFile().mkdirs();
        String remoteDataHash = CryptoUtil.digestToString(CryptoUtil.writeAndComputeHash(remoteData, destFile));
        if (remoteDataHash.equals(hash))
        {
          success = true;
          break;
        }
        else
        {
          destFile.delete();
        }
      }
      catch (Exception e)
      {
        context.make(new MemoryElement("msg", "body", String.format("failed to pull block %s", hash)));

        // TODO: We should delete/recover the block from the adapter
//        context.make(new MemoryElement("recover_block", "hash", hash));
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
