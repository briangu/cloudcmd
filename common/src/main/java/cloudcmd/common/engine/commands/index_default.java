package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.FileUtil;
import cloudcmd.common.MetaUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.BlockCacheService;
import cloudcmd.common.index.IndexStorageService;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import ops.AsyncCommand;
import ops.CommandContext;
import ops.MemoryElement;

public class index_default implements AsyncCommand
{
  // TODO: we will need to break this commadn apart to allow for index_jpg, etc.

  @Override
  public void exec(final CommandContext context, Object[] args)
      throws Exception
  {
    final File file = (File) args[0];
    String type = (String) args[1];

    Set<String> tags = new HashSet<String>();
    if (type != null) tags.add(type);
    tags.addAll((Set<String>) args[2]);

    String blockHash = null;
    FileInputStream fis = null;
    ByteArrayInputStream bais = null;

    try
    {
      Adapter localCache = BlockCacheService.instance().getBlockCache();

      long startTime = System.currentTimeMillis();
      try
      {
        fis = new FileInputStream(file);
        blockHash = localCache.store(fis);

        context.make("new_block", "type", "subblock", "hash", blockHash);

        FileMetaData meta = MetaUtil.createMeta(file, Arrays.asList(blockHash), tags);
        if (!localCache.contains(meta.getHash()))
        {
          bais = new ByteArrayInputStream(meta.getDataAsString().getBytes("UTF-8"));
          localCache.store(bais, meta.getHash());
          IndexStorageService.instance().add(meta);
          context.make("new_block", "type", "meta", "hash", meta.getHash());
        }
      }
      finally
      {
        String msg = String.format("took %6d ms to index %s", (System.currentTimeMillis() - startTime), file.getName());
        context.make(new MemoryElement("msg", "body", msg));
      }
    }
    finally
    {
      FileUtil.SafeClose(fis);
      FileUtil.SafeClose(bais);
    }

    if (blockHash == null)
    {
      throw new RuntimeException("failed to index file: " + file.getAbsolutePath());
    }
  }
}
