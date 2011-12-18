package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
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

public class index_default implements AsyncCommand
{
  @Override
  public void exec(final CommandContext context, Object[] args)
      throws Exception
  {
    final File file = (File) args[0];
    String type = (String) args[1];

    Set<String> tags = new HashSet<String>();
    if (type != null) tags.add(type);
    tags.addAll((Set<String>) args[2]);

    Adapter localCache = BlockCacheService.instance().getBlockCache();

    String blockHash = localCache.store(new FileInputStream(file));

    FileMetaData meta = MetaUtil.createMeta(file, Arrays.asList(blockHash), tags);

    // TODO: we will need to break this commadn apart to allow for index_jpg, etc.

    localCache.store(new ByteArrayInputStream(meta.Meta.toString().getBytes()), meta.MetaHash);

    IndexStorageService.instance().add(meta);
  }
}
