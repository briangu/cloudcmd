package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.MetaUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.BlockCacheService;
import cloudcmd.common.index.IndexStorageService;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
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

    final Set<String> tags = new HashSet<String>();
    if (type != null) tags.add(type);
    tags.addAll((Set<String>) args[2]);

    FileMetaData meta = MetaUtil.createMeta(file, tags);

    // TODO: we will need to break this commadn apart to allow for index_jpg, etc.

    Adapter localCache = BlockCacheService.instance().getBlockCache();

    for (int i = 0; i < meta.BlockHashes.length(); i++)
    {
      localCache.store(new FileInputStream(file), meta.BlockHashes.getString(i));
    }

    localCache.store(new ByteArrayInputStream(meta.Meta.toString().getBytes()), meta.MetaHash);

    IndexStorageService.instance().add(meta);
  }
}
