package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.MetaUtil;
import cloudcmd.common.engine.LocalCacheService;
import cloudcmd.common.index.IndexStorageService;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import ops.Command;
import ops.CommandContext;
import ops.MemoryElement;

public class index_default implements Command
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

    final String taskId = UUID.randomUUID().toString();
    final String fileName = file.getName();

    context.make(new MemoryElement("async_task", "phase", "start", "id", taskId, "name", fileName));

    context.submit(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          FileMetaData meta = MetaUtil.createMeta(file, tags);

          for (int i = 0; i < meta.BlockHashes.length(); i++)
          {
            LocalCacheService.instance().store(new FileInputStream(file), meta.BlockHashes.getString(i));
          }

          LocalCacheService.instance().store(new ByteArrayInputStream(meta.Meta.toString().getBytes()), meta.MetaHash);

          IndexStorageService.instance().add(meta);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
        finally
        {
          context.make(new MemoryElement("async_task", "phase", "stop", "id", taskId, "name", fileName));
        }
      }
    });
  }
}
