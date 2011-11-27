package cloudcmd.common.engine;

import cloudcmd.common.*;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.adapters.FileAdapter;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.index.IndexStorageService;
import ops.Command;
import ops.MemoryElement;
import ops.OPS;
import ops.OpsFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LocalCacheCloudEngine implements CloudEngine
{
  ExecutorService _threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  FileAdapter _localCache;

  OPS _ops;

  @Override
  public void init() throws Exception
  {
    Map<String, Command> registry = OpsFactory.getDefaultRegistry();

    registry.put("process", new ops.Command() {
      @Override
      public void exec(ops.CommandContext context, Object[] args) throws Exception {
        File file = (File)args[0];
        String fileName = file.getName();
        int extIndex = fileName.lastIndexOf(".");
        String ext = extIndex > 0 ? fileName.substring(extIndex+1) : null;
        String type = ext != null ? FileTypeUtil.instance().getTypeFromExtension(ext) : "default";
        context.make(new MemoryElement("index", "name", fileName, "type", type, "ext", ext, "file", file));
      }
    });

    registry.put("index_default", new ops.Command() {
      @Override
      public void exec(ops.CommandContext context, Object[] args) throws Exception {
        File file = (File)args[0];

        Set<String> tags = new HashSet<String>();
        tags.add((String)args[1]);
        tags.addAll(Arrays.asList((String[]) args[2]));

        _add(file, tags);
      }
    });

    _ops = OpsFactory.create(registry, ResourceUtil.loadOps("index.ops"));
    _ops.run();

    JSONObject obj = new JSONObject();
    obj.put("rootPath", ConfigStorageService.instance().getConfigRoot() + File.separator + "cache");

    _localCache = new FileAdapter();
    _localCache.init(0, FileAdapter.class.getName(), new HashSet<String>(), obj);
  }

  @Override
  public void shutdown()
  {
    _threadPool.shutdown();
  }

  private void _add(final File file, final Set<String> tags)
  {
    _threadPool.submit(new Runnable()
    {
      @Override
      public void run()
      {
        FileMetaData meta = MetaUtil.createMeta(file, tags);

        try
        {
          for (int i = 0; i < meta.BlockHashes.length(); i++)
          {
            _localCache.store(new FileInputStream(file), meta.BlockHashes.getString(i));
          }

          _localCache.store(new ByteArrayInputStream(meta.Meta.toString().getBytes()), meta.MetaHash);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }

        IndexStorageService.instance().add(meta);
      }
    });
  }

  @Override
  public void add(File file, String[] tags)
  {
    _ops.make(new MemoryElement("rawFile", "name", file.getName(), "file", file));
  }

  @Override
  public void push(int maxTier)
  {
    final Set<String> localDescription = _localCache.describe();

    for (final Adapter adapter : ConfigStorageService.instance().getAdapters())
    {
      if (adapter.Tier > maxTier) continue;

      try
      {
        Set<String> adapterDescription = adapter.describe();

        for (final String hash : localDescription)
        {
          if (!hash.endsWith(".meta")) continue;

          try
          {
            JSONObject meta = JsonUtil.loadJson(_localCache.load(hash));

            Set<String> tags = IndexStorageService.instance().getTags(hash);

            if (!adapter.acceptsTags(tags)) continue;

            queueStoreTags(adapter, new JSONArray(tags), hash);

            if (!adapterDescription.contains(hash))
            {
              queueStore(adapter, _localCache.load(hash), hash);
            }

            JSONArray blocks = meta.getJSONArray("blocks");

            for (int i = 0; i < blocks.length(); i++)
            {
              String blockHash = blocks.getString(i);
              if (adapterDescription.contains(blockHash)) continue;
              queueStore(adapter, _localCache.load(blockHash), blockHash);
            }
          }
          catch (Exception e)
          {
            e.printStackTrace();
          }
        }
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void pull(int maxTier, boolean retrieveBlocks)
  {
    final Set<String> localDescription = _localCache.describe();

    for (final Adapter adapter : ConfigStorageService.instance().getAdapters())
    {
      if (adapter.Tier > maxTier) continue;

      try
      {
        Set<String> adapterDescription = adapter.describe();

        for (final String hash : adapterDescription)
        {
          if (!hash.endsWith(".meta")) continue;

          try
          {
            FileMetaData fmd = new FileMetaData();

            fmd.Meta = JsonUtil.loadJson(adapter.load(hash));
            fmd.MetaHash = hash;
            fmd.BlockHashes = fmd.Meta.getJSONArray("blocks");
            fmd.Tags = adapter.loadTags(hash);

            if (!localDescription.contains(hash))
            {
              queueStore(_localCache, adapter.load(hash), hash);
            }

            IndexStorageService.instance().add(fmd);

            if (retrieveBlocks)
            {
              JSONArray blocks = fmd.BlockHashes;

              for (int i = 0; i < blocks.length(); i++)
              {
                String blockHash = blocks.getString(i);
                if (localDescription.contains(blockHash)) continue;
                queueStore(_localCache, adapter.load(blockHash), blockHash);
              }
            }
          }
          catch (Exception e)
          {
            e.printStackTrace();
          }
        }
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
  }

  private void queueStore(final Adapter adapter, final InputStream is, final String hash)
  {
    _threadPool.submit(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          adapter.store(is, hash);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });
  }

  private void queueStoreTags(final Adapter adapter, final JSONArray tags, final String hash)
  {
    _threadPool.submit(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          adapter.storeTags(new ByteArrayInputStream(tags.toString().getBytes()), hash);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });
  }
}
