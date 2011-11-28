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

  Thread _opsThread = null;

  @Override
  public void init() throws Exception
  {
    Map<String, Command> registry = OpsFactory.getDefaultRegistry();

    registry.put("process", new ops.Command()
    {
      @Override
      public void exec(ops.CommandContext context, Object[] args) throws Exception
      {
        File file = (File) args[0];
        String fileName = file.getName();
        int extIndex = fileName.lastIndexOf(".");
        String ext = extIndex > 0 ? fileName.substring(extIndex + 1) : null;
        String type = ext != null ? FileTypeUtil.instance().getTypeFromExtension(ext) : null;
        context.make(new MemoryElement("index", "name", fileName, "type", type, "ext", ext, "file", file, "tags", args[1]));
      }
    });

    registry.put("index_default", new ops.Command()
    {
      @Override
      public void exec(ops.CommandContext context, Object[] args) throws Exception
      {
        File file = (File) args[0];
        String type = (String) args[1];

        Set<String> tags = new HashSet<String>();
        if (type != null) tags.add(type);
        tags.addAll((Set<String>) args[2]);

        _add(file, tags);
      }
    });

    _ops = OpsFactory.create(registry, ResourceUtil.loadOps("index.ops"));

/*
    _ops.waitForWork(true);

    _opsThread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        _ops.run();
      }
    });
    _opsThread.start();
*/

    JSONObject obj = new JSONObject();
    obj.put("rootPath", ConfigStorageService.instance().getConfigRoot() + File.separator + "cache");

    _localCache = new FileAdapter();
    _localCache.init(0, FileAdapter.class.getName(), new HashSet<String>(), obj);
  }

  @Override
  public void run()
  {
    _ops.run();
  }

  @Override
  public void shutdown()
  {
    if (_opsThread != null)
    {
      _opsThread.interrupt();
      try
      {
        _opsThread.join(1000);
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
      }
    }

    _threadPool.shutdown();
  }

  private void _add(final File file, final Set<String> tags)
  {
    Runnable runnable = new Runnable()
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
    };

    runnable.run();

//    _threadPool.submit(runnable);
  }

  @Override
  public void add(File file, Set<String> tags)
  {
    _ops.make(new MemoryElement("rawFile", "name", file.getName(), "file", file, "tags", tags));
  }

  @Override
  public void push(int maxTier)
  {
    JSONArray allEntries = IndexStorageService.instance().find(new JSONObject());

    final Set<String> localDescription = _localCache.describe();

    for (final Adapter adapter : ConfigStorageService.instance().getAdapters())
    {
      if (adapter.Tier > maxTier) continue;

      try
      {
        Set<String> adapterDescription = adapter.describe();

        for (int i = 0; i < allEntries.length(); i++)
        {
          JSONObject entry = allEntries.getJSONObject(i);

          String hash = entry.getString("hash");

          if (!hash.endsWith(".meta"))
          {
            // TODO: message (this shouldn't happen)
            continue;
          }

          if (!localDescription.contains(hash))
          {
            // TODO: message (the index should always by in sync with the local cache)
            continue;
          }

          try
          {
            Set<String> tags = MetaUtil.createRowTagSet(entry.getString("tags"));

            if (!adapter.acceptsTags(tags)) continue;

            queueStoreTags(adapter, new JSONArray(tags), hash);

            if (!adapterDescription.contains(hash))
            {
              // TODO: can we get this blob from the index entry?
              queueStore(adapter, _localCache.load(hash), hash);
            }

            JSONArray blocks = entry.getJSONArray("blocks");

            for (int blockIdx = 0; blockIdx < blocks.length(); blockIdx++)
            {
              String blockHash = blocks.getString(blockIdx);
              if (adapterDescription.contains(blockHash)) continue;
              if (!localDescription.contains(blockHash))
              {
                // TODO: message (it's possible that we legitimately don't have the block if we only have the meta data)
                continue;
              }
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

  @Override
  public void reindex()
  {
    final Set<String> localDescription = _localCache.describe();

    for (String hash : localDescription)
    {
      if (!hash.endsWith(".meta"))
      {
        // TODO: message (this shouldn't happen)
        continue;
      }

      try
      {
        FileMetaData fmd = new FileMetaData();

        fmd.Meta = JsonUtil.loadJson(_localCache.load(hash));
        fmd.MetaHash = hash;
        fmd.BlockHashes = fmd.Meta.getJSONArray("blocks");
        fmd.Tags = _localCache.loadTags(hash);

        _ops.make(new MemoryElement("msg", "body", String.format("reindexing: %s %s", hash, fmd.Meta.getString("filename"))));

        IndexStorageService.instance().add(fmd);
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
