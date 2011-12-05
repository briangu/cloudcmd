package cloudcmd.common.engine;

import cloudcmd.common.*;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.commands.*;
import cloudcmd.common.index.IndexStorageService;
import ops.Command;
import ops.OPS;
import ops.OpsFactory;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.util.*;

public class LocalCacheCloudEngine implements CloudEngine
{
  Logger log = Logger.getLogger(LocalCacheCloudEngine.class);
  OPS _ops;
  Thread _opsThread = null;

  @Override
  public void init() throws Exception
  {
    Map<String, Command> registry = OpsFactory.getDefaultRegistry();

    registry.put("fetch", new basic_fetch());
    registry.put("add_meta", new add_meta());
    registry.put("debug", new debug());
    registry.put("process", new process_raw());
    registry.put("index_default", new index_default());
    registry.put("sleep", new sleep());
    registry.put("pull_block", new pull_block());
    registry.put("push_block", new push_block());
    registry.put("push_tags", new push_tags());

    JSONObject indexOps;

    try
    {
      indexOps = ResourceUtil.loadOps("index.ops");
    }
    catch (JSONException e)
    {
      log.error("index.ops is not a valid JSON object.");
      throw e;
    }

    _ops = OpsFactory.create(registry, indexOps);
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
        log.error(e);
      }
    }

    _ops.shutdown();
  }

  @Override
  public void add(File file, Set<String> tags)
  {
    _ops.make("rawFile", "name", file.getName(), "file", file, "tags", tags);
  }

  @Override
  public void push(int maxTier)
      throws Exception
  {
    BlockCacheService.instance().loadCache(maxTier);

    JSONArray allEntries = IndexStorageService.instance().find(new JSONObject());

    Adapter localCache = BlockCacheService.instance().getBlockCache();

    final Set<String> localDescription = localCache.describe();

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

          if (!hash.startsWith(".meta"))
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
            Set<String> tags = MetaUtil.createTagSet(entry.getString("tags"));

            // TODO: accepts should take the file size as well

            if (!adapter.accepts(tags)) continue;

            _ops.make("push_tags", "adapter", adapter, "hash", hash, "tags", new JSONArray(tags));

            if (!adapterDescription.contains(hash))
            {
              _ops.make("push_block", "dest", adapter, "src", localCache, "hash", hash);
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
              _ops.make("push_block", "dest", adapter, "src", localCache, "hash", blockHash);
            }
          }
          catch (Exception e)
          {
            log.error("adapter = " + adapter.Config.getString("name"), e);
          }
        }
      }
      catch (Exception e)
      {
        log.error(e);
      }
    }
  }

  @Override
  public void pull(int maxTier, boolean retrieveBlocks)
      throws Exception
  {
    BlockCacheService.instance().loadCache(maxTier);

    Adapter localCache = BlockCacheService.instance().getBlockCache();

    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    for (String hash : hashProviders.keySet())
    {
      if (!hash.startsWith(".meta")) continue;

      if (!localCache.contains(hash))
      {
        _ops.make("pull_block", "hash", hash, "retrieveSubBlocks", retrieveBlocks);
        continue;
      }

      if (!retrieveBlocks) continue;

      try
      {
        JSONObject meta = JsonUtil.loadJson(localCache.load(hash));
        JSONArray blocks = meta.getJSONArray("blocks");

        for (int i = 0; i < blocks.length(); i++)
        {
          String blockHash = blocks.getString(i);
          if (localCache.contains(blockHash)) continue;
          _ops.make("pull_block", "hash", blockHash);
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
      throws Exception
  {
    Adapter localCache = BlockCacheService.instance().getBlockCache();

    final Set<String> localDescription = localCache.describe();

    List<FileMetaData> fmds = new ArrayList<FileMetaData>();
      
    for (String hash : localDescription)
    {
      if (!hash.startsWith(".meta")) continue;

      try
      {
        FileMetaData fmd = new FileMetaData();

        fmd.Meta = JsonUtil.loadJson(localCache.load(hash));
        fmd.MetaHash = hash;
        fmd.BlockHashes = fmd.Meta.getJSONArray("blocks");
        fmd.Tags = localCache.loadTags(hash);

        log.info(String.format("reindexing: %s %s", hash, fmd.Meta.getString("filename")));

        fmds.add(fmd);
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }

    IndexStorageService.instance().addAll(fmds);
  }

  @Override
  public void fetch(int maxTier, JSONArray selections) throws Exception
  {
    BlockCacheService.instance().loadCache(maxTier);

    for (int i = 0; i < selections.length(); i++)
    {
      try
      {
        _ops.make("fetch", "meta", MetaUtil.createMeta(selections.getJSONObject(i)));
      }
      catch (JSONException e)
      {
        log.error("index = " + i, e);
      }
    }
  }

  @Override
  public void push(int i, JSONArray selections)
  {
  }

  @Override
  public void pull(int i, boolean retrieveBlocks, JSONArray selections)
  {
  }
}
