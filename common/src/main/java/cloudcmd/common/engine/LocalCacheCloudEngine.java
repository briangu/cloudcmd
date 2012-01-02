package cloudcmd.common.engine;

import cloudcmd.common.*;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.config.ConfigStorageService;
import cloudcmd.common.engine.commands.*;
import cloudcmd.common.index.IndexStorageService;
import java.io.ByteArrayInputStream;
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
    if (_ops == null) return;

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

    if (_ops != null)
    {
      _ops.shutdown();
    }
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
    JSONArray selections = IndexStorageService.instance().find(new JSONObject());
    push(maxTier, selections);
  }

  // **
  // TODO: pluggable replication strategies
  //       The current strategy is mirroring with tolerance for offline and full
  // **
  @Override
  public void push(int maxTier, JSONArray selections)
      throws Exception
  {
    BlockCacheService.instance().loadCache(maxTier);

    Adapter localCache = BlockCacheService.instance().getBlockCache();

    final Set<String> localDescription = localCache.describe();

    for (final Adapter adapter : ConfigStorageService.instance().getAdapters())
    {
      if (adapter.Tier > maxTier) continue;
      if (!adapter.IsOnLine())
      {
        log.warn(String.format("skipping adapter because it's not online: %s", adapter.URI.toASCIIString()));
        continue;
      }
      if (adapter.IsFull())
      {
        log.warn(String.format("skipping adapter because it's full: %s", adapter.URI.toASCIIString()));
        continue;
      }

      try
      {
        Set<String> adapterDescription = adapter.describe();

        for (int i = 0; i < selections.length(); i++)
        {
          String hash = selections.getJSONObject(i).getString("hash");

          if (!hash.endsWith(".meta"))
          {
            log.error("unexpected hash type: " + hash);
            continue;
          }

          if (!localDescription.contains(hash))
          {
            // the index should always by in sync with the local cache
            log.error("hash not found in local cache: " + hash);
            continue;
          }

          JSONObject entry = selections.getJSONObject(i).getJSONObject("data");

          try
          {
            Set<String> tags = JsonUtil.createSet(entry.getJSONArray("tags"));

            // TODO: accepts should take the file size as well

            if (!adapter.accepts(tags))
            {
              if (log.isDebugEnabled())
              {
                log.debug(String.format("skipping adapter %s because it doesn't accept tags (%s) for hash %s",
                                        adapter.URI.toASCIIString(),
                                        StringUtil.join(tags, ","),
                                        hash));
              }
              continue;
            }

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
            log.error("adapter = " + adapter.URI, e);
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
    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();
    pull(retrieveBlocks, hashProviders.keySet());
  }

  @Override
  public void pull(int maxTier, boolean retrieveBlocks, JSONArray selections)
      throws Exception
  {
    BlockCacheService.instance().loadCache(maxTier);

    Set<String> hashes = new HashSet<String>(selections.length());

    for (int i = 0; i < selections.length(); i++)
    {
      hashes.add(selections.getJSONObject(i).getString("hash"));
    }

    pull(retrieveBlocks, hashes);
  }

  private void pull(boolean retrieveBlocks, Set<String> hashes)
      throws Exception
  {
    Adapter localCache = BlockCacheService.instance().getBlockCache();

    for (String hash : hashes)
    {
      if (!hash.endsWith(".meta")) continue;

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
    IndexStorageService.instance().purge();

    Adapter localCache = BlockCacheService.instance().getBlockCache();

    final Set<String> localDescription = localCache.describe();

    List<FileMetaData> fmds = new ArrayList<FileMetaData>();
      
    for (String hash : localDescription)
    {
      if (!hash.endsWith(".meta")) continue;

      try
      {
        FileMetaData fmd = MetaUtil.loadMeta(hash, JsonUtil.loadJson(localCache.load(hash)));
        log.info(String.format("reindexing: %s %s", hash, fmd.getFilename()));
        fmds.add(fmd);
      }
      catch (Exception e)
      {
        log.error(hash, e);
      }
    }

    IndexStorageService.instance().addAll(fmds);
    IndexStorageService.instance().pruneHistory(MetaUtil.toJsonArray(fmds));
  }

  @Override
  public JSONArray addTags(JSONArray selections, Set<String> tags)
      throws Exception
  {
    Adapter localCache = BlockCacheService.instance().getBlockCache();
    final Set<String> localDescription = localCache.describe();

    List<FileMetaData> fmds = new ArrayList<FileMetaData>(selections.length());

    for (int i = 0; i < selections.length(); i++)
    {
      String hash = selections.getJSONObject(i).getString("hash");
      JSONObject data = selections.getJSONObject(i).getJSONObject("data");
      FileMetaData oldMeta = FileMetaData.create(hash, data);

      Set<String> newTags = MetaUtil.applyTags(oldMeta.getTags(), tags);
      if (newTags.equals(oldMeta.getTags())) continue;
      data.put("tags", new JSONArray(newTags));

      FileMetaData derivedMeta = MetaUtil.deriveMeta(hash, data);
      if (localDescription.contains(derivedMeta.getHash())) continue;
      fmds.add(derivedMeta);
      localCache.store(new ByteArrayInputStream(derivedMeta.getDataAsString().getBytes("UTF-8")), derivedMeta.getHash());
    }

    JSONArray newSelections = MetaUtil.toJsonArray(fmds);

    IndexStorageService.instance().addAll(fmds);
    IndexStorageService.instance().pruneHistory(newSelections);

    return newSelections;
  }

  @Override
  public void fetch(int maxTier, JSONArray selections) throws Exception
  {
    BlockCacheService.instance().loadCache(maxTier);

    for (int i = 0; i < selections.length(); i++)
    {
      try
      {
        _ops.make("fetch", "meta", MetaUtil.loadMeta(selections.getJSONObject(i)));
      }
      catch (JSONException e)
      {
        log.error("index = " + i, e);
      }
    }
  }
}
