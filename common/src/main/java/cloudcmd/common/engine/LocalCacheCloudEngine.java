package cloudcmd.common.engine;

import cloudcmd.common.FileMetaData;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.MetaUtil;
import cloudcmd.common.ResourceUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.commands.*;
import cloudcmd.common.index.IndexStorageService;
import ops.Command;
import ops.OPS;
import ops.OpsFactory;
import ops.WorkingMemory;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.*;

public class LocalCacheCloudEngine implements CloudEngine
{
  Logger log = Logger.getLogger(LocalCacheCloudEngine.class);
  OPS _ops;
  WorkingMemory _wm;
  Thread _opsThread = null;
  ReplicationStrategy _replicationStrategy;

  private Map<String, Command> buildRegistry()
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
    registry.put("push_block_async", new push_block_async());
    registry.put("verify_block", new verify_block());
    registry.put("remove_block", new remove_block());

    return registry;
  }

  @Override
  public void init(ReplicationStrategy replicationStrategy) throws Exception
  {
    init(replicationStrategy, "index.ops");
  }

  @Override
  public void init(ReplicationStrategy replicationStrategy, String opsName) throws Exception
  {
    _replicationStrategy = replicationStrategy;

    Map<String, Command> registry = buildRegistry();

    JSONObject indexOps;

    try
    {
      indexOps = ResourceUtil.loadOps(opsName);
    }
    catch (JSONException e)
    {
      log.error("index.ops is not a valid JSON object.");
      throw e;
    }

    _ops = OpsFactory.create(registry, indexOps);
    _wm = _ops.getWorkingMemory();
  }

  @Override
  public void prepareFlushToAdapter(Adapter adapter) throws Exception
  {
    BlockCacheService.instance().loadCache(adapter.Tier);
    _wm.make("flush_to_adapter", "src", BlockCacheService.instance().getBlockCache(), "dest", adapter);
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
    _wm.make("rawFile", "name", file.getName(), "file", file, "tags", tags);
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
    _replicationStrategy.push(_wm, maxTier, selections);
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
        _wm.make("pull_block", "hash", hash, "retrieveSubBlocks", retrieveBlocks);
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
          _wm.make("pull_block", "hash", blockHash);
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
        System.out.println(String.format("reindexing: %s %s", hash, fmd.getFilename()));
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
  public void verify(int maxTier, boolean deleteOnInvalid)
    throws Exception
  {
    BlockCacheService.instance().loadCache(maxTier);

    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    Boolean boxed = new Boolean(deleteOnInvalid);
    
    for (String hash : hashProviders.keySet())
    {
      _wm.make("verify_block", "hash", hash, "deleteOnInvalid", boxed);
    }
  }

  @Override
  public void verify(int maxTier, JSONArray selections, boolean deleteOnInvalid)
    throws Exception
  {
    BlockCacheService.instance().loadCache(maxTier);

    Boolean boxed = new Boolean(deleteOnInvalid);

    for (int i = 0; i < selections.length(); i++)
    {
      String hash = selections.getJSONObject(i).getString("hash");
      _wm.make("verify_block", "hash", hash, "deleteOnInvalid", boxed);
    }
  }

  @Override
  public void remove(JSONArray selections) throws Exception
  {
    BlockCacheService.instance().loadCache(Integer.MAX_VALUE);

    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    Adapter localCache = BlockCacheService.instance().getBlockCache();
    
    for (int i = 0; i < selections.length(); i++)
    {
      String hash = selections.getJSONObject(i).getString("hash");

      List<Adapter> adapters = hashProviders.get(hash);
      if (adapters == null)
      {
        continue;
      }

      JSONObject meta = JsonUtil.loadJson(localCache.load(hash));
      JSONArray blocks = meta.getJSONArray("blocks");

      for (int j = 0; j < blocks.length(); j++)
      {
        removeBlock(hashProviders, blocks.getString(j));
      }

      removeBlock(hashProviders, hash);

      JSONObject indexMeta = new JSONObject();
      indexMeta.put("hash", hash);
      indexMeta.put("data", meta);

      // TODO: we should only do this if we are sure the rest happened correctly (although at worst we could reindex)
      IndexStorageService.instance().remove(MetaUtil.loadMeta(indexMeta));
    }
  }

  private void removeBlock(Map<String, List<Adapter>> hashProviders, String hash)
  {
    List<Adapter> adapters = hashProviders.get(hash);
    if (adapters == null) return;

    for (Adapter adapter : adapters)
    {
      _wm.make("remove_block", "hash", hash, "adapter", adapter);
    }
  }
  
  @Override
  public void fetch(int maxTier, JSONArray selections) throws Exception
  {
    BlockCacheService.instance().loadCache(maxTier);

    for (int i = 0; i < selections.length(); i++)
    {
      try
      {
        _wm.make("fetch", "meta", MetaUtil.loadMeta(selections.getJSONObject(i)));
      }
      catch (JSONException e)
      {
        log.error("index = " + i, e);
      }
    }
  }
}
