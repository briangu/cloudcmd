package cloudcmd.common.engine;

import cloudcmd.common.JsonUtil;
import cloudcmd.common.StringUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.config.ConfigStorageService;
import ops.WorkingMemory;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class MirrorReplicationStrategy implements ReplicationStrategy {
  Logger log = Logger.getLogger(LocalCacheCloudEngine.class);

  @Override
  public void push(WorkingMemory wm, int maxTier, JSONArray selections) throws Exception {
    BlockCacheService.instance().loadCache(maxTier);

    Adapter localCache = BlockCacheService.instance().getBlockCache();

    final Set<String> localDescription = localCache.describe();

    for (final Adapter adapter : ConfigStorageService.instance().getAdapters())
    {
      if (adapter.Tier > maxTier) continue;
      if (adapter.Tier == 0) continue;

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

        Set<String> pushSet = new HashSet<String>(selections.length() * 2);

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
              pushSet.add(hash);
            }

            JSONArray blocks = entry.getJSONArray("blocks");

            for (int blockIdx = 0; blockIdx < blocks.length(); blockIdx++)
            {
              String blockHash = blocks.getString(blockIdx);
              if (adapterDescription.contains(blockHash)) continue;
              pushSet.add(blockHash);
            }
          }
          catch (Exception e)
          {
            log.error("adapter = " + adapter.URI, e);
          }
        }

        for (String hash : pushSet)
        {
          wm.make("push_block", "dest", adapter, "hash", hash);
        }
      }
      catch (Exception e)
      {
        log.error(e);
      }
    }
  }
}
