package cloudcmd.common.engine;

import cloudcmd.common.*;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.config.ConfigStorageService;
import ops.CommandContext;
import ops.MemoryElement;
import ops.WorkingMemory;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.util.*;

public class MirrorReplicationStrategy implements ReplicationStrategy {
  Logger log = Logger.getLogger(NCopiesReplicationStrategy.class);

  @Override
  public boolean isReplicated(Set<Adapter> adapters, String hash) throws Exception {
    for (Adapter adapter : adapters) {
      if (!adapter.describe().contains(hash)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void push(WorkingMemory wm, Set<Adapter> adapters, String hash) throws Exception {

    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    if (!hashProviders.containsKey(hash))
    {
      System.err.println();
      System.err.println(String.format("push_block: could not find block %s in existing storage!", hash));
      System.err.println();
      return;
    }

    List<Adapter> blockProviders = new ArrayList<Adapter>(hashProviders.get(hash));
    Collections.shuffle(blockProviders);
    Collections.sort(blockProviders, new Comparator<Adapter>()
    {
      @Override
      public int compare(Adapter o1, Adapter o2)
      {
        return o1.Tier.compareTo(o2.Tier);
      }
    });

    boolean pushed = false;

    for (Adapter adapter : adapters) {
      if (!adapter.describe().contains(hash)) {
        for (Adapter src : blockProviders)
        {
          if (!src.IsOnLine()) continue;

          InputStream is = null;

          try
          {
            is = src.load(hash);
            adapter.store(is, hash);
            pushed = true;
            break;
          }
          catch (Exception e)
          {
            wm.make("error_push_block", "hash", hash);
            log.error(hash, e);
          }
          finally
          {
            FileUtil.SafeClose(is);
          }
        }
      }
    }

    if (!pushed) {
      wm.make("error_push_block", "hash", hash);
    }
  }

  @Override
  public void pull(WorkingMemory wm, Set<Adapter> adapters, String hash) throws Exception {
    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    if (!hashProviders.containsKey(hash))
    {
      System.err.println();
      System.err.println(String.format("unexpected: could not find block %s in existing storage!", hash));
      System.err.println();
      return;
    }

    List<Adapter> blockProviders = new ArrayList<Adapter>(adapters);
    Collections.shuffle(blockProviders);
    Collections.sort(blockProviders, new Comparator<Adapter>()
    {
      @Override
      public int compare(Adapter o1, Adapter o2)
      {
        return o1.Tier.compareTo(o2.Tier);
      }
    });

    boolean success = false;

    for (Adapter adapter : adapters)
    {
      if (!adapter.IsOnLine()) {
        log.info(String.format("adapter %s is offline, skipping", adapter.URI));
        continue;
      }

      InputStream remoteData = null;
      try
      {
        remoteData = adapter.load(hash);
        BlockCacheService.instance().getBlockCache().store(remoteData, hash);
        success = true;
        break;
      }
      catch (Exception e)
      {
        wm.make("error_pull_block", "hash", hash);
        log.error(hash, e);
      }
      finally
      {
        FileUtil.SafeClose(remoteData);
      }
    }

    if (success)
    {
      wm.make(new MemoryElement("msg", "body", String.format("successfully pulled block %s", hash)));
    }
    else
    {
      wm.make(new MemoryElement("msg", "body", String.format("failed to pull block %s", hash)));
      wm.make(new MemoryElement("recover_block", "hash", hash));
    }
  }
}
