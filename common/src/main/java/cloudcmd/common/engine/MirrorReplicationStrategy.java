package cloudcmd.common.engine;

import cloudcmd.common.FileUtil;
import cloudcmd.common.adapters.Adapter;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.*;

public class MirrorReplicationStrategy implements ReplicationStrategy {
  Logger log = Logger.getLogger(MirrorReplicationStrategy.class);

  private BlockCache _blockCache;

  public MirrorReplicationStrategy(BlockCache blockCache) {
    _blockCache = blockCache;
  }

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
  public void push(CloudEngineListener listener, Set<Adapter> adapters, String hash) throws Exception {

    Map<String, List<Adapter>> hashProviders = _blockCache.getHashProviders();

    if (!hashProviders.containsKey(hash)) {
      System.err.println();
      System.err.println(String.format("push_block: could not find block %s in existing storage!", hash));
      System.err.println();
      return;
    }

    List<Adapter> blockProviders = new ArrayList<Adapter>(hashProviders.get(hash));
    Collections.shuffle(blockProviders);
    Collections.sort(blockProviders, new Comparator<Adapter>() {
      @Override
      public int compare(Adapter o1, Adapter o2) {
        return o1.Tier.compareTo(o2.Tier);
      }
    });

    int pushedCount = 0;

    for (Adapter adapter : adapters) {
      if (adapter.describe().contains(hash)) {
        pushedCount++;
      } else {
        for (Adapter src : blockProviders) {
          if (!src.IsOnLine()) continue;

          InputStream is = null;

          try {
            is = src.load(hash);
            adapter.store(is, hash);
            pushedCount++;
            break;
          } catch (Exception e) {
            listener.onMessage(String.format("failed to push block %s from %s to adapter %s", hash, src.URI.toString(), adapter.URI.toString()));
            log.error(hash, e);
          } finally {
            FileUtil.SafeClose(is);
          }
        }
      }
    }

    if (pushedCount != adapters.size()) {
      listener.onMessage("failed to push block: " + hash);
    }
  }

  @Override
  public InputStream load(String hash) throws Exception {
    Map<String, List<Adapter>> hashProviders = _blockCache.getHashProviders();

    List<Adapter> blockProviders = hashProviders.get(hash);
    if (blockProviders == null) {
      return null;
    }
    blockProviders = new ArrayList<Adapter>(blockProviders);
    Collections.shuffle(blockProviders);
    Collections.sort(blockProviders, new Comparator<Adapter>() {
      @Override
      public int compare(Adapter o1, Adapter o2) {
        return o1.Tier.compareTo(o2.Tier);
      }
    });

    InputStream is = null;

    for (Adapter adapter : blockProviders) {
      if (!adapter.IsOnLine()) {
        log.info(String.format("adapter %s is offline, skipping", adapter.URI));
        continue;
      }

      is = adapter.load(hash);
      break;
    }

    return is;
  }
}
