package cloudcmd.common.engine;

import cloudcmd.common.CryptoUtil;
import cloudcmd.common.FileMetaData;
import cloudcmd.common.FileUtil;
import cloudcmd.common.adapters.Adapter;
import ops.MemoryElement;
import ops.WorkingMemory;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import java.io.File;
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
            wm.make("msg", "body", String.format("failed to push block %s from %s to adapter %s", hash, src.URI.toString(), adapter.URI.toString()));
            log.error(hash, e);
          } finally {
            FileUtil.SafeClose(is);
          }
        }
      }
    }

    if (pushedCount != adapters.size()) {
      wm.make("msg", "body", "failed to push block: " + hash);
    }
  }

  @Override
  public void pull(WorkingMemory wm, Set<Adapter> adapters, String hash) throws Exception {
    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    if (!hashProviders.containsKey(hash)) {
      System.err.println();
      System.err.println(String.format("unexpected: could not find block %s in existing storage!", hash));
      System.err.println();
      return;
    }

    List<Adapter> blockProviders = new ArrayList<Adapter>(adapters);
    Collections.shuffle(blockProviders);
    Collections.sort(blockProviders, new Comparator<Adapter>() {
      @Override
      public int compare(Adapter o1, Adapter o2) {
        return o1.Tier.compareTo(o2.Tier);
      }
    });

    boolean success = false;

    for (Adapter adapter : adapters) {
      if (!adapter.IsOnLine()) {
        log.info(String.format("adapter %s is offline, skipping", adapter.URI));
        continue;
      }

      InputStream remoteData = null;
      try {
        remoteData = adapter.load(hash);
        BlockCacheService.instance().getBlockCache().store(remoteData, hash);
        success = true;
        break;
      } catch (Exception e) {
        wm.make("error_pull_block", "hash", hash);
        log.error(hash, e);
      } finally {
        FileUtil.SafeClose(remoteData);
      }
    }

    if (success) {
      wm.make(new MemoryElement("msg", "body", String.format("successfully pulled block %s", hash)));
    } else {
      wm.make(new MemoryElement("msg", "body", String.format("failed to pull block %s", hash)));
      wm.make(new MemoryElement("recover_block", "hash", hash));
    }
  }

  // TODO: most of this logic should bein the parallelcloudengine
  @Override
  public void fetch(WorkingMemory wm, FileMetaData meta)
    throws Exception {
    Map<String, List<Adapter>> hashProviders = BlockCacheService.instance().getHashProviders();

    JSONArray blockHashes = meta.getBlockHashes();

    for (int i = 0; i < blockHashes.length(); i++) {
      if (!hashProviders.containsKey(blockHashes.getString(i))) {
        wm.make("msg", "body", String.format("could not find block %s in existing storage!", blockHashes.getString(i)));
        return;
      }
    }

    for (int i = 0; i < blockHashes.length(); i++) {
      String hash = blockHashes.getString(i);

      List<Adapter> blockProviders = new ArrayList<Adapter>(hashProviders.get(hash));
      Collections.shuffle(blockProviders);
      Collections.sort(blockProviders, new Comparator<Adapter>() {
        @Override
        public int compare(Adapter o1, Adapter o2) {
          return o1.Tier.compareTo(o2.Tier);
        }
      });

      // TODO: we should be supporting offset writes into the target file for each block
      boolean success = pullSubBlock(wm, meta.getPath(), blockProviders, hash);
      if (success) {
        wm.make(new MemoryElement("msg", "body", String.format("%s pulled block %s", meta.getPath(), hash)));
      } else {
        wm.make(new MemoryElement("msg", "body", String.format("%s failed to pull block %s", meta.getFilename(), hash)));

        // attempt to rever the block and write it in the correct target file region
        // TODO: this is currently a NOP
        wm.make(new MemoryElement("recover_block", "hash", hash, "meta", meta));
      }
    }
  }

  private boolean pullSubBlock(
    WorkingMemory wm,
    String path,
    List<Adapter> blockProviders,
    String hash) {
    boolean success = false;

    for (Adapter adapter : blockProviders) {
      InputStream remoteData = null;
      try {
        // TODO: only read the file size bytes back (if the file is one block)
        // TODO: support writing to an offset of the existing file to allow for sub-blocks
        remoteData = adapter.load(hash);
        File destFile = new File(path);
        destFile.getParentFile().mkdirs();
        String remoteDataHash = CryptoUtil.digestToString(CryptoUtil.writeAndComputeHash(remoteData, destFile));
        if (remoteDataHash.equals(hash)) {
          success = true;
          break;
        } else {
          destFile.delete();
        }
      } catch (Exception e) {
        wm.make(new MemoryElement("msg", "body", String.format("failed to pull block %s", hash)));

        // TODO: We should delete/recover the block from the adapter
//        wm.make(new MemoryElement("recover_block", "hash", hash));
        log.error(hash, e);
      } finally {
        FileUtil.SafeClose(remoteData);
      }
    }

    return success;
  }
}
