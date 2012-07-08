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
    for (Adapter adapter : adapters) {
      if (!adapter.describe().contains(hash)) {
        wm.make("push_block", "dest", adapter, "hash", hash);
      }
    }
  }
}
