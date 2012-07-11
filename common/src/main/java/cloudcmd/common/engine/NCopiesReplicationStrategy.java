package cloudcmd.common.engine;

import cloudcmd.common.FileMetaData;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.StringUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.config.ConfigStorageService;
import ops.WorkingMemory;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

public class NCopiesReplicationStrategy implements ReplicationStrategy {
  Logger log = Logger.getLogger(NCopiesReplicationStrategy.class);

  int _n;

  public NCopiesReplicationStrategy(int n)
  {
    _n = n;
  }

  @Override
  public boolean isReplicated(Set<Adapter> adapters, String hash) throws Exception {
    int count = 0;
    for (Adapter adapter : adapters) {
      if (!adapter.describe().contains(hash)) {
        count++;
      }
    }
    return count < _n;
  }

  @Override
  public void push(WorkingMemory wm, Set<Adapter> adapters, String hash) throws Exception {
    for (Adapter adapter : adapters) {
      if (!adapter.describe().contains(hash)) {
        wm.make("push_block", "dest", adapter, "hash", hash);
      }
    }
  }

  @Override
  public void pull(WorkingMemory wm, Set<Adapter> adapters, String hash) throws Exception {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void fetch(WorkingMemory wm, FileMetaData meta) throws Exception {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
