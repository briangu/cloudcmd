package cloudcmd.common.engine;

import ops.WorkingMemory;
import org.json.JSONArray;

public interface ReplicationStrategy {
  public void push(WorkingMemory wm, int maxTier, JSONArray selections) throws Exception;
}
