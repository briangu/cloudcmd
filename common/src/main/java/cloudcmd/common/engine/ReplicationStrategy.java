package cloudcmd.common.engine;

import cloudcmd.common.FileMetaData;
import cloudcmd.common.adapters.Adapter;
import ops.WorkingMemory;
import org.json.JSONArray;

import java.util.Set;

public interface ReplicationStrategy {
  public boolean isReplicated(Set<Adapter> adapters, String hash) throws Exception;
  public void push(WorkingMemory wm, Set<Adapter> adapters, String hash) throws Exception;
  public void pull(WorkingMemory wm, Set<Adapter> adapters, String hash) throws Exception;
}
