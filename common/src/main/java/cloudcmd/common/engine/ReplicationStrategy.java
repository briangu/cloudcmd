package cloudcmd.common.engine;

import cloudcmd.common.adapters.Adapter;

import java.io.InputStream;
import java.util.Set;

public interface ReplicationStrategy {
  public boolean isReplicated(Set<Adapter> adapters, String hash) throws Exception;
  public void push(CloudEngineListener listener, Set<Adapter> adapters, String hash) throws Exception;
  public InputStream load(String hash) throws Exception;
}
