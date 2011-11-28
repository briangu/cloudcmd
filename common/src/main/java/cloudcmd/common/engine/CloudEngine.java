package cloudcmd.common.engine;

import java.io.File;
import java.util.Set;

public interface CloudEngine
{
  void init() throws Exception;

  void run() throws Exception;

  void shutdown();

  void add(File file, Set<String> tags);

  void push(int maxTier);

  void pull(int maxTier, boolean retrieveBlocks);

  void reindex();
}
