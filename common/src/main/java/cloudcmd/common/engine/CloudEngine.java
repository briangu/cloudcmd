package cloudcmd.common.engine;

import java.io.File;

public interface CloudEngine
{
  void init() throws Exception;
  void shutdown();

  void add(File file, String[] tags);

  void push(int maxTier);

  void pull(int maxTier, boolean retrieveBlocks);
}
