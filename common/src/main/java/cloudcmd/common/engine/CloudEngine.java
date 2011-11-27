package cloudcmd.common.engine;

import java.io.File;
import java.util.List;

public interface CloudEngine
{
  void init() throws Exception;
  void shutdown();

  void add(File file, List<String> tags);

  void push(int maxTier);

  void pull(int maxTier, boolean retrieveBlocks);
}
