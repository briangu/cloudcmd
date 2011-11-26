package cloudcmd.common.engine;

import ops.MemoryElement;

import java.io.File;

public interface CloudEngine
{
  void init(String configRoot) throws Exception;
  void shutdown();

  void add(File file, String[] tags);
}
