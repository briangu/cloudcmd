package cloudcmd.common;

import java.io.File;

public interface FileHandler
{
  boolean skipDir(File file);
  void process(File file);
}
