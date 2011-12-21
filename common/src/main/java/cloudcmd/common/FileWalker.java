package cloudcmd.common;

import java.io.File;
import java.util.Stack;

public class FileWalker
{

  public static void enumerateFolders(String startFolder, FileHandler handler)
  {
    Stack<File> stack = new Stack<File>();

    File rootDir = new File(startFolder);
    if (!rootDir.exists())
    {
      throw new IllegalArgumentException("file does not exist: " + startFolder);
    }

    stack.push(rootDir);

    while (!stack.isEmpty())
    {
      File curFile = stack.pop();

      File[] subFiles = curFile.listFiles();

      if (subFiles == null || subFiles.length == 0) continue;

      for (File file : subFiles)
      {
        if (file.isDirectory())
        {
          if (handler.skipDir(file)) continue;
          stack.push(file);
          continue;
        }

        handler.process(file);
      }
    }
  }
}
