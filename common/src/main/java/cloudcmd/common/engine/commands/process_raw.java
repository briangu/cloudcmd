package cloudcmd.common.engine.commands;


import cloudcmd.common.FileTypeUtil;
import java.io.File;
import ops.AsyncCommand;
import ops.Command;
import ops.CommandContext;
import ops.MemoryElement;


public class process_raw implements AsyncCommand
{
  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    File file = (File) args[0];
    String fileName = file.getName();
    int extIndex = fileName.lastIndexOf(".");
    String ext = extIndex > 0 ? fileName.substring(extIndex + 1) : null;
    String type = ext != null ? FileTypeUtil.instance().getTypeFromExtension(ext) : null;
    context.make(new MemoryElement("index", "name", fileName, "type", type, "ext", ext, "file", file, "tags", args[1]));
  }
}
