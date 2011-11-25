package cloudcmd.cld.commands;


import cloudcmd.cld.FileHandler;
import cloudcmd.cld.FileTypeUtil;
import cloudcmd.cld.FileWalker;
import cloudcmd.common.ResourceLoader;
import jpbetz.cli.*;
import ops.MemoryElement;
import ops.OPS;
import ops.OpsFactory;

import java.io.File;
import java.util.Map;

@SubCommand(name="reindex", description="Rebuild the index from the archived files.")
public class Reindex implements Command {
	
	@Override
  public void exec(CommandContext commandLine) throws CommandError, Exception {
  }
}
