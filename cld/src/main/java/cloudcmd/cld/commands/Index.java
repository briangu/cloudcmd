package cloudcmd.cld.commands;


import cloudcmd.cld.FileHandler;
import cloudcmd.cld.FileWalker;
import cloudcmd.cld.OpsLoader;
import jpbetz.cli.*;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import ops.*;

import java.io.File;
import java.util.Map;

@SubCommand(name="index", description="Index files")
public class Index implements Command {
	
	@Arg(name="Path to index", optional=true)
	public String path = ".";

//	@Opt(opt="n", longOpt="repeat", description="Number of times to yell the text")
//	public Number yells = 1;

	@Override
  public void exec(CommandContext commandLine) throws CommandError, Exception {

    Map<String, ops.Command> registry = OpsFactory.getDefaultRegistry();

    registry.put("process", new ops.Command() {

      @Override
      public void exec(ops.CommandContext context, Object[] args) {
        File file = (File)args[0];
        System.out.println("processing: " + file.getAbsolutePath());
      }
    });

    final OPS ops = OpsFactory.create(registry, OpsLoader.load("index.ops"));

    FileWalker.enumerateFolders(path, new FileHandler() {
      @Override
      public void process(File file) {

        ops.make(new MemoryElement("rawFile", "name", file.getName(), "file", file));

      }
    });

    ops.run();
  }
}
