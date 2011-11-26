package cloudcmd.cld.commands;


import jpbetz.cli.*;

@SubCommand(name="reindex", description="Rebuild the index from the archived files.")
public class Tag implements Command {

  @Arg(name="tags", optional = true, isVararg = true)
  public String[] _tags = null;

  @Opt(opt = "r", longOpt = "remove", description = "remove tags", required = false)
  public boolean _remove;

	@Override
  public void exec(CommandContext commandLine) throws CommandError, Exception {
  }
}
