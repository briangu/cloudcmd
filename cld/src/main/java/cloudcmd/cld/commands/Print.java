package cloudcmd.cld.commands;


import cloudcmd.common.JsonUtil;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.SubCommand;


@SubCommand(name = "print", description = "Pretty print JSON from other commands.")
public class Print implements Command
{
  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    System.out.println(JsonUtil.loadJsonArray(System.in).toString(2));
  }
}
