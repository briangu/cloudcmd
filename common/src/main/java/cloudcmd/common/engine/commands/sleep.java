package cloudcmd.common.engine.commands;


import ops.Command;
import ops.CommandContext;


public class sleep implements Command
{
  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    Integer sleep = (Integer) args[0];
    Thread.sleep(sleep);
  }
}
