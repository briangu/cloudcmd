package cloudcmd.common.engine.commands;


import cloudcmd.common.config.ConfigStorageService;
import ops.Command;
import ops.CommandContext;


public class debug implements Command
{
  @Override
  public void exec(CommandContext context, Object[] args)
  {
    if (!ConfigStorageService.instance().isDebugEnabled()) return;

    String fmt = args[0].toString();

    String s = new String(fmt);

    for (int i = 1; i < args.length; i++)
    {
      String val = args[i].toString();
      s = s.replace(String.format("{%d}", i-1), val);
    }

    System.out.println(s);
  }
}
