package cloudcmd.common.engine.commands;


import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.LocalCacheService;
import ops.Command;
import ops.CommandContext;


public class push_block implements Command
{
  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    Adapter dest = (Adapter)args[0];
    Adapter src = (Adapter)args[1];
    String hash = (String) args[2];
    dest.store(src.load(hash), hash);
  }
}
