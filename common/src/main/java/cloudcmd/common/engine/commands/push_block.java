package cloudcmd.common.engine.commands;


import cloudcmd.common.adapters.Adapter;
import ops.AsyncCommand;
import ops.CommandContext;


public class push_block implements AsyncCommand
{
  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    Adapter dest = (Adapter)args[0];
    Adapter src = (Adapter)args[1];
    String hash = (String) args[2];

    if (!dest.contains(hash))
    {
      try
      {
        dest.store(src.load(hash), hash);
      }
      catch (Exception e)
      {
        // TODO: failed
        e.printStackTrace();
      }
    }
  }
}
