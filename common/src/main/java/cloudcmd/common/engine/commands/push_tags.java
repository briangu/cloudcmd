package cloudcmd.common.engine.commands;


import cloudcmd.common.MetaUtil;
import cloudcmd.common.adapters.Adapter;
import ops.AsyncCommand;
import ops.CommandContext;
import org.json.JSONArray;


public class push_tags implements AsyncCommand
{
  @Override
  public void exec(final CommandContext context, Object[] args)
      throws Exception
  {
    final Adapter adapter = (Adapter)args[0];
    final String hash = (String) args[1];
    final JSONArray tags = (JSONArray) args[2];

    try
    {
      adapter.storeTags(hash, MetaUtil.createTagSet(tags));
    }
    catch (Exception e)
    {
      // TODO: failed to push tags to the adapter, put status back into WM
      // context.make(new MemoryElement(""));
      e.printStackTrace();
    }
  }
}
