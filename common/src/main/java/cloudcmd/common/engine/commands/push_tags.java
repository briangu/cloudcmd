package cloudcmd.common.engine.commands;


import cloudcmd.common.adapters.Adapter;
import java.io.ByteArrayInputStream;
import ops.Command;
import ops.CommandContext;
import org.json.JSONArray;


public class push_tags implements Command
{
  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    Adapter adapter = (Adapter)args[0];
    String hash = (String) args[1];
    JSONArray tags = (JSONArray) args[2];
    adapter.storeTags(new ByteArrayInputStream(tags.toString().getBytes()), hash);
  }
}
