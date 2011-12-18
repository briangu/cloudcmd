package cloudcmd.common.engine.commands;


import cloudcmd.common.FileUtil;
import cloudcmd.common.adapters.Adapter;
import ops.AsyncCommand;
import ops.CommandContext;
import org.apache.log4j.Logger;

import java.io.InputStream;


public class push_block implements AsyncCommand
{
  static Logger log = Logger.getLogger(push_block.class);

  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    Adapter dest = (Adapter)args[0];
    Adapter src = (Adapter)args[1];
    String hash = (String) args[2];

    if (dest.contains(hash)) return;

    InputStream is = null;

    try
    {
      is = src.load(hash);
      dest.store(is, hash);
    }
    catch (Exception e)
    {
      context.make("error_push_block", "hash", hash);
      log.error(hash, e);
    }
    finally
    {
      FileUtil.SafeClose(is);
    }
  }
}
