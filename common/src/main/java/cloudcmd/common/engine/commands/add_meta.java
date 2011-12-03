package cloudcmd.common.engine.commands;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.engine.BlockCacheService;
import cloudcmd.common.index.IndexStorageService;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import ops.AsyncCommand;
import ops.CommandContext;
import ops.MemoryElement;
import org.json.JSONArray;


public class add_meta implements AsyncCommand
{
  @Override
  public void exec(CommandContext context, Object[] args)
      throws Exception
  {
    FileMetaData fmd = (FileMetaData)args[0];
    IndexStorageService.instance().add(fmd);
  }
}
