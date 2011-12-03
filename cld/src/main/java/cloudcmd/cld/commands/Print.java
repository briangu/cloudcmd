package cloudcmd.cld.commands;


import jpbetz.cli.*;
import cloudcmd.common.JsonUtil;

import org.json.JSONException;
import org.json.JSONObject;


@SubCommand(name = "print", description = "Pretty print JSON from other commands.")
public class Print implements Command
{
  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    System.out.println(JsonUtil.loadJsonArray(System.in).toString(2));
  }
}
