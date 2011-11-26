package cloudcmd.common.adapters;

import org.json.JSONObject;

public class DataNotFoundException extends Exception
{
  public JSONObject Meta;
  public DataNotFoundException(JSONObject meta)
  {
    Meta = meta;
  }
}
