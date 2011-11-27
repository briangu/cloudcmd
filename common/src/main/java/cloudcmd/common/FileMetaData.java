package cloudcmd.common;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;
import java.util.Set;

public class FileMetaData
{
  // TODO: support file-subblocks
  public JSONArray BlockHashes;
  public List<String> Tags;
  public JSONObject Meta;
  public String MetaHash;
  public byte[] MetaBytes;
}
