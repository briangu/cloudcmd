package cloudcmd.common;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Set;

public class FileMetaData
{
  // TODO: support file-subblocks
  public JSONArray BlockHashes;
  public Set<String> Tags;
  public JSONObject Meta;
  public String MetaHash;
  public byte[] MetaBytes;
}
