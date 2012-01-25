package cloudcmd.common;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class UriUtil
{
  public static Map<String, String> parseQueryString(URI uri)
  {
    Map<String, String> queryParams = new HashMap<String, String>();

    String query = uri.getQuery();

    if (query == null || query.isEmpty()) return queryParams;

    String[] parts = query.split("&");

    for (String part : parts)
    {
      String[] subParts = part.split("=");
      if (subParts.length == 1)
      {
        queryParams.put(subParts[0], "");
      }
      else if (subParts.length == 2)
      {
        queryParams.put(subParts[0], subParts[1]);
      }
    }

    return queryParams;
  }
}
