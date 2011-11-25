package cloudcmd.common;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class OpsLoader
{
  public static File load(String opsResource)
  {
    URL resource = OpsLoader.class.getResource("/ops/"+opsResource);
    try {
      return new File(resource.toURI());
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return null;
  }
}
