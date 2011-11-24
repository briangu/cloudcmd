package cloudcmd.cld;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by IntelliJ IDEA.
 * User: brian
 * Date: 11/24/11
 * Time: 10:30 AM
 * To change this template use File | Settings | File Templates.
 */
public class OpsLoader {
  public static File load(String opsResource)
  {
    URL resource = OpsLoader.class.getResource("/"+opsResource);
    try {
      return new File(resource.toURI());
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return null;
  }
}
