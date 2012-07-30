package cloudcmd.cld;

import cloudcmd.common.engine.CloudEngine;
import cloudcmd.common.engine.ParallelCloudEngine;

public class CloudEngineService {
  private static CloudEngine _instance = new ParallelCloudEngine();

  public static CloudEngine instance() {
    return _instance;
  }
}
