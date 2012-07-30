package cloudcmd.common.adapters;

import java.io.InputStream;

public interface MD5Storable {
  public void store(InputStream data, String hash, byte[] md5, long length) throws Exception;
}
