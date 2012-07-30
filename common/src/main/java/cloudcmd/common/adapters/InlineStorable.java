package cloudcmd.common.adapters;

import java.io.InputStream;

public interface InlineStorable {
  // store the file and compute the hash at runtime
  public abstract String store(InputStream data) throws Exception;
}
