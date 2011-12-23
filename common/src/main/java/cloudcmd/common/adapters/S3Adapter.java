package cloudcmd.common.adapters;

import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.Mimetypes;
import org.jets3t.service.utils.ServiceUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.util.Set;

//     "s3://<aws id>:<aws secret>@<bucket>?tier=2&tags=s3"

// http://www.jets3t.org/toolkit/code-samples.html#connecting
public class S3Adapter extends Adapter
{
  public S3Adapter()
  {
  }

  @Override
  public void init(Integer tier, String type, Set<String> tags, URI uri) throws Exception
  {
    super.init(tier, type, tags, uri);
  }

  @Override
  public void refreshCache() throws Exception
  {
    // TODO: use h2 db to hold list of bucket objects
  }

  @Override
  public boolean contains(String hash) throws Exception
  {
    // TODO: query h2 db
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void shutdown()
  {
    // TODO: shutdown connection pool
  }

  @Override
  public void store(InputStream data, String hash)
  {
    // TODO: check local h2 db for presence
    //       if not present, push to s3 and update db cache info

/*
    S3Object s3Object = new S3Object("HelloWorld2.txt");
    s3Object.setDataInputStream(data);
    s3Object.setContentLength(data.available());
    s3Object.setContentType("text/plain");
    s3Object = new S3Object(new File());

    setContentLength(file.length());
    setContentType(Mimetypes.getInstance().getMimetype(file));
    if (!file.exists()) {
        throw new FileNotFoundException("Cannot read from file: " + file.getAbsolutePath());
    }
    setDataInputFile(file);
    setMd5Hash(ServiceUtils.computeMD5Hash(new FileInputStream(file)));
*/
  }

  @Override
  public String store(InputStream data) throws Exception
  {
    throw new NotImplementedException();
    // TODO: cache locally and compute hash
    //       chech cache for presence
    //       if not present, then push and update cache
  }

  @Override
  public InputStream load(String hash)
  {
    // TODO: if present in cache, fetch and update cache
    return null;
  }

  @Override
  public Set<String> describe()
  {
    // TODO: scan db
    return null;
  }
}
