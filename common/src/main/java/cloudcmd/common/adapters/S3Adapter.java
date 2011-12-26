package cloudcmd.common.adapters;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.ServiceUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

//     "s3://<aws id>:<aws secret>@<bucket>?tier=2&tags=s3"

// http://www.jets3t.org/toolkit/code-samples.html#connecting
public class S3Adapter extends Adapter
{
  String _bucketName;
  RestS3Service _s3Service;

  public S3Adapter()
  {
  }

  @Override
  public void init(Integer tier, String type, Set<String> tags, URI uri) throws Exception
  {
    super.init(tier, type, tags, uri);

    List<String> awsInfo = parseAuthority(uri.getAuthority());
    AWSCredentials creds = new AWSCredentials(awsInfo.get(0), awsInfo.get(1));
    _s3Service = new RestS3Service(creds);
    _bucketName = awsInfo.get(2);
  }

  private static List<String> parseAuthority(String authority)
  {
    String[] parts = authority.split("@");
    if (parts.length != 2) throw new IllegalArgumentException("authority format: awsKey:awssecret@bucketname");

    String[] awsParts = parts[0].split(":");
    if (awsParts.length != 2) throw new IllegalArgumentException("authority format: awsKey:awssecret@bucketname");

    return Arrays.asList(awsParts[0], awsParts[1], parts[1]);
  }

  @Override
  public void refreshCache() throws Exception
  {
    S3Object[] s3Objects = _s3Service.listObjects(_bucketName);

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
      throws Exception
  {
    // TODO: check local h2 db for presence
    //       if not present, push to s3 and update db cache info

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    IOUtils.copy(data, baos);
    byte[] arr = baos.toByteArray();

    S3Object s3Object = new S3Object(hash);
    s3Object.setDataInputStream(new ByteArrayInputStream(arr));
    s3Object.setContentLength(data.available());
    s3Object.setMd5Hash(ServiceUtils.computeMD5Hash(new ByteArrayInputStream(arr)));
    s3Object.setBucketName(_bucketName);

    _s3Service.putObject(_bucketName, s3Object);
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
      throws Exception
  {
    // TODO: if present in cache, fetch and update cache
    S3Object s3Object = _s3Service.getObject(_bucketName, hash);
    return s3Object.getDataInputStream();
  }

  @Override
  public Set<String> describe()
      throws Exception
  {
    // TODO: scan db
    return null;
  }
}
