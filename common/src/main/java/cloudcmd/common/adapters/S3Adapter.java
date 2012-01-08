package cloudcmd.common.adapters;


import cloudcmd.common.SqlUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.h2.jdbcx.JdbcConnectionPool;
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
  JdbcConnectionPool _cp = null;
  Set<String> _description = null;

  public S3Adapter()
  {
  }

  @Override
  public void init(String configDir, Integer tier, String type, Set<String> tags, URI uri) throws Exception
  {
    super.init(configDir, tier, type, tags, uri);

    List<String> awsInfo = parseAuthority(uri.getAuthority());
    AWSCredentials creds = new AWSCredentials(awsInfo.get(0), awsInfo.get(1));
    _s3Service = new RestS3Service(creds);
    _bucketName = awsInfo.get(2);

    bootstrap();
  }

  private String getDbFile()
  {
    return String.format("%s%s%s", ConfigDir, File.separator, _bucketName);
  }

  private String createConnectionString()
  {
    return String.format("jdbc:h2:%s", getDbFile());
  }

  private Connection getDbConnection() throws SQLException
  {
    return _cp.getConnection();
  }

  private Connection getReadOnlyDbConnection() throws SQLException
  {
    Connection conn = getDbConnection();
    conn.setReadOnly(true);
    return conn;
  }

  private void bootstrap()
      throws Exception
  {
    Class.forName("org.h2.Driver");
    _cp = JdbcConnectionPool.create(createConnectionString(), "sa", "sa");
    File file = new File(getDbFile() + ".h2.db");
    if (!file.exists())
    {
      bootstrapDb();
      bootstrapS3();
    }
  }

  private void bootstrapS3()
      throws S3ServiceException
  {
    _s3Service.getOrCreateBucket(_bucketName);
  }

  private void bootstrapDb()
  {
    Connection db = null;
    Statement st = null;
    try
    {
      db = getDbConnection();
      st = db.createStatement();

      st.execute("DROP TABLE if exists BLOCK_INDEX;");
      st.execute("CREATE TABLE BLOCK_INDEX ( HASH VARCHAR PRIMARY KEY );");

      db.commit();
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(st);
      SqlUtil.SafeClose(db);
    }
  }

  public void purge()
  {
    Connection db = null;
    Statement st = null;
    try
    {
      db = getDbConnection();
      st = db.createStatement();
      st.execute("delete from BLOCK_INDEX;");

      _description = null;
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(st);
      SqlUtil.SafeClose(db);
    }
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

    purge();

    Connection db = null;
    PreparedStatement statement = null;
    try
    {
      db = getDbConnection();

      db.setAutoCommit(false);

      statement = db.prepareStatement("INSERT INTO BLOCK_INDEX VALUES (?)");

      for (S3Object s3Object : s3Objects)
      {
        statement.setString(0, s3Object.getName());
        statement.execute();
        statement.clearParameters();
      }

      db.commit();
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(db);
    }
  }

  @Override
  public boolean contains(String hash) throws Exception
  {
    return describe().contains(hash);
  }

  @Override
  public void shutdown()
  {
    if (_cp != null)
    {
      _cp.dispose();
    }
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
    if (!contains(hash)) throw new DataNotFoundException(hash);
    return _s3Service.getObject(_bucketName, hash).getDataInputStream();
  }

  @Override
  public Set<String> describe()
      throws Exception
  {
    if (_description == null)
    {
      _description = _describe();
    }

    return _description;
  }

  private Set<String> _describe()
      throws Exception
  {
    Set<String> description = new HashSet<String>();

    Connection db = null;
    PreparedStatement statement = null;

    try
    {
      db = getReadOnlyDbConnection();

      statement = db.prepareStatement("SELECT * FROM BLOCK_INDEX");

      ResultSet resultSet = statement.executeQuery();

      while (resultSet.next())
      {
        description.add(resultSet.getString("HASH"));
      }
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(db);
    }

    return Collections.unmodifiableSet(description);
  }
}
