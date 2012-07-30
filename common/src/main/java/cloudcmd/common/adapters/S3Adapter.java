package cloudcmd.common.adapters;


import cloudcmd.common.CryptoUtil;
import cloudcmd.common.FileChannelBuffer;
import cloudcmd.common.SqlUtil;
import cloudcmd.common.UriUtil;
import org.apache.commons.io.IOUtils;
import org.h2.jdbcx.JdbcConnectionPool;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.sql.*;
import java.util.*;

//     "s3://<aws id>@<bucket>?tier=2&tags=s3&secret=<aws secret>"

// http://www.jets3t.org/toolkit/code-samples.html#connecting
public class S3Adapter extends Adapter implements MD5Storable {
  String _bucketName;
  RestS3Service _s3Service;
  JdbcConnectionPool _cp = null;
  Set<String> _description = null;

  public S3Adapter() {}

  @Override
  public void init(String configDir, Integer tier, String type, Set<String> tags, URI uri) throws Exception {
    super.init(configDir, tier, type, tags, uri);

    List<String> awsInfo = parseAwsInfo(uri);
    AWSCredentials creds = new AWSCredentials(awsInfo.get(0), awsInfo.get(1));
    _s3Service = new RestS3Service(creds);
    _bucketName = awsInfo.get(2);

    bootstrap();
  }

  private String getDbFile() {
    return String.format("%s%s%s", ConfigDir, File.separator, _bucketName);
  }

  private String createConnectionString() {
    return String.format("jdbc:h2:%s", getDbFile());
  }

  private Connection getDbConnection() throws SQLException {
    return _cp.getConnection();
  }

  private Connection getReadOnlyDbConnection() throws SQLException {
    Connection conn = getDbConnection();
    conn.setReadOnly(true);
    return conn;
  }

  private void bootstrap()
    throws Exception {
    Class.forName("org.h2.Driver");
    _cp = JdbcConnectionPool.create(createConnectionString(), "sa", "sa");
    File file = new File(getDbFile() + ".h2.db");
    if (!file.exists()) {
      bootstrapDb();
      bootstrapS3();
    }
  }

  private void bootstrapS3()
    throws S3ServiceException {
    _s3Service.getOrCreateBucket(_bucketName);
  }

  private void bootstrapDb() {
    Connection db = null;
    Statement st = null;
    try {
      db = getDbConnection();
      st = db.createStatement();

      st.execute("DROP TABLE if exists BLOCK_INDEX;");
      st.execute("CREATE TABLE BLOCK_INDEX ( HASH VARCHAR PRIMARY KEY );");

      db.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      SqlUtil.SafeClose(st);
      SqlUtil.SafeClose(db);
    }
  }

  public void purge() {
    bootstrapDb();
  }

  private static List<String> parseAwsInfo(URI adapterUri) {
    String[] parts = adapterUri.getAuthority().split("@");
    if (parts.length != 2) throw new IllegalArgumentException("authority format: awsKey@bucketname");

    Map<String, String> queryParams = UriUtil.parseQueryString(adapterUri);
    if (!queryParams.containsKey("secret")) throw new IllegalArgumentException("missing aws secret");

    return Arrays.asList(parts[0], queryParams.get("secret"), parts[1]);
  }

  @Override
  public void refreshCache() throws Exception {
    S3Object[] s3Objects = _s3Service.listObjects(_bucketName);

    purge();

    Connection db = null;
    PreparedStatement statement = null;
    try {
      db = getDbConnection();

      db.setAutoCommit(false);

      statement = db.prepareStatement("INSERT INTO BLOCK_INDEX VALUES (?)");

      int k = 0;

      for (S3Object s3Object : s3Objects) {
        statement.setString(1, s3Object.getName());
        statement.addBatch();

        if (++k > 1024) {
          statement.executeBatch();
          k = 0;
        }
      }

      statement.executeBatch();

      db.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(db);
    }
  }

  private void insertHash(String hash) throws Exception {
    Connection db = null;
    PreparedStatement statement = null;
    try {
      db = getDbConnection();

      db.setAutoCommit(false);

      statement = db.prepareStatement("INSERT INTO BLOCK_INDEX VALUES (?)");
      statement.setString(1, hash);
      statement.execute();

      db.commit();

      _description.add(hash);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(db);
    }
  }

  @Override
  public boolean contains(String hash) throws Exception {
    return describe().contains(hash);
  }

  @Override
  public void shutdown() {
    if (_cp != null) {
      _cp.dispose();
    }
  }

  @Override
  public boolean remove(String hash) throws Exception {
    _s3Service.deleteObject(_bucketName, hash);
    _description.remove(hash);
    return true;
  }

  @Override
  public boolean verify(String hash) throws Exception {
    // we rely on S3 md5 integrity check that we used on push
    return true;
  }

  @Override
  public void store(InputStream data, String hash)
    throws Exception {

    if (data instanceof ByteArrayInputStream) {
      ByteArrayInputStream buffer = (ByteArrayInputStream)data;
      byte[] md5Hash = CryptoUtil.computeMD5Hash(Channels.newChannel(buffer));
      buffer.reset();
      store(buffer, hash, md5Hash, buffer.available());
    } else if (data instanceof FileInputStream) {
      FileInputStream buffer = (FileInputStream)data;
      byte[] md5Hash = CryptoUtil.computeMD5Hash(buffer.getChannel());
      buffer.reset();
      store(buffer, hash, md5Hash, buffer.available());
    } else {
      byte[] buffer = IOUtils.toByteArray(data);
      byte[] md5Hash = CryptoUtil.computeMD5Hash(Channels.newChannel(new ByteArrayInputStream(buffer)));
      store(new ByteArrayInputStream(buffer), hash, md5Hash, buffer.length);
    }
  }

  @Override
  public void store(InputStream data, String hash, byte[] md5Digest, long length)
    throws Exception {
    if (contains(hash)) return;

    S3Object s3Object = new S3Object(hash);
    s3Object.setDataInputStream(data);
    s3Object.setContentLength(length);
    s3Object.setMd5Hash(md5Digest);
    s3Object.setBucketName(_bucketName);

    _s3Service.putObject(_bucketName, s3Object);

    insertHash(hash);
  }

  @Override
  public InputStream load(String hash)
    throws Exception {
    if (!contains(hash)) throw new DataNotFoundException(hash);
    return _s3Service.getObject(_bucketName, hash).getDataInputStream();
  }

  @Override
  public ChannelBuffer loadChannel(String hash) throws Exception {
    if (!contains(hash)) throw new DataNotFoundException(hash);
    S3Object s3Object = _s3Service.getObject(_bucketName, hash);
    int length = new Long(s3Object.getContentLength()).intValue();
    return new FileChannelBuffer(s3Object.getDataInputStream(), length);
  }

/*
  @Override
  public ChannelBuffer loadChannel(String hash)
    throws Exception
  {
    if (!contains(hash)) throw new DataNotFoundException(hash);
    S3Object s3Object = _s3Service.getObject(_bucketName, hash);
    DataInputStream dis = new DataInputStream(s3Object.getDataInputStream());
    int length = new Long(s3Object.getContentLength()).intValue();
    byte[] buffer = new byte[length];
    dis.readFully(buffer);
    return new FileChannelBuffer(new ByteArrayInputStream(buffer), new Long(s3Object.getContentLength()).intValue());
  }
*/

  @Override
  public Set<String> describe()
    throws Exception {
    return Collections.unmodifiableSet(getDescription());
  }

  private Set<String> getDescription()
    throws Exception {
    if (_description != null) {
      return _description;
    }

    synchronized (this) {
      if (_description == null) {
        Set<String> description = new HashSet<String>();

        Connection db = null;
        PreparedStatement statement = null;

        try {
          db = getReadOnlyDbConnection();

          statement = db.prepareStatement("SELECT HASH FROM BLOCK_INDEX");

          ResultSet resultSet = statement.executeQuery();

          while (resultSet.next()) {
            description.add(resultSet.getString("HASH"));
          }
        } catch (SQLException e) {
          e.printStackTrace();
        } finally {
          SqlUtil.SafeClose(statement);
          SqlUtil.SafeClose(db);
        }

        _description = description;
      }
    }

    return _description;
  }
}
