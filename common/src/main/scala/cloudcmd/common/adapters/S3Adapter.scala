package cloudcmd.common.adapters

import cloudcmd.common.CryptoUtil
import cloudcmd.common.FileChannelBuffer
import cloudcmd.common.SqlUtil
import cloudcmd.common.UriUtil
import org.apache.commons.io.IOUtils
import org.h2.jdbcx.JdbcConnectionPool
import org.jboss.netty.buffer.ChannelBuffer
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.S3Object
import org.jets3t.service.security.AWSCredentials
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.net.URI
import java.nio.channels.Channels
import java.sql._
import java.util._

//     "s3://<aws id>@<bucket>?tier=2&tags=s3&secret=<aws secret>"
object S3Adapter {
  private def parseAwsInfo(adapterUri: URI): List[String] = {
    val parts: Array[String] = adapterUri.getAuthority.split("@")
    if (parts.length != 2) throw new IllegalArgumentException("authority format: awsKey@bucketname")
    val queryParams: Map[String, String] = UriUtil.parseQueryString(adapterUri)
    if (!queryParams.containsKey("secret")) throw new IllegalArgumentException("missing aws secret")
    Arrays.asList(parts(0), queryParams.get("secret"), parts(1))
  }
}

class S3Adapter extends Adapter with MD5Storable {

  private[adapters] var _bucketName: String = null
  private[adapters] var _s3Service: RestS3Service = null
  private[adapters] var _cp: JdbcConnectionPool = null
  private[adapters] var _description: Set[String] = null

  def init(configDir: String, tier: Integer, `type`: String, tags: Set[String], uri: URI) {
    super.init(configDir, tier, `type`, tags, uri)
    val awsInfo: List[String] = parseAwsInfo(uri)
    val creds: AWSCredentials = new AWSCredentials(awsInfo.get(0), awsInfo.get(1))
    _s3Service = new RestS3Service(creds)
    _bucketName = awsInfo.get(2)
    bootstrap
  }

  private def getDbFile: String = {
    String.format("%s%s%s", ConfigDir, File.separator, _bucketName)
  }

  private def createConnectionString: String = {
    String.format("jdbc:h2:%s", getDbFile)
  }

  private def getDbConnection: Connection = {
    _cp.getConnection
  }

  private def getReadOnlyDbConnection: Connection = {
    val conn: Connection = getDbConnection
    conn.setReadOnly(true)
    conn
  }

  private def bootstrap {
    Class.forName("org.h2.Driver")
    _cp = JdbcConnectionPool.create(createConnectionString, "sa", "sa")
    val file: File = new File(getDbFile + ".h2.db")
    if (!file.exists) {
      bootstrapDb
      bootstrapS3
    }
  }

  private def bootstrapS3 {
    _s3Service.getOrCreateBucket(_bucketName)
  }

  private def bootstrapDb {
    var db: Connection = null
    var st: Statement = null
    try {
      db = getDbConnection
      st = db.createStatement
      st.execute("DROP TABLE if exists BLOCK_INDEX")
      st.execute("CREATE TABLE BLOCK_INDEX ( HASH VARCHAR PRIMARY KEY )")
      db.commit
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    finally {
      SqlUtil.SafeClose(st)
      SqlUtil.SafeClose(db)
    }
  }

  def purge {
    bootstrapDb
  }

  def refreshCache {
    val s3Objects: Array[S3Object] = _s3Service.listObjects(_bucketName)
    purge
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      statement = db.prepareStatement("INSERT INTO BLOCK_INDEX VALUES (?)")
      var k: Int = 0
      for (s3Object <- s3Objects) {
        statement.setString(1, s3Object.getName)
        statement.addBatch
        if (({
          k += 1 k - 1
        }) > 1024) {
          statement.executeBatch
          k = 0
        }
      }
      statement.executeBatch
      db.commit
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  private def insertHash(hash: String) {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      statement = db.prepareStatement("INSERT INTO BLOCK_INDEX VALUES (?)")
      statement.setString(1, hash)
      statement.execute
      db.commit
      _description.add(hash)
    }
    catch {
      case e: SQLException => {
        e.printStackTrace
      }
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  def contains(hash: String): Boolean = {
    describe.contains(hash)
  }

  def shutdown {
    if (_cp != null) {
      _cp.dispose
    }
  }

  def remove(hash: String): Boolean = {
    _s3Service.deleteObject(_bucketName, hash)
    _description.remove(hash)
    true
  }

  def verify(hash: String): Boolean = {
    true
  }

  def store(data: InputStream, hash: String) {
    if (data.isInstanceOf[ByteArrayInputStream]) {
      val buffer: ByteArrayInputStream = data.asInstanceOf[ByteArrayInputStream]
      val md5Hash: Array[Byte] = CryptoUtil.computeMD5Hash(Channels.newChannel(buffer))
      buffer.reset
      store(buffer, hash, md5Hash, buffer.available)
    }
    else if (data.isInstanceOf[FileInputStream]) {
      val buffer: FileInputStream = data.asInstanceOf[FileInputStream]
      val md5Hash: Array[Byte] = CryptoUtil.computeMD5Hash(buffer.getChannel)
      buffer.reset
      store(buffer, hash, md5Hash, buffer.available)
    }
    else {
      val buffer: Array[Byte] = IOUtils.toByteArray(data)
      val md5Hash: Array[Byte] = CryptoUtil.computeMD5Hash(Channels.newChannel(new ByteArrayInputStream(buffer)))
      store(new ByteArrayInputStream(buffer), hash, md5Hash, buffer.length)
    }
  }

  def store(data: InputStream, hash: String, md5Digest: Array[Byte], length: Long) {
    if (contains(hash)) return
    val s3Object: S3Object = new S3Object(hash)
    s3Object.setDataInputStream(data)
    s3Object.setContentLength(length)
    s3Object.setMd5Hash(md5Digest)
    s3Object.setBucketName(_bucketName)
    _s3Service.putObject(_bucketName, s3Object)
    insertHash(hash)
  }

  def load(hash: String): InputStream = {
    if (!contains(hash)) throw new DataNotFoundException(hash)
    _s3Service.getObject(_bucketName, hash).getDataInputStream
  }

  def loadChannel(hash: String): ChannelBuffer = {
    if (!contains(hash)) throw new DataNotFoundException(hash)
    val s3Object: S3Object = _s3Service.getObject(_bucketName, hash)
    val length: Int = new Long(s3Object.getContentLength).intValue
    new FileChannelBuffer(s3Object.getDataInputStream, length)
  }

  def describe: Set[String] = {
    Collections.unmodifiableSet(getDescription)
  }

  private def getDescription: Set[String] = {
    if (_description != null) {
      _description
    }
    this synchronized {
      if (_description == null) {
        val description: Set[String] = new HashSet[String]
        var db: Connection = null
        var statement: PreparedStatement = null
        try {
          db = getReadOnlyDbConnection
          statement = db.prepareStatement("SELECT HASH FROM BLOCK_INDEX")
          val resultSet: ResultSet = statement.executeQuery
          while (resultSet.next) {
            description.add(resultSet.getString("HASH"))
          }
        }
        catch {
          case e: SQLException => {
            e.printStackTrace
          }
        }
        finally {
          SqlUtil.SafeClose(statement)
          SqlUtil.SafeClose(db)
        }
        _description = description
      }
    }
    _description
  }
}