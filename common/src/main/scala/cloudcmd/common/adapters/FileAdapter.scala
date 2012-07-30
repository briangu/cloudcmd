package cloudcmd.common.adapters

import cloudcmd.common._
import cloudcmd.common.engine.FileWalker
import org.h2.jdbcx.JdbcConnectionPool
import org.jboss.netty.buffer.ChannelBuffer
import java.io._
import java.net.URI
import java.sql._
import java.util.UUID
import collection.mutable

//     "file:///tmp/storage?tier=1&tags=image,movie,vacation"
object FileAdapter {
  private def initSubDirs(rootPath: String) {
    (0 until 0x100).par.foreach(i => new File(rootPath + File.separator + "%02x".format(i)).mkdirs)
  }

  val MIN_FREE_STORAGE_SIZE: Int = 1024 * 1024
  val LARGE_FILE_CUTOFF: Int = 128 * 1024 * 1024
}

class FileAdapter extends Adapter with InlineStorable {

  private var _rootPath: String = null
  private var _cp: JdbcConnectionPool = null
  @volatile
  private var _description: mutable.HashSet[String] with mutable.SynchronizedSet[String] = null
  private var _dbDir: String = null
  private var _dataDir: String = null

  override def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], config: URI) {
    super.init(configDir, tier, adapterType, tags, config)
    _rootPath = URI.getPath
    _dbDir = _rootPath + File.separator + "db"
    ConfigDir = _dbDir
    _dataDir = _rootPath + File.separator + "data"
    val rootPathDir: File = new File(_rootPath)
    rootPathDir.mkdirs
    _isOnline = rootPathDir.exists
    if (_isOnline) bootstrap(_dataDir, _dbDir)
  }

  private def getDbFileName(dbPath: String): String = "%s%sindex".format(, dbPath, File.separator)

  private def createConnectionString(dbPath: String): String = "jdbc:h2:%s".format(getDbFileName(dbPath))

  private def getDbConnection: Connection = _cp.getConnection

  private def getReadOnlyDbConnection: Connection = {
    val conn: Connection = getDbConnection
    conn.setReadOnly(true)
    conn
  }

  private def bootstrap(dataPath: String, dbPath: String) {
    Class.forName("org.h2.Driver")
    _cp = JdbcConnectionPool.create(createConnectionString(dbPath), "sa", "sa")
    val file: File = new File(getDbFileName(dbPath) + ".h2.db")
    if (!file.exists) {
      bootstrapDb
      FileAdapter.initSubDirs(dataPath)
    }
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
      case e: SQLException => e.printStackTrace
    }
    finally {
      SqlUtil.SafeClose(st)
      SqlUtil.SafeClose(db)
    }
  }

  override def IsOnLine(): Boolean = _isOnline
  override def IsFull(): Boolean = new File(_rootPath).getUsableSpace < FileAdapter.MIN_FREE_STORAGE_SIZE

  def refreshCache {
    val foundHashes = rebuildHashIndexFromDisk
    val description = getDescription.toSet
    val newHashes = foundHashes -- description
    addToDb(newHashes)
    val deletedHashes = description -- foundHashes
    deleteFromDb(deletedHashes)
  }

  private def addToDb(hashes: Set[String]) {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      statement = db.prepareStatement("INSERT INTO BLOCK_INDEX VALUES (?)")
      var k = 0
      for (hash <- hashes) {
        statement.setString(1, hash)
        statement.addBatch
        k += 1
        if (k > 1024) {
          statement.executeBatch
          k = 0
        }
      }
      statement.executeBatch
      db.commit
    }
    catch {
      case e: SQLException => e.printStackTrace
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  def contains(hash: String): Boolean = getDescription.contains(hash)

  def shutdown {
    if (_cp != null) {
      _cp.dispose
    }
  }

  def remove(hash: String): Boolean = {
    val file: File = new File(getDataFileFromHash(hash))
    if (file.exists) file.delete
    deleteFromDb(Set(hash))
    true
  }

  private def deleteFromDb(hashes: Set[String]) {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      statement = db.prepareStatement("DELETE FROM BLOCK_INDEX WHERE HASH = ?")
      var k = 0
      for (hash <- hashes) {
        statement.setString(1, hash)
        statement.addBatch
        k += 1
        if (k > 1024) {
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

  def verify(hash: String): Boolean = {
    val file: File = new File(getDataFileFromHash(hash))
    if (!file.exists) false
    val idx: Int = hash.lastIndexOf(".")
    val testHash = if (idx >= 0) {
      hash.substring(0, idx)
    } else {
      hash
    }
    CryptoUtil.computeHashAsString(file) == testHash
  }

  private def getHashFromDataFile(hash: String): String = {
    val idx: Int = hash.lastIndexOf(".")
    if ((idx >= 0)) hash.substring(0, idx) else hash
  }

  private def getPathFromHash(hash: String): String = _dataDir + File.separator + hash.substring(0, 2)

  private def getDataFileFromHash(hash: String): String = getPathFromHash(hash) + File.separator + hash

  def store(is: InputStream, hash: String) {
    val writeHash: String = FileUtil.writeFileAndComputeHash(is, new File(getDataFileFromHash(hash)))
    if (!(writeHash == getHashFromDataFile(hash))) {
      throw new RuntimeException(String.format("failed to store data: expected %s got %s", hash, writeHash))
    }
    insertHash(hash)
  }

  def store(is: InputStream): String = {
    if (is.available > FileAdapter.LARGE_FILE_CUTOFF) {
      storeLargeFile(is)
    }
    var baos: ByteArrayOutputStream = null
    var bais: ByteArrayInputStream = null
    var hash: String = null
    try {
      baos = new ByteArrayOutputStream
      hash = CryptoUtil.digestToString(CryptoUtil.writeAndComputeHash(is, baos))
      bais = new ByteArrayInputStream(baos.toByteArray)
      FileUtil.writeFile(bais, getDataFileFromHash(hash))
      insertHash(hash)
    }
    finally {
      FileUtil.SafeClose(bais)
      FileUtil.SafeClose(baos)
    }
    hash
  }

  private def storeLargeFile(is: InputStream): String = {
    val tmpFile: File = new File(_dataDir + File.separator + UUID.randomUUID.toString + ".tmp")
    tmpFile.createNewFile
    val hash: String = FileUtil.writeFileAndComputeHash(is, tmpFile)
    val newFile: File = new File(getDataFileFromHash(hash))
    if (newFile.exists && newFile.length == tmpFile.length) {
      tmpFile.delete
    }
    else {
      val success: Boolean = tmpFile.renameTo(newFile)
      if (!success) {
        tmpFile.delete
        throw new IOException("failed to move file: " + tmpFile.getAbsolutePath)
      }
    }
    insertHash(hash)
    hash
  }

  def load(hash: String): InputStream = {
    val file: File = new File(getDataFileFromHash(hash))
    if (!file.exists) {
      System.err.println(String.format("could not find hash %s on %s.", hash, URI.toString))
      throw new DataNotFoundException(hash)
    }
    new FileInputStream(file)
  }

  def loadChannel(hash: String): ChannelBuffer = {
    val file: File = new File(getDataFileFromHash(hash))
    if (!file.exists) {
      System.err.println(String.format("could not find hash %s on %s.", hash, URI.toString))
      throw new DataNotFoundException(hash)
    }
    new FileChannelBuffer(file)
  }

  private def insertHash(hash: String) {
    if (getDescription.contains(hash)) return
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection
      statement = db.prepareStatement("INSERT INTO BLOCK_INDEX VALUES (?)")
      statement.setString(1, hash)
      statement.execute
      getDescription.add(hash)
    }
    catch {
      case e: SQLException => e.printStackTrace
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  def describe: Set[String] = {
    getDescription.toSet
  }

  private def getDescription: mutable.HashSet[String] with mutable.SynchronizedSet[String] = {
    if (_description != null) {
      _description
    }
    this synchronized {
      if (_description == null) {
        val description = new mutable.HashSet[String] with mutable.SynchronizedSet[String]
        var db: Connection = null
        var statement: PreparedStatement = null
        try {
          db = getReadOnlyDbConnection
          statement = db.prepareStatement("SELECT HASH FROM BLOCK_INDEX")
          val resultSet: ResultSet = statement.executeQuery
          while (resultSet.next) {
            description.add(resultSet.getString("HASH"))
          }
          _description = description
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
    }
    _description
  }

  def rebuildHashIndexFromDisk: Set[String] = {
    val hashes = new mutable.HashSet[String] with mutable.SynchronizedSet[String]
    FileWalker.enumerateFolders(_dataDir, new FileWalker.FileHandler {
      def skipDir(file: File): Boolean = {
        false
      }

      def process(file: File) {
        hashes.add(file.getName)
      }
    })
    hashes.toSet
  }
}