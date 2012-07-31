package cloudcmd.common.adapters

import cloudcmd.common.SqlUtil
import org.h2.jdbcx.JdbcConnectionPool
import org.jboss.netty.buffer.ChannelBuffer
import java.io._
import java.net.URI
import java.sql._
import collection.mutable

class DescriptionCacheAdapter(wrappedAdapter: Adapter) extends Adapter {

  protected var _rootPath: String = null
  private var _cp: JdbcConnectionPool = null
  @volatile private var _description: mutable.HashSet[String] with mutable.SynchronizedSet[String] = null
  protected var _dbDir: String = null
  protected var _dataDir: String = null

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

    wrappedAdapter.init(configDir, tier, adapterType, tags, config)
  }

  private def getDbFileName(dbPath: String): String = "%s%sindex".format(dbPath, File.separator)

  private def createConnectionString(dbPath: String): String = "jdbc:h2:%s".format(getDbFileName(dbPath))

  private def getDbConnection: Connection = _cp.getConnection

  private def getReadOnlyDbConnection: Connection = {
    val conn: Connection = getDbConnection
    conn.setReadOnly(true)
    conn
  }

  protected def bootstrap(dataPath: String, dbPath: String) {
    Class.forName("org.h2.Driver")
    _cp = JdbcConnectionPool.create(createConnectionString(dbPath), "sa", "sa")
    val file: File = new File(getDbFileName(dbPath) + ".h2.db")
    if (!file.exists) {
      bootstrapDb
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

  def refreshCache {
    wrappedAdapter.refreshCache

    val foundHashes = wrappedAdapter.describe
    val description = getDescription.toSet
    val newHashes = foundHashes -- description
    addToDb(newHashes)
    val deletedHashes = description -- foundHashes
    deleteFromDb(deletedHashes)
  }

  def contains(hash: String): Boolean = getDescription.contains(hash)

  def verify(hash: String): Boolean = wrappedAdapter.verify(hash)

  def store(is: InputStream, hash: String) {
    wrappedAdapter.store(is, hash)
    insertHash(hash)
  }

  def load(hash: String): InputStream = wrappedAdapter.load(hash)

  def loadChannel(hash: String): ChannelBuffer = wrappedAdapter.loadChannel(hash)

  def remove(hash: String): Boolean = {
    val result = wrappedAdapter.remove(hash)
    deleteFromDb(Set(hash))
    result
  }

  def describe: Set[String] = getDescription.toSet

  def shutdown {
    if (_cp != null) {
      _cp.dispose
    }
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
      case e: SQLException => e.printStackTrace
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
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
          val resultSet = statement.executeQuery
          while (resultSet.next) {
            description.add(resultSet.getString("HASH"))
          }
          _description = description
        }
        catch {
          case e: SQLException => e.printStackTrace
        }
        finally {
          SqlUtil.SafeClose(statement)
          SqlUtil.SafeClose(db)
        }
      }
    }
    _description
  }
}


