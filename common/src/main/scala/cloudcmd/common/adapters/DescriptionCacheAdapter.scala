package cloudcmd.common.adapters

import cloudcmd.common.SqlUtil
import org.h2.jdbcx.JdbcConnectionPool
import java.io._
import java.net.URI
import java.sql._
import collection.mutable
import org.apache.log4j.Logger

class DescriptionCacheAdapter(wrappedAdapter: Adapter) extends Adapter {

  private val log = Logger.getLogger(classOf[DescriptionCacheAdapter])

  private val BATCH_SIZE = 1024
  
  protected var _rootPath: String = null
  private var _cp: JdbcConnectionPool = null
  private var _description: mutable.HashSet[String] with mutable.SynchronizedSet[String] = null
  protected var _dbDir: String = null
  protected var _dataDir: String = null

  private def getDbFileName(dbPath: String): String = "%s%sindex".format(dbPath, File.separator)
  private def createConnectionString(dbPath: String): String = "jdbc:h2:%s".format(getDbFileName(dbPath))
  private def getDbConnection: Connection = _cp.getConnection

  override def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], config: URI) {
    super.init(configDir, tier, adapterType, tags, config)

    _rootPath = URI.getPath
    if (_rootPath.length == 0) _rootPath = ConfigDir
    _dbDir = _rootPath + File.separator + "db"
    ConfigDir = _dbDir
    _dataDir = _rootPath + File.separator + "data"
    val rootPathDir: File = new File(_rootPath)
    rootPathDir.mkdirs
    _isOnline = rootPathDir.exists
    if (_isOnline) bootstrap(_dataDir, _dbDir)

    wrappedAdapter.init(configDir, tier, adapterType, tags, config)
  }

  def shutdown() {
    try {
      wrappedAdapter.shutdown()
    } finally {
      if (_cp != null) {
        _cp.dispose()
        _cp = null
      }
    }
  }

  protected def bootstrap(dataPath: String, dbPath: String) {
    Class.forName("org.h2.Driver")
    _cp = JdbcConnectionPool.create(createConnectionString(dbPath), "sa", "sa")
    val file: File = new File(getDbFileName(dbPath) + ".h2.db")
    if (!file.exists) {
      bootstrapDb()
    }
  }

  private def bootstrapDb() {
    var db: Connection = null
    var st: Statement = null
    try {
      db = getDbConnection
      st = db.createStatement
      st.execute("DROP TABLE if exists BLOCK_INDEX")
      st.execute("CREATE TABLE BLOCK_INDEX ( HASH VARCHAR PRIMARY KEY )")
      db.commit()
    }
    catch {
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(st)
      SqlUtil.SafeClose(db)
    }
  }

  def refreshCache() {
    wrappedAdapter.refreshCache()

    val foundContexts = wrappedAdapter.describe()
    val cachedContexts = getDescription.toSet
    val newContexts = foundContexts -- cachedContexts
    addToDb(newContexts)
    val deletedContexts = cachedContexts -- foundContexts
    deleteFromDb(deletedContexts)
  }

  override def contains(hash: String) : Boolean = {
    getDescription.contains(hash)
  }

  def containsAll(hashes: Set[String]) : Map[String, Boolean] = {
    val present = getDescription.intersect(hashes)
    val missing = hashes -- present
    // TODO: which way is faster? hashes.flatmap or this
    Map() ++ present.par.flatMap(h => Map(h -> true)) ++ missing.par.flatMap(h => Map(h -> false))
  }

  override def ensure(hash: String, blockLevelCheck: Boolean): Boolean = wrappedAdapter.ensure(hash, blockLevelCheck)

  def ensureAll(hashes: Set[String], blockLevelCheck: Boolean): Map[String, Boolean] = {
    Map() ++ hashes.par.flatMap{ hash =>
      Map(hash -> wrappedAdapter.ensure(hash, blockLevelCheck))
    }
  }

  def store(hash: String, is: InputStream) {
    wrappedAdapter.store(hash, is)
    addToDb(Set(hash))
  }

  def load(hash: String): (InputStream, Int) = wrappedAdapter.load(hash)

  def removeAll(hashes : Set[String]) : Map[String, Boolean] = {
    val result = wrappedAdapter.removeAll(hashes)
    val wasRemoved = Set() ++ result.par.flatMap{ case (hash, removed) =>  if (removed) Set(hash) else Nil }
    deleteFromDb(wasRemoved)
    result
  }

  def describe(): Set[String] = getDescription.toSet

  private def addToDb(hashes: Set[String]) {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      statement = db.prepareStatement("MERGE INTO BLOCK_INDEX VALUES (?)")

      var k = 0
      for (hash <- hashes) {
        statement.setString(1, hash)
        statement.addBatch()

        if (_description != null) {
          _description.add(hash)
        }

        k += 1
        if (k > BATCH_SIZE) {
          statement.executeBatch
          k = 0
        }
      }

      statement.executeBatch
      db.commit()
    }
    catch {
      case e: SQLException => log.error(e)
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
        statement.addBatch()
        k += 1
        if (k > BATCH_SIZE) {
          statement.executeBatch
          k = 0
        }
      }
      statement.executeBatch
      db.commit()
    }
    catch {
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  private def getDescription: mutable.HashSet[String] with mutable.SynchronizedSet[String] = {
    if (_description != null) return _description

    this synchronized {
      if (_description == null) {
        var db: Connection = null
        var statement: PreparedStatement = null
        try {
          db = getDbConnection
          statement = db.prepareStatement("SELECT HASH FROM BLOCK_INDEX")

          val description = new mutable.HashSet[String] with mutable.SynchronizedSet[String]

          val resultSet = statement.executeQuery
          while (resultSet.next) {
            val hash = resultSet.getString("HASH")
            description.add(hash)
          }

          _description = description
        }
        catch {
          case e: SQLException => log.error(e)
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


