package cloudcmd.common.adapters

import cloudcmd.common.{BlockContext, SqlUtil}
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
  @volatile private var _description: mutable.HashSet[BlockContext] with mutable.SynchronizedSet[BlockContext] = null
  protected var _dbDir: String = null
  protected var _dataDir: String = null

  private def getDbFileName(dbPath: String): String = "%s%sindex".format(dbPath, File.separator)
  private def createConnectionString(dbPath: String): String = "jdbc:h2:%s".format(getDbFileName(dbPath))
  private def getDbConnection: Connection = _cp.getConnection
  private def getReadOnlyDbConnection: Connection = {
    val conn: Connection = getDbConnection
    conn.setReadOnly(true)
    conn
  }

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

  def shutdown {
    if (_cp != null) {
      _cp.dispose
      _cp = null
    }
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
      st.execute("CREATE TABLE BLOCK_INDEX ( HASH VARCHAR, TAGS VARCHAR, PRIMARY KEY(HASH, TAGS) )")
      st.execute("CREATE INDEX IDX_BI ON BLOCK_INDEX (HASH, TAGS)")
      db.commit
    }
    catch {
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(st)
      SqlUtil.SafeClose(db)
    }
  }

  def refreshCache {
    wrappedAdapter.refreshCache

    val foundContexts = wrappedAdapter.describe
    val cachedContexts = getDescription.toSet
    val newContexts = foundContexts -- cachedContexts
    addToDb(newContexts)
    val deletedContexts = cachedContexts -- foundContexts
    deleteFromDb(deletedContexts)
  }

  override def contains(ctx: BlockContext) : Boolean = getDescription.contains(ctx)

  def containsAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    val present = getDescription.intersect(ctxs)
    val missing = ctxs -- present
    // TODO: which way is faster? hashes.flatmap or this
    Map() ++ present.par.flatMap(h => Map(h -> true)) ++ missing.par.flatMap(h => Map(h -> false))
  }

  override
  def ensure(ctx: BlockContext, blockLevelCheck: Boolean): Boolean = wrappedAdapter.ensure(ctx, blockLevelCheck)

  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ ctx =>
      Map(ctx -> wrappedAdapter.ensure(ctx, blockLevelCheck))
    }
  }

  def store(ctx: BlockContext, is: InputStream) = {
    wrappedAdapter.store(ctx, is)
    addToDb(Set(ctx))
  }

  def load(ctx: BlockContext): InputStream = wrappedAdapter.load(ctx)

  def removeAll(ctxs : Set[BlockContext]) : Map[BlockContext, Boolean] = {
    val result = wrappedAdapter.removeAll(ctxs)
    val removed = Set() ++ result.par.flatMap{ case (ctx, removed) =>  if (removed) Set(ctx) else Nil }
    deleteFromDb(removed)
    result
  }

  def describe: Set[BlockContext] = getDescription.toSet

  def describeHashes: Set[String] = wrappedAdapter.describeHashes

  private def addToDb(ctxs: Set[BlockContext]) {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      statement = db.prepareStatement("MERGE INTO BLOCK_INDEX VALUES (?,?)")

      var k = 0
      for (ctx <- ctxs) {
        statement.setString(1, ctx.hash)
        statement.setString(2, ctx.routingTags.mkString(" "))
        statement.addBatch

        k += 1
        if (k > BATCH_SIZE) {
          statement.executeBatch
          k = 0
        }
      }

      statement.executeBatch
      db.commit
    }
    catch {
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  private def deleteFromDb(ctxs: Set[BlockContext]) {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      statement = db.prepareStatement("DELETE FROM BLOCK_INDEX WHERE HASH = ?")
      var k = 0
      for (ctx <- ctxs) {
        statement.setString(1, ctx.hash)
        statement.addBatch
        k += 1
        if (k > BATCH_SIZE) {
          statement.executeBatch
          k = 0
        }
      }
      statement.executeBatch
      db.commit
    }
    catch {
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  private def getDescription: mutable.HashSet[BlockContext] with mutable.SynchronizedSet[BlockContext] = {
    if (_description != null) return _description

    this synchronized {
      if (_description == null) {
        var db: Connection = null
        var statement: PreparedStatement = null
        try {
          db = getReadOnlyDbConnection
          statement = db.prepareStatement("SELECT HASH,TAGS FROM BLOCK_INDEX")

          val description = new mutable.HashSet[BlockContext] with mutable.SynchronizedSet[BlockContext]
          val resultSet = statement.executeQuery
          while (resultSet.next) {
            description.add(new BlockContext(resultSet.getString("HASH"), resultSet.getString("TAGS").split(" ").toSet))
          }

          println(_rootPath)
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


