package cloudcmd.common.adapters

import cloudcmd.common._
import org.h2.jdbcx.JdbcConnectionPool
import java.io._
import java.net.URI
import collection.mutable
import org.apache.log4j.Logger
import org.json.{JSONArray, JSONException, JSONObject}
import org.h2.fulltext.{FullText, FullTextLucene}
import scala.collection.mutable.ListBuffer
import cloudcmd.common.util.{SqlUtil, StreamUtil, JsonUtil}
import java.sql.{PreparedStatement, Statement, SQLException, Connection}

class IndexFilterAdapter(underlying: DirectAdapter) extends IndexedAdapter {

  private val log = Logger.getLogger(classOf[IndexFilterAdapter])

  override def IsOnLine: Boolean = underlying.IsOnLine

  private val BATCH_SIZE = 1024
  private val WHITESPACE = " ,:-._$/\\"// + File.separator

  protected var _rootPath: String = null
  private var _cp: JdbcConnectionPool = null
  private val _fmdCache = new mutable.HashMap[BlockContext, String] with mutable.SynchronizedMap[BlockContext, String]
  private var _description: mutable.HashSet[String] with mutable.SynchronizedSet[String] = null
  protected var _dbDir: String = null

  private def _getDbFile = "%s%sindex".format(_dbDir, File.separator)
  private def _createConnectionString: String = "jdbc:h2:%s".format(_getDbFile)
  private def _getDbConnection = _cp.getConnection

  override def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], config: URI) {
    super.init(configDir, tier, adapterType, tags, config)

    _rootPath = URI.getPath
    if (_rootPath.length == 0) _rootPath = ConfigDir
    _dbDir = _rootPath + File.separator + "indexdb"
    _configDir = _dbDir
    new File(_dbDir).mkdirs

    underlying.init(configDir, tier, adapterType, tags, config)

    if (IsOnLine) {
      _bootstrap(_configDir, _dbDir)
    }
  }

  def shutdown() {
    try {
      underlying.shutdown()
    } finally {
      if (_cp != null) {
        _cp.dispose()
        _cp = null
      }
    }
  }

  def reindex(cas: ContentAddressableStorage) {
    //  TODO: in order to take care of local data,
    //        we should load from all available adapters, not just this one
    val underlyingMeta = underlying.describe().filter(_.endsWith(".meta"))
    val cachedMeta = describe().filter(_.endsWith(".meta"))
    val newMeta = underlyingMeta.diff(cachedMeta)
    val deletedMeta = cachedMeta -- underlyingMeta

    if (newMeta.size > 0) {
      val collections = newMeta.grouped(1024)
      collections.foreach{
        group =>
          val addedFileMetaData = group.par.flatMap {
            hash =>
              try {
                val ctx = new BlockContext(hash)
                val is = if (cas.contains(ctx)) {
                  cas.load(ctx)._1
                } else {
                  underlying.load(ctx)._1
                }
                List(FileMetaData.create(hash, JsonUtil.loadJson(is)))
              } catch {
                case e: Exception => {
                  log.error(hash, e)
                  Nil
                }
              }
          }.toList

          System.err.println("indexing %d files".format(addedFileMetaData.size))
          _addAllFileMetaData(addedFileMetaData, rebuildIndex = true)
      }
    }

    if (deletedMeta.size > 0) {
      _deleteBlockContextsFromDb(_getMetaHashesAsBlockContexts(deletedMeta))
    }
  }

//  def describe(): Set[BlockContext] = {
//    val hashes = describeHashes()
//
//    val extraHashes = new mutable.HashSet[String] with mutable.SynchronizedSet[String]
//    val referencedBlockHashes = new mutable.HashSet[String] with mutable.SynchronizedSet[String]
//
//    val ctxs = new mutable.HashSet[BlockContext] with mutable.SynchronizedSet[BlockContext]
//    hashes.par.foreach{ hash =>
//      if (hash.endsWith(".meta")) {
//        val fis = _s3Service.getObject(_bucketName, hash).getDataInputStream
//        try {
//          val fmd = FileMetaData.create(hash, JsonUtil.loadJson(fis))
//          ctxs.add(fmd.createBlockContext)
//          fmd.getBlockHashes.foreach{ blockHash =>
//            if (hashes.contains(blockHash)) {
//              ctxs.add(fmd.createBlockContext(blockHash))
//              referencedBlockHashes.add(blockHash)
//            } else {
//              // TODO: log as we should have the blockHash in the description on the same adapter
//              println("missing blockhash %s (%s) on adapter: %s".format(blockHash, fmd.getPath, this.URI))
//            }
//          }
//        } finally {
//          FileUtil.SafeClose(fis)
//        }
//      } else {
//        extraHashes.add(hash)
//      }
//    }
//
//    if (extraHashes.size > 0) {
//      val unreferencedHashes = extraHashes.diff(referencedBlockHashes)
//      unreferencedHashes.foreach { hash =>
//        ctxs.add(FileMetaData.createBlockContext(hash, Set[String]()))
//      }
//    }
//
//    ctxs.toSet
//  }

  /***
    * Flush the index cache that may be populated during a series of modifications (e.g. store)
    */
  def flushIndex() {
    val fmds = _fmdCache.map{ case (ctx: BlockContext, meta: String) =>
      FileMetaData.create(ctx.hash, new JSONObject(meta))
    }
    _addAllFileMetaData(fmds.toSeq, rebuildIndex = true)
    _fmdCache.clear()
  }

  /**
   * Find a set of meta blocks based on a filter.
   * @param filter
   * @return a set of meta blocks
   */
  def find(filter: JSONObject): Set[FileMetaData] = {
    val results = new ListBuffer[FileMetaData]

    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = _getDbConnection

      val bind = new ListBuffer[AnyRef]
      var prefix = ""
      var handledOffset = false

      var sql = if (filter.has("tags")) {
        bind.append(filter.getString("tags"))
        prefix = "T."
        handledOffset = true
        val limit = if (filter.has("count")) filter.getInt("count") else Int.MaxValue
        val offset = if (filter.has("offset")) filter.getInt("offset") else 0
        "SELECT T.HASH,T.RAWMETA FROM FTL_SEARCH_DATA(?, %d, %d) FTL, FILE_INDEX T WHERE FTL.TABLE='FILE_INDEX' AND T.HASH = FTL.KEYS[0]".format(limit, offset)
      }
      else {
        "SELECT HASH,RAWMETA FROM FILE_INDEX"
      }

      val list = new ListBuffer[String]

      val iter = filter.keys
      while (iter.hasNext) {
        iter.next.asInstanceOf[String] match {
          case "orderBy" | "count" | "offset" | "tags" => ;
          case rawKey => {
            val key = prefix + rawKey

            val obj = filter.get(rawKey)
            if (obj.isInstanceOf[Array[String]] || obj.isInstanceOf[Array[Long]]) {
              val foo = List(obj)
              list.append(String.format("%s In (%s)", key.toUpperCase, StringUtil.joinRepeat(foo.size, "?", ",")))
              bind.appendAll(foo)
            }
            else if (obj.isInstanceOf[JSONArray]) {
              val foo = obj.asInstanceOf[JSONArray]
              list.append(String.format("%s In (%s)", key.toUpperCase, StringUtil.joinRepeat(foo.length, "?", ",")))
              bind.appendAll(JsonUtil.createSet(foo))
            }
            else {
              if (obj.toString.contains("%")) {
                list.append(String.format("%s LIKE ?", key))
              }
              else {
                list.append(String.format("%s IN (?)", key))
              }
              bind.append(obj)
            }
          }
        }
      }

      if (list.size > 0) {
        sql += (if (sql.contains("WHERE")) " AND" else " WHERE")
        sql += " %s".format(list.mkString(" AND "))
      }

      if (filter.has("orderBy")) {
        val orderBy = filter.getJSONObject("orderBy")
        sql += " ORDER BY %s".format(prefix + orderBy.getString("name"))
        if (orderBy.has("asc")) sql += " ASC"
        if (orderBy.has("desc")) sql += " DESC"
      }

      if (!handledOffset) {
        if (filter.has("count")) sql += " LIMIT %d".format(filter.getInt("count"))
        if (filter.has("offset")) sql += " OFFSET %d".format(filter.getInt("offset"))
      }

      statement = db.prepareStatement(sql)
      (0 until bind.size).foreach(i => SqlUtil.bindVar(statement, i + 1, bind(i)))

      val rs = statement.executeQuery
      while (rs.next) {
        results.append(FileMetaData.create(rs.getString("HASH"), new JSONObject(rs.getString("RAWMETA"))))
      }
    }
    catch {
      case e: JSONException => log.error(e)
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
    results.toSet
  }

  override def contains(ctx: BlockContext) : Boolean = {
    _getDescription.contains(ctx.hash)
  }

  def containsAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    val description = _getDescription
    Map() ++ ctxs.par.flatMap(ctx => Map(ctx -> description.contains(ctx.hash)))
  }

  override def ensure(ctx: BlockContext, blockLevelCheck: Boolean): Boolean = {
    underlying.ensure(ctx, blockLevelCheck)
  }

  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ ctx =>
      Map(ctx -> underlying.ensure(ctx, blockLevelCheck))
    }
  }

  def store(ctx: BlockContext, is: InputStream) {
    if (ctx.isMeta()) {
      val (meta, forwardIs) = if (is.markSupported()) {
        is.mark(0)
        val meta = StreamUtil.spoolStreamToString(is)
        is.reset()
        (meta, is)
      } else {
        val meta = StreamUtil.spoolStreamToString(is)
        (meta, StreamUtil.stringToInputStream(meta))
      }
      try {
        underlying.store(ctx, forwardIs)
        _fmdCache.put(ctx, meta)
      } finally {
        if (is != forwardIs) {
          forwardIs.close()
        }
      }
    } else {
      underlying.store(ctx, is)
    }
  }

  def load(ctx: BlockContext): (InputStream, Int) = {
    if (ctx.isMeta() && _getDescription.contains(ctx.hash)) {
      val rawMeta = _loadRawMetaDataFromDb(ctx)
      val bytes = rawMeta.getBytes("UTF-8")
      (new ByteArrayInputStream(bytes), bytes.length)
    } else {
      underlying.load(ctx)
    }
  }

  def removeAll(ctxs : Set[BlockContext]) : Map[BlockContext, Boolean] = {
    val result = underlying.removeAll(ctxs)
    val wasRemoved = Set() ++ result.par.flatMap{ case (ctx, removed) =>  if (removed) Set(ctx) else Nil }
    _deleteBlockContextsFromDb(wasRemoved)
    result
  }

  def describe(): Set[String] = {
    _getDescription.toSet
  }

  def _addAllFileMetaData(meta: Seq[FileMetaData], rebuildIndex: Boolean = false) {
    if (meta == null) return
    var db: Connection = null
    try {
      db = _getDbConnection

      if (rebuildIndex) {
        FullText.dropIndex(db, "PUBLIC", "FILE_INDEX")
        FullTextLucene.dropAll(db)
        FullText.closeAll()
      }

      db.setAutoCommit(false)
      _addMetaToDb(db, meta)
      db.commit()

      if (rebuildIndex) {
        Class.forName("org.h2.fulltext.FullTextLucene")
        _createLuceneIndex(db)
      }
    }
    catch {
      case e: JSONException => log.error(e)
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(db)
    }
  }

  protected def _bootstrap(dataPath: String, dbPath: String) {
    Class.forName("org.h2.Driver")
    Class.forName("org.h2.fulltext.FullTextLucene")
    _cp = JdbcConnectionPool.create(_createConnectionString, "sa", "sa")
    val file: File = new File(_getDbFile + ".h2.db")
    if (!file.exists) {
      _recreateDb()
    }
  }

  private def _bootstrapDb() {
    var db: Connection = null
    var st: Statement = null
    try {
      db = _getDbConnection
      st = db.createStatement
      st.execute("DROP TABLE if exists FILE_INDEX")
      st.execute("CREATE TABLE FILE_INDEX ( HASH VARCHAR, BLOCK_HASHES VARCHAR, PATH VARCHAR, FILENAME VARCHAR, FILEEXT VARCHAR, FILESIZE BIGINT, FILEDATE BIGINT, CREATEDDATE BIGINT, TAGS VARCHAR, PROPERTIES__OWNERID BIGINT, RAWMETA VARCHAR, PRIMARY KEY (HASH, TAGS))")
      db.commit()

      _createLuceneIndex(db)
    }
    catch {
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(st)
      SqlUtil.SafeClose(db)
    }
  }

  private def _createLuceneIndex(db: Connection) {
    FullTextLucene.init(db)
    FullText.setWhitespaceChars(db, WHITESPACE)
    FullTextLucene.createIndex(db, "PUBLIC", "FILE_INDEX", "PATH,TAGS")
  }

  def _recreateDb() {
    var db: Connection = null
    try {
      db = _getDbConnection
      FullText.dropIndex(db, "PUBLIC", "FILE_INDEX")
      FullTextLucene.dropAll(db)
      FullText.closeAll()
    }
    catch {
      case e: SQLException => {
        log.error(e)
      }
    }
    finally {
      SqlUtil.SafeClose(db)
    }

    shutdown()

    Class.forName("org.h2.fulltext.FullTextLucene")

    var file = new File(_getDbFile + ".h2.db")
    if (file.exists) FileUtil.delete(file)

    file = new File(_getDbFile)
    if (file.exists) FileUtil.delete(file)

    _cp = JdbcConnectionPool.create(_createConnectionString, "sa", "sa")

    _bootstrapDb()
  }

  private val _fields = List("HASH", "BLOCK_HASHES", "PATH", "FILENAME", "FILEEXT", "FILESIZE", "FILEDATE", "CREATEDDATE", "TAGS", "PROPERTIES__OWNERID", "RAWMETA")
  private val _addMetaSql = "MERGE INTO FILE_INDEX (%s) VALUES (%s)".format(_fields.mkString(","), StringUtil.joinRepeat(_fields.size, "?", ","))

  private def _addMetaToDb(db: Connection, fmds: Seq[FileMetaData]) {
    var statement: PreparedStatement = null
    try {
      val bind = new ListBuffer[AnyRef]
      statement = db.prepareStatement(_addMetaSql)

      var k = 0

      for (meta <- fmds) {
        bind.clear()
        bind.append(meta.getHash)
        bind.append(meta.getBlockHashes.mkString(","))
        bind.append(meta.getPath)
        bind.append(meta.getFilename)
        bind.append(meta.getFileExt)
        bind.append(meta.getFileSize.asInstanceOf[AnyRef])
        bind.append(meta.getFileDate.asInstanceOf[AnyRef])
        bind.append(meta.getCreatedDate.asInstanceOf[AnyRef])
        bind.append(_buildTags(meta))
        // TODO: TOTAL HACK...USE STORED.IO ASAP
        bind.append(if (meta.hasProperty("ownerId")) meta.getProperties.getLong("ownerId").asInstanceOf[AnyRef] else 0.asInstanceOf[AnyRef])
        bind.append(meta.getDataAsString)
        (0 until bind.size).foreach(i => SqlUtil.bindVar(statement, i + 1, bind(i)))
        statement.addBatch()

        k += 1
        if (k > BATCH_SIZE) {
          statement.executeBatch
          k = 0
        }
      }
      statement.executeBatch
    }
    catch {
      case e: Exception => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(statement)
    }
  }

  private def _buildTags(meta: FileMetaData): String = {
    var tagSet = meta.getTags
    if (meta.getType != null) tagSet = tagSet ++ meta.getType.split("/")
    tagSet.mkString(" ")
  }

  //  def pruneHistory(selections: Seq[FileMetaData]) {
  //    //    removeAll(selections.filter(_.getParent != null).map(_.getParent).toSet)
  //    removeAll(Set() ++ selections.flatMap(fmd => if (fmd.getParent == null) {
  //      Nil
  //    } else {
  //      Set(fmd.getParent)
  //    }))
  //  }

  private def _deleteBlockContextsFromDb(ctxs: Set[BlockContext]) {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = _getDbConnection
      db.setAutoCommit(false)
      statement = db.prepareStatement("DELETE FROM FILE_INDEX WHERE HASH = ? AND TAGS = ?")
      var k = 0
      for (ctx <- ctxs) {
        statement.setString(1, ctx.hash)
        statement.setString(2, ctx.routingTags.mkString(" "))
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

  private def _getMetaHashesAsBlockContexts(hashes: Set[String]): Set[BlockContext] = {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = _getDbConnection
      statement = db.prepareStatement("SELECT HASH,TAGS FROM FILE_INDEX WHERE HASH in ?")
      SqlUtil.bindVar(statement, 1, hashes.mkString(","))

      val blockContexts = new mutable.HashSet[BlockContext]

      val resultSet = statement.executeQuery
      while (resultSet.next) {
        blockContexts.add(new BlockContext(resultSet.getString("HASH"), resultSet.getString("TAGS").split(" ").toSet))
      }

      blockContexts.toSet
    }
    catch {
      case e: SQLException => {
        log.error(e)
        Set()
      }
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  private def _loadRawMetaDataFromDb(ctx: BlockContext): String = {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = _getDbConnection
      statement = db.prepareStatement("SELECT RAWMETA FROM FILE_INDEX WHERE HASH = ?")
      SqlUtil.bindVar(statement, 1, ctx.hash)

      val resultSet = statement.executeQuery
      if (resultSet.next) {
        resultSet.getString("RAWMETA")
      } else {
        null
      }
    } catch {
      case e: SQLException => {
        log.error(e)
        null
      }
    } finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  private def _getDescription: mutable.HashSet[String] with mutable.SynchronizedSet[String] = {
    if (_description == null) {
      this synchronized {
        if (_description == null) {
          var db: Connection = null
          var statement: PreparedStatement = null
          try {
            db = _getDbConnection
            statement = db.prepareStatement("SELECT HASH, BLOCK_HASHES FROM FILE_INDEX")

            val description = new mutable.HashSet[String] with mutable.SynchronizedSet[String]

            val resultSet = statement.executeQuery
            while (resultSet.next) {
              description.add(resultSet.getString("HASH"))
              resultSet.getString("BLOCK_HASHES").split(",").foreach(description.add)
            }

            _description = description
          } catch {
            case e: SQLException => {
              log.error(e)
            }
          } finally {
            SqlUtil.SafeClose(statement)
            SqlUtil.SafeClose(db)
          }
        }
      }
    }

    _description
  }
}


