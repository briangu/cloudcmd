package cloudcmd.common.adapters

import cloudcmd.common._
import org.h2.jdbcx.JdbcConnectionPool
import java.io._
import java.net.URI
import java.sql._
import collection.mutable
import org.apache.log4j.Logger
import org.json.{JSONArray, JSONException, JSONObject}
import org.h2.fulltext.{FullText, FullTextLucene}
import scala.collection.mutable.ListBuffer
import cloudcmd.common.util.{CryptoUtil, JsonUtil}
import scala.Array
import java.sql.Array

class IndexFilterAdapter(underlying: DirectAdapter) extends IndexedAdapter {

  private val log = Logger.getLogger(classOf[IndexFilterAdapter])

  override def IsOnLine: Boolean = underlying.IsOnLine

  private val BATCH_SIZE = 1024
  private val WHITESPACE = " ,:-._$/\\"// + File.separator
  private val MAX_FETCH_RETRIES = 3

  protected var _rootPath: String = null
  private var _cp: JdbcConnectionPool = null
  private var _description: mutable.HashSet[BlockContext] with mutable.SynchronizedSet[BlockContext] = null
  private var _descriptionHashes: mutable.HashSet[String] with mutable.SynchronizedSet[String] = null
  protected var _dbDir: String = null

  private def getDbFileName(dbPath: String): String = "%s%sindex".format(dbPath, File.separator)
  private def createConnectionString(dbPath: String): String = "jdbc:h2:%s".format(getDbFileName(dbPath))
  private def getDbConnection: Connection = _cp.getConnection

  override def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], config: URI) {
    super.init(configDir, tier, adapterType, tags, config)

    _rootPath = URI.getPath
    if (_rootPath.length == 0) _rootPath = ConfigDir
    _dbDir = _rootPath + File.separator + "db"
    _configDir = _dbDir
    new File(_dbDir).mkdirs

    underlying.init(configDir, tier, adapterType, tags, config)

    if (IsOnLine) bootstrap(_configDir, _dbDir)
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

  protected def bootstrap(dataPath: String, dbPath: String) {
    Class.forName("org.h2.Driver")
    Class.forName("org.h2.fulltext.FullTextLucene")
    _cp = JdbcConnectionPool.create(createConnectionString(dbPath), "sa", "sa")
  }

  private def bootstrapDb() {
    var db: Connection = null
    var st: Statement = null
    try {
      db = getDbConnection
      st = db.createStatement
      st.execute("DROP TABLE if exists BLOCK_INDEX")
      st.execute("CREATE TABLE BLOCK_INDEX ( HASH VARCHAR, TAGS VARCHAR, PRIMARY KEY(HASH, TAGS) )")
      st.execute("CREATE INDEX IDX_BI ON BLOCK_INDEX (HASH, TAGS)")
      st.execute("DROP TABLE if exists FILE_INDEX")
      st.execute("CREATE TABLE FILE_INDEX ( HASH VARCHAR, PATH VARCHAR, FILENAME VARCHAR, FILEEXT VARCHAR, FILESIZE BIGINT, FILEDATE BIGINT, CREATEDDATE BIGINT, TAGS VARCHAR, PROPERTIES__OWNERID BIGINT, RAWMETA VARCHAR, PRIMARY KEY (HASH, TAGS))")
      db.commit()

      createLuceneIndex(db)
    }
    catch {
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(st)
      SqlUtil.SafeClose(db)
    }
  }

  private def createLuceneIndex(db: Connection) {
    FullTextLucene.init(db)
    FullText.setWhitespaceChars(db, WHITESPACE)
    FullTextLucene.createIndex(db, "PUBLIC", "FILE_INDEX", "PATH,TAGS")
  }

/*
  def purge() {
    var db: Connection = null
    try {
      db = getDbConnection
      FullText.dropIndex(db, "PUBLIC", "FILE_INDEX")
      FullTextLucene.dropAll(db)
      FullText.closeAll()
    }
    catch {
      case e: SQLException => ;
    }
    finally {
      SqlUtil.SafeClose(db)
    }

    shutdown()

    Class.forName("org.h2.fulltext.FullTextLucene")

    var file = new File(getDbFile + ".h2.db")
    if (file.exists) FileUtil.delete(file)

    file = new File(getDbFile)
    if (file.exists) FileUtil.delete(file)

    _cp = JdbcConnectionPool.create(createConnectionString, "sa", "sa")

    bootstrapDb()
  }

  private val fields = List("HASH", "PATH", "FILENAME", "FILEEXT", "FILESIZE", "FILEDATE", "CREATEDDATE", "TAGS", "PROPERTIES__OWNERID", "RAWMETA")
  private val addMetaSql = "MERGE INTO FILE_INDEX (%s) VALUES (%s)".format(fields.mkString(","), StringUtil.joinRepeat(fields.size, "?", ","))

  private def addMeta(db: Connection, fmds: Seq[FileMetaData]) {
    var statement: PreparedStatement = null
    try {
      val bind = new ListBuffer[AnyRef]
      statement = db.prepareStatement(addMetaSql)

      var k = 0

      for (meta <- fmds) {
        bind.clear()
        bind.append(meta.getHash)
        bind.append(meta.getPath)
        bind.append(meta.getFilename)
        bind.append(meta.getFileExt)
        bind.append(meta.getFileSize.asInstanceOf[AnyRef])
        bind.append(meta.getFileDate.asInstanceOf[AnyRef])
        bind.append(meta.getCreatedDate.asInstanceOf[AnyRef])
        bind.append(buildTags(meta))
        // TODO: TOTAL HACK...USE STORED.IO ASAP
        bind.append(if (meta.hasProperty("ownerId")) meta.getProperties.getLong("ownerId").asInstanceOf[AnyRef] else 0.asInstanceOf[AnyRef])
        bind.append(meta.getDataAsString)
        (0 until bind.size).foreach(i => bindVar(statement, i + 1, bind(i)))
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

  private def buildTags(meta: FileMetaData): String = {
    var tagSet = meta.getTags
    if (meta.getType != null) tagSet = tagSet ++ meta.getType.split("/")
    tagSet.mkString(" ")
  }

  private def bindVar(statement: PreparedStatement, idx: Int, obj: AnyRef) {
    if (obj.isInstanceOf[String]) {
      statement.setString(idx, obj.asInstanceOf[String])
    }
    else if (obj.isInstanceOf[Long]) {
      statement.setLong(idx, obj.asInstanceOf[Long])
    }
    else if (obj.isInstanceOf[java.lang.Integer]) {
      statement.setInt(idx, obj.asInstanceOf[java.lang.Integer])
    }
    else if (obj == null) {
      statement.setString(idx, null)
    }
    else {
      throw new IllegalArgumentException("unknown obj type: " + obj.toString)
    }
  }

  def add(meta: FileMetaData) {
    var db: Connection = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      addMeta(db, List(meta))
      db.commit()
    }
    catch {
      case e: JSONException => log.error(e)
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(db)
    }
  }

  def remove(meta: FileMetaData) {
    removeAll(Set(meta.getHash))
  }

  def pruneHistory(selections: Seq[FileMetaData]) {
    //    removeAll(selections.filter(_.getParent != null).map(_.getParent).toSet)
    removeAll(Set() ++ selections.flatMap(fmd => if (fmd.getParent == null) {
      Nil
    } else {
      Set(fmd.getParent)
    }))
  }

  def removeAll(hashes: Set[String]) {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      statement = db.prepareStatement("DELETE FROM FILE_INDEX WHERE HASH = ?")

      var k = 0
      hashes.foreach {
        hash =>
          bindVar(statement, 1, hash)
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
      case e: JSONException => log.error(e)
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  def addAll(meta: Seq[FileMetaData]) {
    if (meta == null) return
    var db: Connection = null
    try {
      db = getDbConnection
      FullText.dropIndex(db, "PUBLIC", "FILE_INDEX")
      FullTextLucene.dropAll(db)
      FullText.closeAll()

      db.setAutoCommit(false)
      addMeta(db, meta)
      db.commit()

      Class.forName("org.h2.fulltext.FullTextLucene")
      createLuceneIndex(db)
    }
    catch {
      case e: JSONException => log.error(e)
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(db)
    }
  }

  def reindex() {
    purge()

    val fmds = cas.describe().filter(_.isMeta()).par.flatMap {
      ctx =>
        try {
          List(FileMetaData.create(ctx.hash, JsonUtil.loadJson(cas.load(ctx)._1)))
        } catch {
          case e: Exception => {
            log.error(ctx, e)
            Nil
          }
        }
    }.toList

    addAll(fmds)
    //    pruneHistory(fmds)
  }

  def get(selections: Seq[FileMetaData]) {
    selections.par.foreach(fetch)
  }

  // TODO: only read the file size bytes back (if the file is one block)
  // TODO: support writing to an offset of the existing file to allow for sub-blocks
  def fetch(fmd: FileMetaData) {
    if (fmd.getBlockHashes.size == 0) throw new IllegalArgumentException("no block hashes found!")
    if (fmd.getBlockHashes.find(h => !cas.contains(fmd.createBlockContext(h))) == None) {
      if (fmd.getBlockHashes.size == 1) {
        attemptSingleBlockFetch(fmd.getBlockHashes(0), fmd)
      } else {
        throw new RuntimeException("multiple block hashes not yet supported!")
      }
    } else {
      onMessage(String.format("some blocks of %s not currently available!", fmd.getBlockHashes.mkString(",")))
    }
  }

  def attemptSingleBlockFetch(blockHash: String, fmd: FileMetaData) : Boolean = {
    var success = false
    var retries = MAX_FETCH_RETRIES

    while (!success && retries > 0) {
      var remoteData: InputStream = null
      try {
        remoteData = cas.load(fmd.createBlockContext(blockHash))._1
        val destFile = new File(fmd.getPath)
        destFile.getParentFile.mkdirs
        val remoteDataHash = CryptoUtil.digestToString(CryptoUtil.writeAndComputeHash(remoteData, destFile))
        if (remoteDataHash.equals(blockHash)) {
          onMessage("retrieved: %s".format(fmd.getPath))
          success = true
        } else {
          onMessage("%s is corrupted after download using block %s".format(fmd.getFilename, blockHash))
          destFile.delete
        }
      } catch {
        case e: Exception => {
          onMessage("%s failed to read block %s".format(fmd.getFilename, blockHash))
          log.error(blockHash, e)
        }
      } finally {
        FileUtil.SafeClose(remoteData)
      }

      if (!success) {
        retries -= 1
        cas.ensure(fmd.createBlockContext(blockHash), blockLevelCheck = true)
        if (!cas.contains(fmd.createBlockContext(blockHash))) {
          onMessage("giving up on %s, block %s not currently available!".format(fmd.getFilename, blockHash))
          retries = 0
        }
      }
    }
    success
  }

  def addTags(selections: Seq[FileMetaData], tags: Set[String]): Seq[FileMetaData] = {
    val fmds = selections.par.flatMap {
      selection =>
        val newTags = FileMetaData.applyTags(selection.getTags, tags)
        if (newTags.equals(selection.getTags)) {
          Nil
        } else {
          val selectionJson = selection.toJson
          val data = selectionJson.getJSONObject("data")
          data.put("tags", new JSONArray(newTags))

          val derivedMeta = FileMetaData.deriveMeta(selection.getHash, data)
          cas.store(derivedMeta.createBlockContext, new ByteArrayInputStream(derivedMeta.getDataAsString.getBytes("UTF-8")))
          List(derivedMeta)
        }
    }.toList

    addAll(fmds)
    //    pruneHistory(fmds)

    fmds
  }

  def ensure(selections: Seq[FileMetaData], blockLevelCheck: Boolean) {
    selections.par.foreach{
      fmd =>
        val hashes = Set(fmd.getHash) ++ fmd.getBlockHashes
        hashes.foreach{ hash =>
          if (!cas.ensure(fmd.createBlockContext(hash), blockLevelCheck)) {
            onMessage("%s: found incosistent block %s".format(fmd.getFilename, hash))
          }
        }
    }
  }

  // TODO: what about the meta.Parent chain? do we want to wipe out the entire chain?
  def remove(selections: Seq[FileMetaData]) {
    selections.par.foreach {
      fmd =>
        cas.remove(fmd.createBlockContext)

        // TODO: only delete if there are no other files referencing these blocks
        if (false) {
          fmd.getBlockHashes.foreach(blockHash => cas.remove(fmd.createBlockContext(blockHash)))
        }

        // TODO: we should only do this if we are sure the rest happened correctly (although at worst we could reindex)
        remove(fmd)
    }
  }

  def find(filter: JSONObject): Seq[FileMetaData] = {
    val results = new ListBuffer[FileMetaData]

    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection

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
      (0 until bind.size).foreach(i => bindVar(statement, i + 1, bind(i)))

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
    results.toList
  }
*/
  def reindex() {
    val foundContexts = underlying.describe()
    val cachedContexts = getDescription.toSet
    val newContexts = foundContexts -- cachedContexts
    addToDb(newContexts)
    val deletedContexts = cachedContexts -- foundContexts
    deleteFromDb(deletedContexts)
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
  }

  /**
   * Find a set of meta blocks based on a filter.
   * @param filter
   * @return a set of meta blocks
   */
  def find(filter: JSONObject): Set[FileMetaData] = {
    Set()
  }

  override def contains(ctx: BlockContext) : Boolean = {
    if (_description == null) getDescription
    _descriptionHashes.contains(ctx.hash)
  }

  def containsAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    if (_description == null) getDescription
    Map() ++ ctxs.par.flatMap(ctx => Map(ctx -> _descriptionHashes.contains(ctx.hash)))
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
    underlying.store(ctx, is)
    addToDb(Set(ctx))
  }

  def load(ctx: BlockContext): (InputStream, Int) = {
    underlying.load(ctx)
  }

  def removeAll(ctxs : Set[BlockContext]) : Map[BlockContext, Boolean] = {
    val result = underlying.removeAll(ctxs)
    val wasRemoved = Set() ++ result.par.flatMap{ case (ctx, removed) =>  if (removed) Set(ctx) else Nil }
    deleteFromDb(wasRemoved)
    result
  }

  def describe(): Set[String] = {
    underlying.describe()
  }

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
        statement.addBatch()

        if (_description != null) {
          _description.add(ctx)
          _descriptionHashes.add(ctx.hash)
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

  private def getDescription: mutable.HashSet[BlockContext] with mutable.SynchronizedSet[BlockContext] = {
    if (_description != null) return _description

    this synchronized {
      if (_description == null) {
        var db: Connection = null
        var statement: PreparedStatement = null
        try {
          db = getDbConnection
          statement = db.prepareStatement("SELECT HASH,TAGS FROM BLOCK_INDEX")

          val description = new mutable.HashSet[BlockContext] with mutable.SynchronizedSet[BlockContext]
          val descriptionHashes = new mutable.HashSet[String] with mutable.SynchronizedSet[String]

          val resultSet = statement.executeQuery
          while (resultSet.next) {
            val hash = resultSet.getString("HASH")
            val tags = resultSet.getString("TAGS").split(" ").filter(_.length > 0).toSet
            description.add(new BlockContext(hash, tags))
            descriptionHashes.add(hash)
          }

          _description = description
          _descriptionHashes = descriptionHashes
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


