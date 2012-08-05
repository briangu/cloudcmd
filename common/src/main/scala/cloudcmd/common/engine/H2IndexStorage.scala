package cloudcmd.common.engine

import cloudcmd.common._
import org.apache.log4j.Logger
import org.h2.fulltext.{FullText, FullTextLucene}
import org.h2.jdbcx.JdbcConnectionPool
import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import java.io.{ByteArrayInputStream, InputStream, File}
import java.sql.{PreparedStatement, SQLException, Statement, Connection}
import collection.mutable.ListBuffer
import util._

class H2IndexStorage(cloudEngine: CloudEngine) extends IndexStorage with EventSource {
  private val log = Logger.getLogger(classOf[H2IndexStorage])

  private val BATCH_SIZE = 1024
  private val WHITESPACE = " ,:-._" + File.separator
  private val MAX_FETCH_RETRIES = 3

  private var _configRoot: String = null

  private var _cp: JdbcConnectionPool = null

  private def getDbFile = "%s%sindex".format(_configRoot, File.separator)

  private def createConnectionString: String = "jdbc:h2:%s".format(getDbFile)

  private def getDbConnection = _cp.getConnection

  private def getReadOnlyDbConnection: Connection = {
    val conn = getDbConnection
    conn.setReadOnly(true)
    conn
  }

  def init(configRoot: String) {
    _configRoot = configRoot

    Class.forName("org.h2.Driver")
    Class.forName("org.h2.fulltext.FullTextLucene")
    _cp = JdbcConnectionPool.create(createConnectionString, "sa", "sa")
    val file: File = new File(getDbFile + ".h2.db")
    if (!file.exists) {
      purge
      bootstrapDb
    }
  }

  def shutdown {
    flush
    if (_cp != null) {
      _cp.dispose
      _cp = null
    }
  }

  private def bootstrapDb {
    var db: Connection = null
    var st: Statement = null
    try {
      db = getDbConnection
      st = db.createStatement
      st.execute("DROP TABLE if exists FILE_INDEX")
      st.execute("CREATE TABLE FILE_INDEX ( HASH VARCHAR PRIMARY KEY, PATH VARCHAR, FILENAME VARCHAR, FILEEXT VARCHAR, FILESIZE BIGINT, FILEDATE BIGINT, TAGS VARCHAR, RAWMETA VARCHAR )")
      db.commit

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
    FullTextLucene.createIndex(db, "PUBLIC", "FILE_INDEX", "TAGS")
  }

  def purge {
    var db: Connection = null
    try {
      db = getDbConnection
      FullText.dropIndex(db, "PUBLIC", "FILE_INDEX")
      FullTextLucene.dropAll(db)
      FullText.closeAll
    }
    catch {
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(db)
    }

    shutdown

    Class.forName("org.h2.fulltext.FullTextLucene")

    var file = new File(getDbFile + ".h2.db")
    if (file.exists) FileUtil.delete(file)

    file = new File(getDbFile)
    if (file.exists) FileUtil.delete(file)

    _cp = JdbcConnectionPool.create(createConnectionString, "sa", "sa")

    bootstrapDb
  }

  def flush {}

  private val fields = List("HASH", "PATH", "FILENAME", "FILEEXT", "FILESIZE", "FILEDATE", "TAGS", "RAWMETA")
  private val addMetaSql = "INSERT INTO FILE_INDEX (%s) VALUES (%s)".format(fields.mkString(","), StringUtil.joinRepeat(fields.size, "?", ","))

  private def addMeta(db: Connection, fmds: List[FileMetaData]) {
    var statement: PreparedStatement = null
    try {
      val bind = new ListBuffer[AnyRef]
      statement = db.prepareStatement(addMetaSql)

      var k = 0

      for (meta <- fmds) {
        bind.clear
        bind.append(meta.getHash)
        bind.append(meta.getPath)
        bind.append(meta.getFilename)
        bind.append(meta.getFileExt)
        bind.append(meta.getFileSize.asInstanceOf[AnyRef])
        bind.append(meta.getFileDate.asInstanceOf[AnyRef])
        bind.append(buildTags(meta))
        bind.append(meta.getDataAsString)
        (0 until bind.size).foreach(i => bindVar(statement, i + 1, bind(i)))
        statement.addBatch

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
    val tagSet = meta.getTags ++ Set(meta.getPath) ++ Set(FileTypeUtil.instance.getTypeFromName(meta.getFilename))
    var tags = tagSet.mkString(" ")
    WHITESPACE.toCharArray.foreach { ch => tags = tags.replace(ch, ' ') }
    tags
  }

  private def bindVar(statement: PreparedStatement, idx: Int, obj: AnyRef) {
    if (obj.isInstanceOf[String]) {
      statement.setString(idx, obj.asInstanceOf[String])
    }
    else if (obj.isInstanceOf[Long]) {
      statement.setLong(idx, obj.asInstanceOf[Long])
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
      db.commit
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

  def pruneHistory(selections: List[FileMetaData]) {
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
      case e: JSONException => log.error(e)
      case e: SQLException => log.error(e)
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  def addAll(meta: List[FileMetaData]) {
    if (meta == null) return
    var db: Connection = null
    try {
      db = getDbConnection
      FullText.dropIndex(db, "PUBLIC", "FILE_INDEX")
      FullTextLucene.dropAll(db)
      FullText.closeAll

      db.setAutoCommit(false)
      addMeta(db, meta)
      db.commit

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

  def reindex {
    purge

    val fmds = cloudEngine.describeMeta.par.flatMap {
      ctx =>
        try {
          List(FileMetaData.create(ctx.hash, JsonUtil.loadJson(cloudEngine.load(ctx))))
        } catch {
          case e: Exception => {
            log.error(ctx, e)
            Nil
          }
        }
    }.toList

    addAll(fmds)
    pruneHistory(fmds)
  }

  def get(selections: JSONArray) {
    (0 until selections.length).par.foreach(i => fetch(FileMetaData.fromJson(selections.getJSONObject(i))))
  }

  // TODO: only read the file size bytes back (if the file is one block)
  // TODO: support writing to an offset of the existing file to allow for sub-blocks
  def fetch(fmd: FileMetaData) {
    val blockHashes = (0 until fmd.getBlockHashes.length).map(fmd.getBlockHashes.getString)
    if (blockHashes.size == 0) throw new IllegalArgumentException("no block hashes found!")
    if (blockHashes.find(h => !cloudEngine.contains(fmd.createBlockContext(h))) == None) {
      if (blockHashes.size == 1) {
        attemptSingleBlockFetch(blockHashes(0), fmd)
      } else {
        throw new RuntimeException("multiple block hashes not yet supported!")
      }
    } else {
      onMessage(String.format("some blocks of %s not currently available!", blockHashes.mkString(",")))
    }
  }

  def attemptSingleBlockFetch(blockHash: String, fmd: FileMetaData) : Boolean = {
    var success = false
    var retries = MAX_FETCH_RETRIES

    while (!success && retries > 0) {
      var remoteData: InputStream = null
      try {
        remoteData = cloudEngine.load(fmd.createBlockContext(blockHash))
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
        cloudEngine.ensure(fmd.createBlockContext(blockHash), true)
        if (!cloudEngine.contains(fmd.createBlockContext(blockHash))) {
          onMessage("giving up on %s, block %s not currently available!".format(fmd.getFilename, blockHash))
          retries = 0
        }
      }
    }
    success
  }

  def addTags(selections: JSONArray, tags: Set[String]): JSONArray = {
    val fmds = (0 until selections.length).par.flatMap {
      i =>
        val hash = selections.getJSONObject(i).getString("hash")
        val data = selections.getJSONObject(i).getJSONObject("data")
        val oldMeta = FileMetaData.create(hash, data)

        val newTags = FileMetaData.applyTags(oldMeta.getTags, tags)
        if (newTags.equals(oldMeta.getTags)) {
          Nil
        } else {
          data.put("tags", new JSONArray(newTags))

          val derivedMeta = FileMetaData.deriveMeta(hash, data)
          cloudEngine.store(derivedMeta.createBlockContext, new ByteArrayInputStream(derivedMeta.getDataAsString.getBytes("UTF-8")))
          List(derivedMeta)
        }
    }.toList

    addAll(fmds)
    pruneHistory(fmds)

    FileMetaData.toJsonArray(fmds)
  }

  def ensure(selections: JSONArray, blockLevelCheck: Boolean) {
    (0 until selections.length()).par.foreach{
      i =>
        val fmd = FileMetaData.fromJson(selections.getJSONObject(i))
        val hashes = Set(fmd.getHash) ++ (0 until fmd.getBlockHashes.length).map(fmd.getBlockHashes.getString)
        hashes.foreach{ hash =>
          if (!cloudEngine.ensure(fmd.createBlockContext(hash), blockLevelCheck)) {
            onMessage("%s: found incosistent block %s".format(fmd.getFilename, hash))
          }
        }
    }
  }

  // TODO: what about the meta.Parent chain? do we want to wipe out the entire chain?
  def remove(selections: JSONArray) {
    (0 until selections.length).par.foreach {
      i =>
        val fmd = FileMetaData.fromJson(selections.getJSONObject(i))
        cloudEngine.remove(fmd.createBlockContext)

        // TODO: only delete if there are no other files referencing these blocks
        if (false) {
          val blocks = fmd.getBlockHashes
          (0 until blocks.length).foreach(j => cloudEngine.remove(fmd.createBlockContext(blocks.getString(j))))
        }

         // TODO: we should only do this if we are sure the rest happened correctly (although at worst we could reindex)
        remove(fmd)
    }
  }

  def find(filter: JSONObject): JSONArray = {
    val results: JSONArray = new JSONArray

    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getReadOnlyDbConnection

      var sql: String = null
      val bind = new ListBuffer[AnyRef]

      if (filter.has("tags")) {
        val limit = if (filter.has("count")) filter.getInt("count") else 0
        val offset = if (filter.has("offset")) filter.getInt("offset") else 0
        sql = "SELECT T.HASH,T.RAWMETA FROM FTL_SEARCH_DATA(?, %d, %d) FTL, FILE_INDEX T WHERE FTL.TABLE='FILE_INDEX' AND T.HASH = FTL.KEYS[0]".format(limit, offset)
        bind.append(filter.getString("tags"))
      }
      else {
        val list = new ListBuffer[String]
        val iter = filter.keys
        while (iter.hasNext) {
          val key = iter.next.asInstanceOf[String]
          val obj = filter.get(key)
          if (obj.isInstanceOf[Array[String]] || obj.isInstanceOf[Array[Long]]) {
            val foo = List(obj)
            list.append(String.format("%s In (%s)", key.toUpperCase, StringUtil.joinRepeat(foo.size, "?", ",")))
            bind.appendAll(foo)
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
        if (list.size > 0) {
          sql = "SELECT HASH,RAWMETA FROM FILE_INDEX WHERE %s".format(list.mkString(" AND "))
        }
        else {
          sql = "SELECT HASH,RAWMETA FROM FILE_INDEX"
        }

        if (filter.has("count")) sql += " LIMIT %d".format(filter.getInt("count"))
        if (filter.has("offset")) sql += " OFFSET %d".format(filter.getInt("offset"))
      }
      statement = db.prepareStatement(sql)
      (0 until bind.size).foreach(i => bindVar(statement, i + 1, bind(i)))

      val rs = statement.executeQuery
      while (rs.next) {
        results.put(FileMetaData.create(rs.getString("HASH"), new JSONObject(rs.getString("RAWMETA"))).toJson)
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
    results
  }
}