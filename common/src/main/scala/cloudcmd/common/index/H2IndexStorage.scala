package cloudcmd.common.index

import cloudcmd.common._
import engine.{CloudEngineListener, CloudEngine}
import org.apache.log4j.Logger
import org.h2.fulltext.{FullText, FullTextLucene}
import org.h2.jdbcx.JdbcConnectionPool
import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import java.io.{FileInputStream, ByteArrayInputStream, InputStream, File}
import java.sql.{PreparedStatement, SQLException, Statement, Connection}
import collection.mutable.ListBuffer
import util.Random

class H2IndexStorage extends IndexStorage with IndexStorageListener {
  private val log = Logger.getLogger(classOf[H2IndexStorage])
  
  private val BATCH_SIZE = 1024
  private val WHITESPACE = " ,:-._" + File.separator
  
  private var _configRoot: String = null

  private var _cloudEngine: CloudEngine = null

  private var _listeners : List[CloudEngineListener] = List()

  private var _cp: JdbcConnectionPool = null

  private def getDbFile = "%s%sindex".format(_configRoot, File.separator)
  private def createConnectionString: String = "jdbc:h2:%s".format(getDbFile)
  private def getDbConnection = _cp.getConnection
  private def getReadOnlyDbConnection: Connection = {
    val conn = getDbConnection
    conn.setReadOnly(true)
    conn
  }

  def registerListener(listener: CloudEngineListener) {
    _listeners = _listeners ++ List(listener)
  }

  def onMessage(msg: String) {
    _listeners.foreach(_.onMessage(msg))
  }

  def init(configRoot: String, cloudEngine: CloudEngine) {
    _configRoot = configRoot
    _cloudEngine = cloudEngine

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
    import scala.collection.JavaConversions._

    var statement: PreparedStatement = null
    try {
      val bind = new ListBuffer[AnyRef]
      statement = db.prepareStatement(addMetaSql)

      var k = 0

      for (meta <- fmds) {
        bind.clear
        bind.add(meta.getHash)
        bind.add(meta.getPath)
        bind.add(meta.getFilename)
        bind.add(meta.getFileExt)
        bind.add(meta.getFileSize)
        bind.add(meta.getFileDate)
        bind.add(buildTags(meta))
        bind.add(meta.getDataAsString)
        (0 until bind.size).foreach(i => bindVar(statement, i + 1, bind.get(i)))
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

  private def buildTags(meta: FileMetaData) : String = {
    import scala.collection.JavaConversions._

    val tagSet = meta.getTags ++ meta.getPath ++ FileTypeUtil.instance.getTypeFromName(meta.getFilename)
    var tags = tagSet.mkString(" ")

    WHITESPACE.toCharArray.foreach{ ch =>
      tags = tags.replace(ch, ' ')
    }

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

  def pruneHistory(selections: List[FileMetaData]) {
    removeAll(Set() ++ selections.flatMap(fmd => if (fmd.getParent == null) { Nil } else { Set(fmd.getParent) }))
  }

  def removeAll(hashes : Set[String]) {
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      statement = db.prepareStatement("DELETE FROM FILE_INDEX WHERE HASH = ?")

      var k = 0
      hashes.foreach{ hash =>
        bindVar(statement, 1, hash)
        statement.addBatch

        k +=1
        if (k > BATCH_SIZE) {
          statement.executeBatch()
          k = 0
        }
      }

      statement.executeBatch()
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

    val fmds = _cloudEngine.getMetaHashSet().par.flatMap {
      hash =>
        try {
          List(MetaUtil.loadMeta(hash, JsonUtil.loadJson(_cloudEngine.load(hash))))
        } catch {
          case e: Exception => {
            log.error(hash, e)
            Nil
          }
        }
    }.toList

    addAll(fmds)
    pruneHistory(fmds)
  }

  def fetch(selections: JSONArray) {
    (0 until selections.length).par.foreach(i => fetch(MetaUtil.loadMeta(selections.getJSONObject(i))))
  }

  def fetch(meta: FileMetaData) {
    val blockHashes = (0 until meta.getBlockHashes().length()).map(meta.getBlockHashes().getString)
    val hashAdapterMap = blockHashes.flatMap {
      hash =>
        val hashProviders = _cloudEngine.getHashProviders(hash)
        if (hashProviders.size > 0) {
          Map(hash -> Random.shuffle(hashProviders).sortBy(_.Tier))
        } else {
          onMessage(String.format("could not find block %s in existing storage!", hash))
          Nil
        }
    }.toMap

    if (hashAdapterMap.size == blockHashes.size) {
      blockHashes.foreach {
        hash =>
          val blockProviders = hashAdapterMap.get(hash).get
          var success = false
          var i = 0
          while (!success && i < blockProviders.size) {
            var remoteData: InputStream = null
            try {
              // TODO: only read the file size bytes back (if the file is one block)
              // TODO: support writing to an offset of the existing file to allow for sub-blocks
              remoteData = blockProviders(i).load(hash)
              val destFile = new File(meta.getPath)
              destFile.getParentFile().mkdirs()
              val remoteDataHash = CryptoUtil.digestToString(CryptoUtil.writeAndComputeHash(remoteData, destFile))
              if (remoteDataHash.equals(hash)) {
                success = true
              } else {
                destFile.delete()
              }
            } catch {
              case e: Exception => {
                onMessage(String.format("failed to pull block %s", hash))
                // TODO: We should delete/recover the block from the adapter
              }
              log.error(hash, e)
            } finally {
              FileUtil.SafeClose(remoteData)
            }
            i += 1
          }

          if (success) {
            onMessage(String.format("%s pulled block %s", meta.getPath(), hash))
          } else {
            onMessage(String.format("%s failed to pull block %s", meta.getFilename(), hash))
            // TODO: attempt to rever the block and write it in the correct target file region
          }
      }
    }
  }

  def addTags(selections: JSONArray, tags: java.util.Set[String]): JSONArray = {
    val fmds = (0 until selections.length).par.flatMap {
      i =>
        val hash = selections.getJSONObject(i).getString("hash")
        val data = selections.getJSONObject(i).getJSONObject("data")
        val oldMeta = FileMetaData.create(hash, data)

        val newTags = MetaUtil.applyTags(oldMeta.getTags, tags)
        if (newTags.equals(oldMeta.getTags)) {
          Nil
        } else {
          data.put("tags", new JSONArray(newTags))

          val derivedMeta = MetaUtil.deriveMeta(hash, data)
          _cloudEngine.store(derivedMeta.getHash, new ByteArrayInputStream(derivedMeta.getDataAsString.getBytes("UTF-8")))
          List(derivedMeta)
        }
    }.toList

    import collection.JavaConversions._

    addAll(fmds)
    pruneHistory(fmds)

    MetaUtil.toJsonArray(fmds)
  }

  def verify(selections: JSONArray, deleteOnInvalid: Boolean) {
    _cloudEngine.verify(getHashesFromSelections(selections), deleteOnInvalid)
  }

  private def getHashesFromSelections(selections: JSONArray) : Set[String] = {
    Set() ++ (0 until selections.length).par.map {
      i => selections.getJSONObject(i).getString("hash")
    }
  }

  // TODO: remove java.util.
  def add(file: File, tags: java.util.Set[String]) {
    import collection.JavaConversions._
    _add(file, tags.toSet)
  }

  private def _add(file: File, tags: Set[String]) {
    _batchAdd(Set(file), tags)
  }

  // TODO: remove java.util.
  def batchAdd(fileSet: java.util.Set[File], tags: java.util.Set[String]) {
    import collection.JavaConversions._
    _batchAdd(fileSet.toSet, tags.toSet)
  }                                                                                 ˜

  private def _batchAdd(fileSet: Set[File], tags: Set[String]) {
    import collection.JavaConversions._

    val metaSet = new collection.mutable.HashSet[FileMetaData] with collection.mutable.SynchronizedSet[FileMetaData]

    fileSet.par.foreach {
      file =>
        var blockHash: String = null

        val startTime = System.currentTimeMillis()
        try {
          var fis = new FileInputStream(file)
          try {
            blockHash = CryptoUtil.computeHashAsString(fis)
          } finally {
            FileUtil.SafeClose(fis)
          }
          fis = new FileInputStream(file)
          try {
            _cloudEngine.store(blockHash, fis)
          } finally {
            FileUtil.SafeClose(fis)
          }
          val meta = MetaUtil.createMeta(file, java.util.Arrays.asList(blockHash), tags)
          _cloudEngine.store(meta.getHash(), new ByteArrayInputStream(meta.getDataAsString().getBytes("UTF-8")))
          metaSet.add(meta)
        }
        finally {
          onMessage("took %6d ms to index %s".format((System.currentTimeMillis() - startTime), file.getName()))
          if (blockHash == null) {
            throw new RuntimeException("failed to index file: " + file.getAbsolutePath())
          }
        }
    }

    addAll(metaSet.toList)
  }

  def sync(selections: JSONArray) {
    val pushSet = Set() ++ (0 until selections.length).par.flatMap {
      i =>
        val hash = selections.getJSONObject(i).getString("hash")
        if (hash.endsWith(".meta")) {
          val blocks = selections.getJSONObject(i).getJSONObject("data").getJSONArray("blocks")
          val allHashes = Set(hash) ++ (0 until blocks.length).flatMap(idx => Set(blocks.getString(idx)))
          allHashes.flatMap{ h =>
            val providers = _cloudEngine.getHashProviders(hash)
            if (providers.size > 0) {
              Set(h)
            } else {
              // TODO: we need to fire a data not found event here
              log.error("hash not found in storage: " + hash)
              Nil
            }
          }
        } else {
          log.error("unexpected hash type: " + hash)
          Nil
        }
    }

    _cloudEngine.sync(pushSet)
  }

  // TODO: what about the meta.Parent chain? do we want to wipe out the entire chain?
  def remove(selections: JSONArray) {
    (0 until selections.length).par.foreach {
      i =>
        val hash = selections.getJSONObject(i).getString("hash")
        val meta = JsonUtil.loadJson(_cloudEngine.load(hash))

        _cloudEngine.remove(hash)

        if (false) {
          // TODO: only delete if there are no other files referencing these blocks
          val blocks = meta.getJSONArray("blocks")
          (0 until blocks.length).foreach(j => _cloudEngine.remove(blocks.getString(j)))
        }

        val indexMeta = new JSONObject
        indexMeta.put("hash", hash)
        indexMeta.put("data", meta)

        // TODO: we should only do this if we are sure the rest happened correctly (although at worst we could reindex)
        remove(MetaUtil.loadMeta(indexMeta))
    }
  }

  def find(filter: JSONObject): JSONArray = {
    val results: JSONArray = new JSONArray
    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getReadOnlyDbConnection
      var sql: String = null
      val bind: List[AnyRef] = new ArrayList[AnyRef]
      if (filter.has("tags")) {
        sql = "SELECT T.HASH,T.RAWMETA FROM FTL_SEARCH_DATA(?, 0, 0) FTL, FILE_INDEX T WHERE FTL.TABLE='FILE_INDEX' AND T.HASH = FTL.KEYS[0]"
        bind.add(filter.getString("tags"))
      }
      else {
        val list: List[String] = new ArrayList[String]
        val iter: Iterator[String] = filter.keys
        while (iter.hasNext) {
          val key: String = iter.next
          val obj: AnyRef = filter.get(key)
          if (obj.isInstanceOf[Array[String]] || obj.isInstanceOf[Array[Long]]) {
            val foo: Collection[AnyRef] = Arrays.asList(obj)
            list.add(String.format("%s In (%s)", key.toUpperCase, StringUtil.joinRepeat(foo.size, "?", ",")))
            bind.addAll(foo)
          }
          else {
            if (obj.toString.contains("%")) {
              list.add(String.format("%s LIKE ?", key))
            }
            else {
              list.add(String.format("%s IN (?)", key))
            }
            bind.add(obj)
          }
        }
        if (list.size > 0) {
          sql = String.format("SELECT HASH,RAWMETA FROM FILE_INDEX WHERE %s", StringUtil.join(list, " AND "))
        }
        else {
          sql = String.format("SELECT HASH,RAWMETA FROM FILE_INDEX")
        }
      }
      if (filter.has("count")) sql += String.format(" LIMIT %d", filter.getInt("count"))
      if (filter.has("offset")) sql += String.format(" OFFSET %d", filter.getInt("offset"))
      statement = db.prepareStatement(sql)
      {
        var i: Int = 0
        var paramIdx: Int = 1
        while (i < bind.size) {
          {
            bind(statement, paramIdx, bind.get(i))
          }
          i += 1
          paramIdx += 1
        }
      }
      val rs: ResultSet = statement.executeQuery
      while (rs.next) {
        results.put(MetaUtil.loadMeta(rs.getString("HASH"), new JSONObject(rs.getString("RAWMETA"))).toJson)
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