package cloudcmd.common.engine

import cloudcmd.common._
import org.json.{JSONArray, JSONObject}
import adapters.{MD5Storable, InlineStorable, Adapter}
import java.io.{InputStream, FileInputStream, ByteArrayInputStream, File}
import org.apache.log4j.Logger
import index.IndexStorage
import config.ConfigStorage
import collection.mutable
import scala.util.Random

class ParallelCloudEngine extends CloudEngine with CloudEngineListener {

  val log = Logger.getLogger(classOf[ParallelCloudEngine])

  var _opsThread: Thread = null
  var _replicationStrategy: ReplicationStrategy = null
  var _blockCache: BlockCache = null
  var _configService: ConfigStorage = null
  var _indexStorage: IndexStorage = null
  var _listeners : List[CloudEngineListener] = List()

  def init(configService: ConfigStorage, indexStorage: IndexStorage) {
    _configService = configService
    _blockCache = _configService.getBlockCache
    _replicationStrategy = _configService.getReplicationStrategy
  }

  def registerListeners(listener: CloudEngineListener) {
    _listeners = _listeners ++ List(listener)
  }

  def run {}
  def shutdown {}

  def onMessage(msg: String) {
    _listeners.foreach(_.onMessage(msg))
  }

  private def available(p: Adapter, minTier: Int, maxTier: Int) = (p.IsOnLine && p.Tier >= minTier && p.Tier <= maxTier)

  def refreshAdapterCaches(minTier: Int, maxTier: Int) {
    import scala.collection.JavaConversions._
    _configService.getAdapters.filter(available(_, minTier, maxTier)).par.foreach(_.refreshCache())
  }

  def getMetaHashSet(minTier: Int, maxTier: Int) : Set[String] = {
    import scala.collection.JavaConversions._
    val adapters = _configService.getAdapters.filter(available(_, minTier, maxTier))
    Set() ++ adapters.flatMap(p => p.describe.toSet).par.filter(hash => hash.endsWith(".meta"))
  }

  def getHashProviders(minTier: Int, maxTier: Int): Map[String, List[Adapter]] = {
    import scala.collection.JavaConversions._
    val adapters = _configService.getAdapters.filter(available(_, minTier, maxTier))
    Map() ++ adapters.flatMap(p => p.describe.toSet).par.flatMap {
      hash => Map(hash -> adapters.filter(_.describe().contains(hash)).toList)
    }
  }

  def getHashAdapters(minTier: Int, maxTier: Int, hash: String) : List[Adapter] = {
    import scala.collection.JavaConversions._
    _configService.getAdapters.filter(available(_, minTier, maxTier)).filter(_.describe().contains(hash)).toList
  }

  def add(file: File, tags: Set[String], adapter: Adapter) {
    batchAdd(Set(file), tags, adapter)
  }

  def batchAdd(fileSet: Set[File], tags: Set[String], adapter: Adapter) {
    import collection.JavaConversions._

    val metaSet = new mutable.HashSet[FileMetaData] with mutable.SynchronizedSet[FileMetaData]

    fileSet.par.foreach {
      file =>

        var blockHash: String = null
        var fis: FileInputStream = null
        var bais: ByteArrayInputStream = null

        try {
          val startTime = System.currentTimeMillis()
          try {
            val fileName = file.getName()
            val extIndex = fileName.lastIndexOf(".")
            val ext = if (extIndex > 0) {
              fileName.substring(extIndex + 1)
            } else {
              null
            }
            if (!FileTypeUtil.instance().skipExt(ext)) {
              val fileTags: Set[String] = if (ext == null) {
                tags.toSet
              } else {
                val fileType = FileTypeUtil.instance().getTypeFromExtension(ext)
                if (fileType != null) {
                  tags.toSet + fileType
                } else {
                  tags.toSet
                }
              }

              fis = new FileInputStream(file)
              blockHash = if (adapter.isInstanceOf[InlineStorable]) {
                adapter.asInstanceOf[InlineStorable].store(fis)
              } else {
                // TODO: compute all the hashes in a single pass
                //       adapters should register which hashes they need to do the store
                val hash = CryptoUtil.computeHashAsString(file)
                if (adapter.isInstanceOf[MD5Storable]) {
                  adapter.asInstanceOf[MD5Storable].store(fis, hash, CryptoUtil.computeMD5Hash(file), file.length())
                } else {
                  adapter.store(fis, hash)
                }
                hash
              }

              val meta = MetaUtil.createMeta(file, java.util.Arrays.asList(blockHash), fileTags)
              if (!adapter.contains(meta.getHash())) {
                adapter.store(new ByteArrayInputStream(meta.getDataAsString().getBytes("UTF-8")), meta.getHash())
                metaSet.add(meta)
              }
            }
          }
          finally {
            onMessage("took %6d ms to index %s".format((System.currentTimeMillis() - startTime), file.getName()))
          }
        }
        finally {
          FileUtil.SafeClose(fis)
          FileUtil.SafeClose(bais)
        }

        if (blockHash == null) {
          throw new RuntimeException("failed to index file: " + file.getAbsolutePath())
        }
    }

    _indexStorage.addAll(metaSet.toList)
  }

  def sync(minTier: Int, maxTier: Int, selections: JSONArray) {

    import collection.JavaConversions._

    val destAdapters = _configService.getAdapters.filter {
      adapter => adapter.Tier >= minTier && adapter.Tier <= maxTier
    }.toSet

    if (destAdapters.size == 1) return

    val hashProviders = _blockCache.loadCache(minTier, maxTier)

    val pushSet = (0 until selections.length).par.flatMap {
      i =>
        val hash = selections.getJSONObject(i).getString("hash")
        if (hash.endsWith(".meta")) {
          val blocks = selections.getJSONObject(i).getJSONObject("data").getJSONArray("blocks")
          val allHashes = Set(hash) ++ (0 until blocks.length).flatMap(idx => Set(blocks.getString(idx)))
          allHashes.flatMap{ h =>
            if (hashProviders.containsKey(hash)) {
              if (_replicationStrategy.isReplicated(destAdapters, h)) {
                Nil
              } else {
                Set(h)
              }
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

    pushSet.par.foreach {
      hash => _replicationStrategy.push(this, destAdapters, hash)
    }
  }

  def reindex {
    import collection.JavaConversions._

    _indexStorage.purge

    val fmds = _blockCache.getHashProviders.keySet.par.filter(_.endsWith(".meta")).par.flatMap {
      hash =>
        try {
          List(MetaUtil.loadMeta(hash, JsonUtil.loadJson(_replicationStrategy.load(hash))))
        } catch {
          case e: Exception => {
            log.error(hash, e)
            Nil
          }
        }
    }.toList

    _indexStorage.addAll(fmds)
    _indexStorage.pruneHistory(fmds)
  }

  def fetch(minTier: Int, maxTier: Int, selections: JSONArray) {
    _blockCache.loadCache(minTier, maxTier)
    (0 until selections.length).par.foreach(i => fetch(minTier, maxTier, MetaUtil.loadMeta(selections.getJSONObject(i))))
  }

  def fetch(minTier: Int, maxTier: Int, meta: FileMetaData) {
    import collection.JavaConversions._

    val hashProviders = _blockCache.getHashProviders()
    val blockHashes = (0 until meta.getBlockHashes().length()).map(meta.getBlockHashes().getString)

    val hashAdapterMap: Map[String, List[Adapter]] = blockHashes.flatMap {
      hash =>
        if (hashProviders.contains(hash)) {
          val blockProviders = hashProviders.get(hash).get.filter {
            a => a.Tier >= minTier && a.Tier <= maxTier
          }.toList
          if (blockProviders.size == 0) {
            onMessage(String.format("could not find block %s in existing storage! no adapters in specified tier range", hash))
            Nil
          } else {
            Map(hash -> Random.shuffle(blockProviders).sortWith(_.Tier < _.Tier))
          }
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

  def addTags(selections: JSONArray, tags: Set[String]): JSONArray = {
    val hashProviders = _blockCache.getHashProviders

    val fmds = (0 until selections.length).par.flatMap {
      i =>
        val hash = selections.getJSONObject(i).getString("hash")
        val data = selections.getJSONObject(i).getJSONObject("data")
        val oldMeta = FileMetaData.create(hash, data)

        import collection.JavaConversions._
        val newTags = MetaUtil.applyTags(oldMeta.getTags, tags)
        if (newTags.equals(oldMeta.getTags)) {
          Nil
        } else {
          data.put("tags", new JSONArray(newTags))

          val derivedMeta = MetaUtil.deriveMeta(hash, data)
          if (hashProviders.contains(derivedMeta.getHash)) {
            Nil
          } else {
//            localCache.store(new ByteArrayInputStream(derivedMeta.getDataAsString.getBytes("UTF-8")), derivedMeta.getHash)
            List(derivedMeta)
          }
        }
    }.toList

    import collection.JavaConversions._

    _indexStorage.addAll(fmds)
    _indexStorage.pruneHistory(fmds)

    MetaUtil.toJsonArray(fmds)
  }

  private def verify(minTier: Int, maxTier: Int, deleteOnInvalid: Boolean, hashes: Set[String]) {
    hashes.par.foreach {
      hash =>
        _blockCache.getHashProviders.get(hash).get.par.foreach {
          adapter =>
            try {
              val isValid = adapter.verify(hash)
              if (isValid) {
                // TODO: enable verbose flag
                //_wm.make(new MemoryElement("msg", "body", String.format("successfully validated block %s is on adapter %s", hash, adapter.URI)))
              } else {
                onMessage(String.format("bad block %s found on adapter %s", hash, adapter.URI))
                if (deleteOnInvalid) {
                  try {
                    val deleteSuccess = adapter.remove(hash)
                    if (deleteSuccess) {
                      onMessage(String.format("successfully deleted block %s found on adapter %s", hash, adapter.URI))
                    } else {
                      onMessage(String.format("failed to delete block %s found on adapter %s", hash, adapter.URI))
                    }
                  } catch {
                    case e: Exception => {
                      onMessage(String.format("failed to delete block %s on adapter %s", hash, adapter.URI))
                      log.error(hash, e)
                    }
                  }
                }
              }
            } catch {
              case e: Exception => {
                onMessage(String.format("failed to verify block %s on adapter %s", hash, adapter.URI))
                log.error(hash, e)
              }
            }
        }
    }
  }

  def verify(minTier: Int, maxTier: Int, selections: JSONArray, deleteOnInvalid: Boolean) {
    _blockCache.loadCache(minTier, maxTier)
    val hashes = (0 until selections.length).map {
      i => selections.getJSONObject(i).getString("hash")
    }
    verify(minTier, maxTier, deleteOnInvalid, hashes.toSet)
  }

  def remove(minTier: Int, maxTier: Int, selections: JSONArray) {
    _blockCache.loadCache(minTier, maxTier)

    val hashProviders = _blockCache.getHashProviders

    (0 until selections.length).par.foreach {
      i =>
        val hash = selections.getJSONObject(i).getString("hash")
        val meta = JsonUtil.loadJson(_replicationStrategy.load(hash))

        removeBlock(hashProviders, hash)

        if (false) {
          // TODO: only delete if there are no other files referencing these blocks
          val blocks = meta.getJSONArray("blocks")
          (0 until blocks.length).foreach(j => removeBlock(hashProviders, blocks.getString(j)))
        }

        val indexMeta = new JSONObject
        indexMeta.put("hash", hash)
        indexMeta.put("data", meta)

        // TODO: we should only do this if we are sure the rest happened correctly (although at worst we could reindex)
        _indexStorage.remove(MetaUtil.loadMeta(indexMeta))
    }
  }

  private def removeBlock(hashProviders: Map[String, List[Adapter]], hash: String) {
    import collection.JavaConversions._

    Option(hashProviders.get(hash).get).foreach {
      adapters =>
        adapters.par.foreach {
          adapter =>
            try {
              val deleteSuccess = adapter.remove(hash)
              if (deleteSuccess) {
                onMessage(String.format("successfully deleted block %s found on adapter %s", hash, adapter.URI))
              } else {
                onMessage(String.format("failed to delete block %s found on adapter %s", hash, adapter.URI))
              }
            } catch {
              case e: Exception => {
                onMessage(String.format("failed to delete block %s on adapter %s", hash, adapter.URI))
                log.error(hash, e)
              }
            }
        }
    }
  }
}