package cloudcmd.common.engine

import ops._
import cloudcmd.common._
import org.json.{JSONArray, JSONException, JSONObject}
import cloudcmd.common.adapters.{FileAdapter, Adapter}
import java.io.{InputStream, FileInputStream, ByteArrayInputStream, File}
import org.apache.log4j.Logger
import cloudcmd.common.index.IndexStorageService
import cloudcmd.common.config.ConfigStorageService
import java.util
import collection.mutable
import scala.util.Random

class ParallelCloudEngine extends CloudEngine {

  val log = Logger.getLogger(classOf[ParallelCloudEngine])

  var _ops : OPS = null
  var _wm : WorkingMemory = null
  var _opsThread : Thread = null
  var _replicationStrategy : ReplicationStrategy = null

  def init(replicationStrategy: ReplicationStrategy, opsName: String) {
    _replicationStrategy = replicationStrategy

    var indexOps : JSONObject = null

    try {
      indexOps = ResourceUtil.loadOps(opsName)
    } catch {
      case e:JSONException => {
        log.error("index.ops is not a valid JSON object.")
        throw e
      }
    }

    _ops = OpsFactory.create(OpsFactory.getDefaultRegistry, indexOps)
    _wm = _ops.getWorkingMemory
  }

  def run {
    if (_ops != null) _ops.run
  }

  def shutdown {
    if (_opsThread != null) {
      _opsThread.interrupt

      try{
        _opsThread.join(1000)
      } catch {
        case e:InterruptedException => log.error(e)
      }
    }

    if (_ops != null) {
      _ops.shutdown
    }
  }

  def add(file: File, tags: java.util.Set[String], adapter: Adapter) {
    val set = new java.util.HashSet[File]
    set.add(file)
    batchAdd(set, tags, adapter)
  }

  def batchAdd(fileSet: java.util.Set[File], tags: java.util.Set[String], adapter: Adapter) {
    import collection.JavaConversions._

    val metaSet = new mutable.HashSet[FileMetaData] with mutable.SynchronizedSet[FileMetaData]

    fileSet.par.foreach{ file =>

     var blockHash : String = null
     var fis : FileInputStream = null
     var bais :ByteArrayInputStream = null

     try
     {
       val startTime = System.currentTimeMillis()
       try
       {
         val fileName = file.getName()
         val extIndex = fileName.lastIndexOf(".")
         val ext = if ( extIndex > 0 ) { fileName.substring(extIndex + 1) } else { null }
         if (!FileTypeUtil.instance().skipExt(ext))
         {
           val fileTags : Set[String] = if (ext == null) {
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
           blockHash = adapter.store(fis)

           val meta = MetaUtil.createMeta(file, util.Arrays.asList(blockHash), fileTags)
           if (!adapter.contains(meta.getHash()))
           {
             bais = new ByteArrayInputStream(meta.getDataAsString().getBytes("UTF-8"))
             adapter.store(bais, meta.getHash())
             metaSet.add(meta)
           }
         }
       }
       finally
       {
         val msg = "took %6d ms to index %s".format((System.currentTimeMillis() - startTime), file.getName())
         _wm.make(new MemoryElement("msg", "body", msg))
       }
     }
     finally
     {
       FileUtil.SafeClose(fis)
       FileUtil.SafeClose(bais)
     }

     if (blockHash == null)
     {
       throw new RuntimeException("failed to index file: " + file.getAbsolutePath())
     }
   }

    IndexStorageService.instance().addAll(metaSet.toList)
  }

  def push(minTier: Int, maxTier: Int, selections: JSONArray) {

    import collection.JavaConversions._

    val destAdapters = ConfigStorageService.instance.getAdapters.filter{ adapter =>
      adapter.Tier >= minTier && adapter.Tier <= maxTier
    }.toSet

    BlockCacheService.instance.loadCache(minTier, maxTier)
    val localDescription = BlockCacheService.instance.getBlockCache.describe

    val pushSet = (0 until selections.length).par.flatMap{ i =>
      val hash = selections.getJSONObject(i).getString("hash")
      if (!hash.endsWith(".meta")) {
        log.error("unexpected hash type: " + hash)
        Nil
      } else if (!localDescription.contains(hash)) {
        // the index should always by in sync with the local cache
        log.error("hash not found in local cache: " + hash)
        Nil
      } else {
        val blocks = selections.getJSONObject(i).getJSONObject("data").getJSONArray("blocks")
        val searchSet = Set(hash) ++ (0 until blocks.length).flatMap{ blockIdx => Set(blocks.getString(blockIdx))}
        searchSet.filterNot{p =>  _replicationStrategy.isReplicated(destAdapters, hash)}
      }
    }

    pushSet.par.foreach{ hash => _replicationStrategy.push(_wm, destAdapters, hash) }
  }

  def pull(minTier: Int, maxTier: Int, retrieveBlocks: Boolean) {
    import collection.JavaConversions._
    BlockCacheService.instance.loadCache(minTier, maxTier)
    val hashProviders = BlockCacheService.instance.getHashProviders
    pull(minTier, maxTier, retrieveBlocks, hashProviders.keySet.filter(n => n.endsWith(".meta")).toSet)
  }

  def pull(minTier: Int, maxTier: Int, retrieveBlocks: Boolean, selections: JSONArray) {
    BlockCacheService.instance.loadCache(minTier, maxTier)
    val hashes = (0 until selections.length).map{ i => selections.getJSONObject(i).getString("hash")}
    pull(minTier, maxTier, retrieveBlocks, hashes.toSet)
  }

  private def pull(minTier: Int, maxTier: Int, retrieveBlocks: Boolean, hashes: Set[String]) {
    import collection.JavaConversions._

    val hashProviders = BlockCacheService.instance.getHashProviders

    val localCache = BlockCacheService.instance.getBlockCache
    val missingHashes = hashes -- localCache.describe()

    missingHashes.par.foreach{ hash =>
      val srcAdapters = hashProviders.get(hash).filter{ a => a.Tier >= minTier && a.Tier <= maxTier }.toSet
      _replicationStrategy.pull(_wm, srcAdapters, hash)

      /*
             FileMetaData fmd = MetaUtil.loadMeta(hash, JsonUtil.loadJson(BlockCacheService.instance().getBlockCache().load(hash)))
         // if localcache has block continue
 //        IndexStorageService.instance().add(fmd)
      */
    }

    if (retrieveBlocks) {
      val localCache = BlockCacheService.instance.getBlockCache

      val blockSet = hashes.par.flatMap{ hash =>
        if (hash.endsWith(".meta")) {
          val meta = JsonUtil.loadJson(localCache.load(hash))
          val blocks = meta.getJSONArray("blocks")

          (0 until blocks.length).flatMap{ i =>
            val blockHash = blocks.getString(i)
            if (localCache.contains(blockHash)) {
              Nil
            } else {
              Set(blockHash)
            }
          }
        } else {
          log.error("unexpected hash type: " + hash)
          Nil
        }
      }

      blockSet.par.foreach{ hash =>
        val srcAdapters = hashProviders.get(hash).filter{ a => a.Tier >= minTier && a.Tier <= maxTier }.toSet
        _replicationStrategy.pull(_wm, srcAdapters, hash)
      }
    }
  }

  def reindex {
    import collection.JavaConversions._

    IndexStorageService.instance.purge

    val localCache = BlockCacheService.instance.getBlockCache

    val fmds = if (localCache.isInstanceOf[FileAdapter]) {
      localCache.asInstanceOf[FileAdapter].describeMeta().toList
    } else {
      localCache.describe().par.flatMap{ hash =>
        if (hash.endsWith(".meta")) {
          try {
            List(MetaUtil.loadMeta(hash, JsonUtil.loadJson(localCache.load(hash))))
          } catch {
            case e:Exception => {
              log.error(hash, e)
              Nil
            }
          }
        } else {
          Nil
        }
      }
    }.toList

    IndexStorageService.instance.addAll(fmds)
    IndexStorageService.instance.pruneHistory(fmds)
  }

  def fetch(minTier: Int, maxTier: Int, selections: JSONArray) {
    BlockCacheService.instance.loadCache(minTier, maxTier)
    (0 until selections.length).par.foreach(i => fetch(minTier, maxTier, MetaUtil.loadMeta(selections.getJSONObject(i))))
  }

  def fetch(minTier: Int, maxTier: Int, meta: FileMetaData) {
    import collection.JavaConversions._

    val hashProviders = BlockCacheService.instance.getHashProviders()
    val blockHashes = (0 until meta.getBlockHashes().length()).map(meta.getBlockHashes().getString)

    val hashAdapterMap: Map[String, List[Adapter]] = blockHashes.flatMap { hash =>
      if (hashProviders.contains(hash)) {
        val blockProviders = hashProviders.get(hash).filter{ a => a.Tier >= minTier && a.Tier <= maxTier }.toList
        if (blockProviders.size == 0) {
          _wm.make("msg", "body", String.format("could not find block %s in existing storage! no adapters in specified tier range", hash))
          Nil
        } else {
          Map(hash -> Random.shuffle(blockProviders).sortWith(_.Tier < _.Tier))
        }
      } else {
        _wm.make("msg", "body", String.format("could not find block %s in existing storage!", hash))
        Nil
      }
    }.toMap

    if (hashAdapterMap.size == blockHashes.size) {
      blockHashes.foreach { hash =>
        val blockProviders = hashAdapterMap.get(hash).get
        var success = false
        var i = 0
        while (!success && i < blockProviders.size) {
          var remoteData : InputStream = null
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
            case e:Exception => {
              _wm.make(new MemoryElement("msg", "body", String.format("failed to pull block %s", hash)))
              // TODO: We should delete/recover the block from the adapter
            }
            log.error(hash, e)
          } finally {
            FileUtil.SafeClose(remoteData)
          }
          i += 1
        }

        if (success) {
          _wm.make(new MemoryElement("msg", "body", String.format("%s pulled block %s", meta.getPath(), hash)))
        } else {
          _wm.make(new MemoryElement("msg", "body", String.format("%s failed to pull block %s", meta.getFilename(), hash)))
          // TODO: attempt to rever the block and write it in the correct target file region
        }
      }
    }
  }

  def addTags(selections: JSONArray, tags: java.util.Set[String]) : JSONArray = {
    val localCache = BlockCacheService.instance.getBlockCache
    val localDescription = localCache.describe

    val fmds = (0 until selections.length).par.flatMap{ i =>
      val hash = selections.getJSONObject(i).getString("hash")
      val data = selections.getJSONObject(i).getJSONObject("data")
      val oldMeta = FileMetaData.create(hash, data)

      val newTags = MetaUtil.applyTags(oldMeta.getTags, tags)
      if (newTags.equals(oldMeta.getTags)) {
        Nil
      } else {
        data.put("tags", new JSONArray(newTags))

        val derivedMeta = MetaUtil.deriveMeta(hash, data)
        if (localDescription.contains(derivedMeta.getHash)) {
          Nil
        } else {
          localCache.store(new ByteArrayInputStream(derivedMeta.getDataAsString.getBytes("UTF-8")), derivedMeta.getHash)
          List(derivedMeta)
        }
      }
    }.toList

    import collection.JavaConversions._

    IndexStorageService.instance.addAll(fmds)
    IndexStorageService.instance.pruneHistory(fmds)

    MetaUtil.toJsonArray(fmds)
  }

  private def verify(minTier: Int, maxTier: Int, deleteOnInvalid: Boolean, hashes: Set[String]) {
    import collection.JavaConversions._

    hashes.par.foreach{ hash =>
      BlockCacheService.instance.getHashProviders.get(hash).par.foreach{ adapter =>
        try {
          val isValid = adapter.verify(hash)
          if (isValid) {
            // TODO: enable verbose flag
            //_wm.make(new MemoryElement("msg", "body", String.format("successfully validated block %s is on adapter %s", hash, adapter.URI)))
          } else {
            _wm.make(new MemoryElement("msg", "body", String.format("bad block %s found on adapter %s", hash, adapter.URI)))
            if (deleteOnInvalid) {
              try {
                val deleteSuccess = adapter.remove(hash)
                if (deleteSuccess) {
                  _wm.make(new MemoryElement("msg", "body", String.format("successfully deleted block %s found on adapter %s", hash, adapter.URI)))
                } else {
                  _wm.make(new MemoryElement("msg", "body", String.format("failed to delete block %s found on adapter %s", hash, adapter.URI)))
                }
              } catch {
                case e:Exception => {
                  _wm.make(new MemoryElement("msg", "body", String.format("failed to delete block %s on adapter %s", hash, adapter.URI)))
                  log.error(hash, e)
                }
              }
            }
          }
        } catch {
          case e:Exception => {
            _wm.make(new MemoryElement("msg", "body", String.format("failed to verify block %s on adapter %s", hash, adapter.URI)))
            log.error(hash, e)
          }
        }
      }
    }
  }

  def verify(minTier: Int, maxTier: Int, selections: JSONArray, deleteOnInvalid: Boolean) {
    BlockCacheService.instance.loadCache(minTier, maxTier)
    val hashes = (0 until selections.length).map{ i => selections.getJSONObject(i).getString("hash")}
    verify(minTier, maxTier, deleteOnInvalid, hashes.toSet)
  }

  def remove(minTier: Int, maxTier: Int, selections: JSONArray) {
    BlockCacheService.instance.loadCache(minTier, maxTier)

    val hashProviders = BlockCacheService.instance.getHashProviders
    val localCache = BlockCacheService.instance.getBlockCache

    (0 until selections.length).par.foreach{ i =>
      val hash = selections.getJSONObject(i).getString("hash")

      removeBlock(hashProviders, hash)

      val meta = JsonUtil.loadJson(localCache.load(hash))

      if (false) {
        // TODO: only delete if there are no other files referencing these blocks
        val blocks = meta.getJSONArray("blocks")
        (0 until blocks.length).foreach(j => removeBlock(hashProviders, blocks.getString(j)))
      }

      val indexMeta = new JSONObject
      indexMeta.put("hash", hash)
      indexMeta.put("data", meta)

      // TODO: we should only do this if we are sure the rest happened correctly (although at worst we could reindex)
      IndexStorageService.instance.remove(MetaUtil.loadMeta(indexMeta))
    }
  }

  private def removeBlock(hashProviders : java.util.Map[String, java.util.List[Adapter]], hash: String) {
    import collection.JavaConversions._

    Option(hashProviders.get(hash)).foreach{ adapters =>
      adapters.par.foreach{ adapter =>
        try {
          val deleteSuccess = adapter.remove(hash)
          if (deleteSuccess) {
            _wm.make(new MemoryElement("msg", "body", String.format("successfully deleted block %s found on adapter %s", hash, adapter.URI)))
          } else {
            _wm.make(new MemoryElement("msg", "body", String.format("failed to delete block %s found on adapter %s", hash, adapter.URI)))
          }
        } catch {
          case e:Exception => {
            _wm.make(new MemoryElement("msg", "body", String.format("failed to delete block %s on adapter %s", hash, adapter.URI)))
            log.error(hash, e)
          }
        }
      }
    }
  }
}