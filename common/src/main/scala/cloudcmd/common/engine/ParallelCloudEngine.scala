package cloudcmd.common.engine

import commands._
import ops.{Command, OpsFactory, WorkingMemory, OPS}
import cloudcmd.common.{FileMetaData, MetaUtil, JsonUtil, ResourceUtil}
import org.json.{JSONArray, JSONException, JSONObject}
import cloudcmd.common.adapters.Adapter
import java.io.{ByteArrayInputStream, File}
import org.apache.log4j.Logger
import cloudcmd.common.index.IndexStorageService
import cloudcmd.common.config.ConfigStorageService

class ParallelCloudEngine extends CloudEngine {

  val log = Logger.getLogger(classOf[ParallelCloudEngine])

  var _ops : OPS = null
  var _wm : WorkingMemory = null
  var _opsThread : Thread = null
  var _replicationStrategy : ReplicationStrategy = null

  private def buildRegistry : java.util.Map[String, Command] = {
    val registry = OpsFactory.getDefaultRegistry

    registry.put("fetch", new basic_fetch)
    registry.put("add_meta", new add_meta)
    registry.put("debug", new debug)
    registry.put("process", new process_raw)
    registry.put("index_default", new index_default)
    registry.put("sleep", new sleep)
    registry.put("pull_block", new pull_block)
    registry.put("push_block", new push_block)
    registry.put("push_block_async", new push_block_async)
    registry.put("verify_block", new verify_block)
    registry.put("remove_block", new remove_block)

    registry
  }

  def init(replicationStrategy: ReplicationStrategy) {
    init(replicationStrategy, "index.ops")
  }

  def init(replicationStrategy: ReplicationStrategy, opsName: String) {
    _replicationStrategy = replicationStrategy

    val registry = buildRegistry
    var indexOps : JSONObject = null

    try {
      indexOps = ResourceUtil.loadOps(opsName)
    } catch {
      case e:JSONException => {
        log.error("index.ops is not a valid JSON object.")
        throw e
      }
    }

    _ops = OpsFactory.create(registry, indexOps)
    _wm = _ops.getWorkingMemory
  }

  def prepareFlushToAdapter(adapter: Adapter) {
    BlockCacheService.instance.loadCache(adapter.Tier, adapter.Tier)
    _wm.make("flush_to_adapter", "src", BlockCacheService.instance.getBlockCache, "dest", adapter)
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

  def add(file: File, tags: java.util.Set[String]) {
    _wm.make("rawFile", "name", file.getName, "file", file, "tags", tags)
  }

  def push(minTier: Int, maxTier: Int) {
    push(minTier, maxTier, IndexStorageService.instance.find(new JSONObject))
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
    pull(retrieveBlocks, hashProviders.keySet.toSet)
  }

  def pull(minTier: Int, maxTier: Int, retrieveBlocks: Boolean, selections: JSONArray) {
    BlockCacheService.instance.loadCache(minTier, maxTier)
    val hashes = (0 until selections.length).map{ i => selections.getJSONObject(i).getString("hash")}
    pull(retrieveBlocks, hashes.toSet)
  }

  private def pull(retrieveBlocks: Boolean, hashes: Set[String]) {
    val localCache = BlockCacheService.instance.getBlockCache

    hashes.foreach{ hash =>
      if (!hash.endsWith(".meta")) {
        log.error("unexpected hash type: " + hash)
      } else if (!localCache.contains(hash)) {
        _wm.make("pull_block", "hash", hash, "retrieveSubBlocks", retrieveBlocks.asInstanceOf[AnyRef])
      }

      if (retrieveBlocks) {
        try {
          val meta = JsonUtil.loadJson(localCache.load(hash))
          val blocks = meta.getJSONArray("blocks")

          (0 until blocks.length).foreach{ i =>
            val blockHash = blocks.getString(i)
            if (!localCache.contains(blockHash)) {
              _wm.make("pull_block", "hash", blockHash)
            }
          }
        }
        catch
        {
          case e:Exception => e.printStackTrace
        }
      }
    }
  }

  def reindex {
    IndexStorageService.instance.purge

    val localCache = BlockCacheService.instance.getBlockCache
    val localDescription = localCache.describe

    import collection.JavaConversions._

    val fmds = localDescription.par.flatMap{ hash =>
      if (!hash.endsWith(".meta")) {
        Nil
      } else {
        try {
          val fmd = MetaUtil.loadMeta(hash, JsonUtil.loadJson(localCache.load(hash)))
          System.out.println(String.format("reindexing: %s %s", hash, fmd.getFilename))
          List(fmd)
        } catch {
          case e:Exception => {
            log.error(hash, e)
            Nil
          }
        }
      }
    }.toList

    IndexStorageService.instance.addAll(fmds)
    IndexStorageService.instance.pruneHistory(MetaUtil.toJsonArray(fmds))
  }

  def fetch(minTier: Int, maxTier: Int, selections: JSONArray) {
    BlockCacheService.instance.loadCache(minTier, maxTier)

    (0 until selections.length).par.foreach{ i =>
      try{
        _wm.make("fetch", "meta", MetaUtil.loadMeta(selections.getJSONObject(i)))
      } catch {
        case e:JSONException => log.error("index = " + i, e)
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

    val newSelections = MetaUtil.toJsonArray(fmds)

    IndexStorageService.instance.addAll(fmds)
    IndexStorageService.instance.pruneHistory(newSelections)

    newSelections
  }

  def verify(minTier: Int, maxTier: Int, deleteOnInvalid: Boolean) {
    import collection.JavaConversions._

    BlockCacheService.instance.loadCache(minTier, maxTier)
    BlockCacheService.instance.getHashProviders.keySet.foreach{ hash =>
      _wm.make("verify_block", "hash", hash, "deleteOnInvalid", deleteOnInvalid.asInstanceOf[AnyRef])
    }
  }

  def verify(minTier: Int, maxTier: Int, selections: JSONArray, deleteOnInvalid: Boolean) {
    BlockCacheService.instance.loadCache(minTier, maxTier)
    (0 until selections.length).par.foreach{ i =>
      val hash = selections.getJSONObject(i).getString("hash")
      _wm.make("verify_block", "hash", hash, "deleteOnInvalid", deleteOnInvalid.asInstanceOf[AnyRef])
    }
  }

  def remove(minTier: Int, maxTier: Int, selections: JSONArray) {
    BlockCacheService.instance.loadCache(minTier, maxTier)

    val hashProviders = BlockCacheService.instance.getHashProviders
    val localCache = BlockCacheService.instance.getBlockCache

    (0 until selections.length).par.foreach{ i =>
      val hash = selections.getJSONObject(i).getString("hash")

      if (hashProviders.containsKey(hash)) {
        val meta = JsonUtil.loadJson(localCache.load(hash))
        val blocks = meta.getJSONArray("blocks")

        (0 until blocks.length).foreach { j =>
          removeBlock(hashProviders, blocks.getString(j))
        }

        removeBlock(hashProviders, hash)

        val indexMeta = new JSONObject
        indexMeta.put("hash", hash)
        indexMeta.put("data", meta)

        // TODO: we should only do this if we are sure the rest happened correctly (although at worst we could reindex)
        IndexStorageService.instance.remove(MetaUtil.loadMeta(indexMeta))
      }
    }
  }

  private def removeBlock(hashProviders : java.util.Map[String, java.util.List[Adapter]], hash: String) {
    Option(hashProviders.get(hash)).foreach{ adapters =>
      import collection.JavaConversions._
      adapters.foreach(_wm.make("remove_block", "hash", hash, "adapter", _))
    }
  }
}