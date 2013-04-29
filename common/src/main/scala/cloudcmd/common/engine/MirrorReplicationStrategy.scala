package cloudcmd.common.engine

import cloudcmd.common.FileUtil
import cloudcmd.common.adapters.{DataNotFoundException, Adapter}
import org.apache.log4j.Logger
import java.io.InputStream
import util.Random
import java.util.concurrent.atomic.AtomicInteger

class MirrorReplicationStrategy extends ReplicationStrategy {

  private val log: Logger = Logger.getLogger(classOf[MirrorReplicationStrategy])

  def isReplicated(hash: String, adapters: List[Adapter]): Boolean = {
    val acceptsAdapters = adapters.filter(_.accepts(hash))
    val adaptersMissingBlock = acceptsAdapters.filter(!_.contains(hash))
    val replicated = adaptersMissingBlock.size == 0
    replicated
  }

  def store(hash: String, dis: InputStream, adapters: List[Adapter]) {
    if (adapters == null || adapters.size == 0) throw new IllegalArgumentException("no adapters to store to")

    var containsAdapters = adapters.filter(_.contains(hash)).toList
    val nis = if (containsAdapters.size == 0) {
      adapters(0).store(hash, dis)
      containsAdapters = containsAdapters ++ List(adapters(0))
      null
    } else {
      dis
    }

    var missingAdapters = adapters.diff(containsAdapters)
    val pushedCount = new AtomicInteger(containsAdapters.size)

    if (missingAdapters.size > 0 && nis != null) {
      val adapter = missingAdapters(0)
      val is: InputStream = nis
      try {
        adapter.store(hash, is)
        pushedCount.incrementAndGet()
        containsAdapters = containsAdapters ++ List(adapter)
      }
      catch {
        case e: Exception => {
          onMessage(String.format("failed to sync block %s to %s", hash, adapter.URI.toString))
          log.error(hash, e)
        }
      }
      finally {
        FileUtil.SafeClose(is)
      }

      missingAdapters = missingAdapters.drop(1)
    }

    missingAdapters.par.foreach{ adapter =>
      val is: InputStream = load(hash, containsAdapters)._1
      try {
        adapter.store(hash, is)
        pushedCount.incrementAndGet()
        containsAdapters = containsAdapters ++ List(adapter)
      }
      catch {
        case e: Exception => {
          log.error(hash, e)
          onMessage(String.format("failed to sync block %s to %s", hash, String.valueOf(adapter.URI)))
        }
      }
      finally {
        FileUtil.SafeClose(is)
      }
    }

    if (pushedCount.get() != adapters.size) {
      onMessage("failed to store block %s on %d of %d adapters".format(hash, pushedCount.get, adapters.size))
    }
  }

  def load(hash: String, hashProviders: List[Adapter]): (InputStream, Int) = {
    if (hashProviders.size == 0) throw new DataNotFoundException(hash)
    Random.shuffle(hashProviders).sortBy(x => x.Tier).toList(0).load(hash)
  }

  def remove(hash: String, hashProviders: List[Adapter]) : Boolean = {
    var success = true
    hashProviders.par.foreach {
      adapter =>
        try {
          val deleteSuccess = adapter.remove(hash)
          if (deleteSuccess) {
            onMessage(String.format("successfully deleted block %s found on adapter %s", hash, adapter.URI))
          } else {
            success = false
            onMessage(String.format("failed to delete block %s found on adapter %s", hash, adapter.URI))
          }
        } catch {
          case e: Exception => {
            onMessage(String.format("failed to delete block %s on adapter %s", hash, adapter.URI))
            log.error(hash, e)
          }
        }
    }
    success
  }

  def ensure(hash: String, hashProviders: List[Adapter], adapters: List[Adapter], blockLevelCheck: Boolean) : Boolean = {
    if (hashProviders.size == 0) {
      return false
    }
    val consistencyResults = ensureExistingBlocks(hash, hashProviders, blockLevelCheck)
    val validProviders = consistencyResults.filter{case (adapter:Adapter, consistent: Boolean) => consistent}.keySet.toList
    if (validProviders.size == 0) {
      throw new DataNotFoundException(hash)
    }
    sync(hash, validProviders, adapters)
  }

  private def sync(hash: String, hashProviders: List[Adapter], adapters: List[Adapter]) : Boolean = {
    if (isReplicated(hash, adapters)) return true

    var is: InputStream = null
    try {
      is = load(hash, hashProviders)._1
      store(hash, is, adapters)
    }
    catch {
      case e: DataNotFoundException => {
        onMessage("no adapter has block %s".format(hash))
        log.error(hash, e)
      }
      case e: Exception => {
        onMessage("failed to sync block %s".format(hash))
        log.error(hash, e)
      }
    }
    finally {
      FileUtil.SafeClose(is)
    }

    isReplicated(hash, adapters)
  }

  private def ensureExistingBlocks(hash: String, hashProviders: List[Adapter], blockLevelCheck: Boolean) : Map[Adapter, Boolean] = {
    Map() ++ hashProviders.flatMap {
      adapter =>
        var isConsistent = false
        try {
          isConsistent = adapter.ensure(hash, blockLevelCheck)
          if (isConsistent) {
            // TODO: enable verbose flag
            //_wm.make(new MemoryElement("msg", "body", String.format("successfully validated block %s is on adapter %s", hash, adapter.URI)))
          } else {
            onMessage(String.format("bad block %s found on adapter %s", hash, adapter.URI))
          }
        } catch {
          case e: Exception => {
            onMessage(String.format("failed to verify block %s on adapter %s", hash, adapter.URI))
            log.error(hash, e)
          }
        }
        Map(adapter -> isConsistent)
    }
  }
}