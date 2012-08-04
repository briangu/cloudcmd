package cloudcmd.common.engine

import cloudcmd.common.{BlockContext, FileUtil}
import cloudcmd.common.adapters.{DataNotFoundException, Adapter}
import org.apache.log4j.Logger
import java.io.InputStream
import util.Random
import java.util.concurrent.atomic.AtomicInteger

class MirrorReplicationStrategy extends ReplicationStrategy {

  private val log: Logger = Logger.getLogger(classOf[MirrorReplicationStrategy])

  def isReplicated(ctx: BlockContext, adapters: List[Adapter]): Boolean = {
    adapters.par.filterNot(_.contains(ctx)).size > 0
  }

  def store(ctx: BlockContext, dis: InputStream, adapters: List[Adapter]) {
    if (adapters == null || adapters.size == 0) throw new IllegalArgumentException("no adapters to store to")

    var containsAdapters = adapters.par.filter(_.contains(ctx)).toList
    val nis = if (containsAdapters.size == 0) {
      adapters(0).store(ctx, dis)
      containsAdapters = containsAdapters ++ List(adapters(0))
      null
    } else {
      dis
    }

    val missingAdapters = adapters -- containsAdapters
    val pushedCount = new AtomicInteger(containsAdapters.size)

    missingAdapters.par.foreach{ adapter =>
      var is: InputStream = nis
      try {
        if (is == null) is = load(ctx, containsAdapters)
        adapter.store(ctx, is)
        pushedCount.incrementAndGet()
        containsAdapters = containsAdapters ++ List(adapter)
      }
      catch {
        case e: Exception => {
          onMessage(String.format("failed to sync block %s to %s", ctx, adapter.URI.toString))
          log.error(ctx, e)
        }
      }
      finally {
        FileUtil.SafeClose(is)
      }
    }

    if (pushedCount.get() != adapters.size) {
      onMessage("failed to store block %s on %d of %d adapters".format(ctx, pushedCount.get, adapters.size))
    }
  }

  def load(ctx: BlockContext, hashProviders: List[Adapter]): InputStream = {
    if (hashProviders.size == 0) throw new DataNotFoundException(ctx.hash)
    Random.shuffle(hashProviders).sortBy(x => x.Tier).toList(0).load(ctx)
  }

  def remove(ctx: BlockContext, hashProviders: List[Adapter]) : Boolean = {
    var success = true
    hashProviders.par.foreach {
      adapter =>
        try {
          val deleteSuccess = adapter.remove(ctx)
          if (deleteSuccess) {
            onMessage(String.format("successfully deleted block %s found on adapter %s", ctx, adapter.URI))
          } else {
            success = false
            onMessage(String.format("failed to delete block %s found on adapter %s", ctx, adapter.URI))
          }
        } catch {
          case e: Exception => {
            onMessage(String.format("failed to delete block %s on adapter %s", ctx, adapter.URI))
            log.error(ctx, e)
          }
        }
    }
    success
  }

  def ensure(ctx: BlockContext, hashProviders: List[Adapter], adapters: List[Adapter], blockLevelCheck: Boolean) : Boolean = {
    val consistencyResults = ensureExistingBlocks(ctx, hashProviders, blockLevelCheck)
    val validProviders = consistencyResults.filter{case (adapter:Adapter, consistent: Boolean) => consistent}.keySet.toList
    if (validProviders.size == 0) throw new DataNotFoundException(ctx.hash)
    sync(ctx, validProviders, adapters)
  }

  private def sync(ctx: BlockContext, hashProviders: List[Adapter], adapters: List[Adapter]) : Boolean = {
    if (isReplicated(ctx, hashProviders)) return true

    var is: InputStream = null
    try {
      is = load(ctx, hashProviders)
      store(ctx, is, adapters)
    }
    catch {
      case e: DataNotFoundException => {
        onMessage("no adapter has block %s".format(ctx))
        log.error(ctx, e)
      }
      case e: Exception => {
        onMessage("failed to sync block %s".format(ctx))
        log.error(ctx, e)
      }
    }
    finally {
      FileUtil.SafeClose(is)
    }

    isReplicated(ctx, hashProviders)
  }

  private def ensureExistingBlocks(ctx: BlockContext, hashProviders: List[Adapter], blockLevelCheck: Boolean) : Map[Adapter, Boolean] = {
    Map() ++ hashProviders.par.flatMap {
      adapter =>
        var isConsistent = false
        try {
          val isConsistent = adapter.ensure(ctx, blockLevelCheck)
          if (isConsistent) {
            // TODO: enable verbose flag
            //_wm.make(new MemoryElement("msg", "body", String.format("successfully validated block %s is on adapter %s", hash, adapter.URI)))
          } else {
            onMessage(String.format("bad block %s found on adapter %s", ctx, adapter.URI))
          }
        } catch {
          case e: Exception => {
            onMessage(String.format("failed to verify block %s on adapter %s", ctx, adapter.URI))
            log.error(ctx, e)
          }
        }
        Map(adapter -> isConsistent)
    }
  }
}