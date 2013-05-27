package cloudcmd.common.engine

import cloudcmd.common.{BlockContext, FileUtil}
import cloudcmd.common.adapters.{MultiWriteBlockException, DataNotFoundException, IndexedAdapter}
import org.apache.log4j.Logger
import java.io.InputStream
import util.Random
import java.util.concurrent.atomic.AtomicInteger

class MirrorReplicationStrategy extends ReplicationStrategy {

  private val log: Logger = Logger.getLogger(classOf[MirrorReplicationStrategy])

  def isReplicated(ctx: BlockContext, adapters: List[IndexedAdapter]): Boolean = {
    val acceptsAdapters = adapters.filter(_.accepts(ctx))
    val adaptersMissingBlock = acceptsAdapters.filter(!_.contains(ctx))
    val replicated = adaptersMissingBlock.size == 0
    replicated
  }

  def store(ctx: BlockContext, dis: InputStream, adapters: List[IndexedAdapter]) {
    if (adapters == null || adapters.size == 0) {
      throw new IllegalArgumentException("no adapters to store to")
    }

    var containsAdapters = adapters.filter(_.contains(ctx)).sortBy(_.Tier).toList
    val nis = if (containsAdapters.size == 0) {
      log.debug("storing %s to adapter %s".format(ctx.getId(), adapters(0).getSignature))
      adapters(0).store(ctx, dis)
      containsAdapters = List(adapters(0))
      null
    } else {
      dis
    }

    val startContainsAdapters = containsAdapters
    var failedAdapters = List[IndexedAdapter]()
    var missingAdapters = adapters.diff(containsAdapters).sortBy(_.Tier).toList
    if (missingAdapters.size > 0) {

      val pushedCount = new AtomicInteger()

      if (nis != null) {
        val adapter = missingAdapters(0)
        val is: InputStream = nis
        try {
          log.debug("storing %s to adapter %s".format(ctx.getId(), adapter.getSignature))
          adapter.store(ctx, is)
          pushedCount.incrementAndGet()
          containsAdapters = containsAdapters ++ List(adapter)
          missingAdapters = missingAdapters.drop(1)
        }
        catch {
          case e: Exception => {
            log.error(String.format("failed to sync block %s to %s", ctx, adapter.getSignature), e)
            failedAdapters = failedAdapters ++ List(adapter)
          }
        }
        finally {
          FileUtil.SafeClose(is)
        }
      }

      if (missingAdapters.size > 0) {
        if (containsAdapters.size > 0) {
          // TODO: we should have better planning here on which containsAdapters to use so the same
          //       one doesn't get overused if there are equal options
          missingAdapters.par.foreach { adapter =>
            val is: InputStream = load(ctx, containsAdapters)._1
            try {
              log.debug("storing %s to adapter %s".format(ctx.getId(), adapter.getSignature))
              adapter.store(ctx, is)
              pushedCount.incrementAndGet()
              containsAdapters = containsAdapters ++ List(adapter)
            }
            catch {
              case e: Exception => {
                log.error(String.format("failed to sync block %s to %s", ctx, String.valueOf(adapter.URI)), e)
                failedAdapters = failedAdapters ++ List(adapter)
              }
            }
            finally {
              FileUtil.SafeClose(is)
            }
          }
        }
      }

      if ((pushedCount.get() + startContainsAdapters.size) != adapters.size) {
        val missingCount = adapters.size - startContainsAdapters.size - pushedCount.get()
        log.error("failed to store block %s on %d of %d adapters".format(ctx, missingCount, adapters.size))
        val successAdapters = containsAdapters.diff(startContainsAdapters)
        throw new MultiWriteBlockException(ctx, adapters, successAdapters, failedAdapters)
      }
    }
  }

  def load(ctx: BlockContext, hashProviders: List[IndexedAdapter]): (InputStream, Int) = {
    if (hashProviders.size == 0) throw new DataNotFoundException(ctx)
    Random.shuffle(hashProviders).sortBy(x => x.Tier).toList(0).load(ctx)
  }

  def remove(ctx: BlockContext, hashProviders: List[IndexedAdapter]) : Boolean = {
    var success = true
    hashProviders.par.foreach {
      adapter =>
        try {
          val deleteSuccess = adapter.remove(ctx)
          if (deleteSuccess) {
            log.debug(String.format("successfully deleted block %s found on adapter %s", ctx, adapter.URI))
          } else {
            success = false
            log.error(String.format("failed to delete block %s found on adapter %s", ctx, adapter.URI))
          }
        } catch {
          case e: Exception => {
            log.error(String.format("failed to delete block %s on adapter %s", ctx, adapter.URI), e)
          }
        }
    }
    success
  }

  def ensure(ctx: BlockContext, hashProviders: List[IndexedAdapter], adapters: List[IndexedAdapter], blockLevelCheck: Boolean) : Boolean = {
    if (hashProviders.size == 0) {
      return false
    }
    val consistencyResults = ensureExistingBlocks(ctx, hashProviders, blockLevelCheck)
    val validProviders = consistencyResults.filter{case (adapter:IndexedAdapter, consistent: Boolean) => consistent}.keySet.toList
    if (validProviders.size == 0) {
      throw new DataNotFoundException(ctx)
    }
    sync(ctx, validProviders, adapters)
  }

  private def sync(ctx: BlockContext, hashProviders: List[IndexedAdapter], adapters: List[IndexedAdapter]) : Boolean = {
    if (isReplicated(ctx, adapters)) {
      return true
    }

    var is: InputStream = null
    try {
      is = load(ctx, hashProviders)._1
      store(ctx, is, adapters)
    }
    catch {
      case e: DataNotFoundException => {
        log.error("no adapter has block %s".format(ctx), e)
      }
      case e: Exception => {
        log.error("failed to sync block %s".format(ctx), e)
      }
    }
    finally {
      FileUtil.SafeClose(is)
    }

    isReplicated(ctx, adapters)
  }

  private def ensureExistingBlocks(ctx: BlockContext, hashProviders: List[IndexedAdapter], blockLevelCheck: Boolean) : Map[IndexedAdapter, Boolean] = {
    Map() ++ hashProviders.flatMap {
      adapter =>
        var isConsistent = false
        try {
          isConsistent = adapter.ensure(ctx, blockLevelCheck)
          if (isConsistent) {
            // TODO: enable verbose flag
            //_wm.make(new MemoryElement("msg", "body", String.format("successfully validated block %s is on adapter %s", hash, adapter.URI)))
          } else {
            log.warn(String.format("bad block %s found on adapter %s", ctx, adapter.URI))
          }
        } catch {
          case e: Exception => {
            log.error(String.format("failed to verify block %s on adapter %s", ctx, adapter.URI), e)
          }
        }
        Map(adapter -> isConsistent)
    }
  }
}