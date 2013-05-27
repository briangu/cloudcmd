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

    val containsAdapters = adapters.filter(_.contains(ctx))
    val missingAdapters = adapters.diff(containsAdapters)

    if (containsAdapters.size == 0) {
      if (missingAdapters.size == 1) {
        storeSingleStream(ctx, dis, adapters)
      } else {
        storeViaMultiStreamBootstrap(ctx, dis, adapters)
      }
    } else {
      storeViaTierClusters(ctx, adapters)
    }
  }

  def storeSingleStream(ctx: BlockContext, dis: InputStream, adapters: List[IndexedAdapter]) {
    val adapter = adapters(0)
    if (!adapter.contains(ctx)) {
      try {
        log.debug("storing %s to adapter %s".format(ctx.getId(), adapter.getSignature))
        adapter.store(ctx, dis)
      }
      catch {
        case e: Exception => {
          log.error(String.format("failed to sync block %s to %s", ctx, adapter.getSignature), e)
          throw new MultiWriteBlockException(ctx, adapters, List(), List(adapter))
        }
      }
    }
  }

  def storeViaMultiStreamBootstrap(ctx: BlockContext, dis: InputStream, adapters: List[IndexedAdapter]) {
    val containsAdapters = adapters.filter(_.contains(ctx))
    val missingAdapters = adapters.diff(containsAdapters).sortBy(_.Tier).toList

    if (missingAdapters.size > 0) {
      if (containsAdapters.size == 0) {
        try {
          log.debug("storing %s to adapter %s".format(ctx.getId(), adapters(0).getSignature))
          missingAdapters(0).store(ctx, dis)
        } catch {
          case e: Exception => {
            if (containsAdapters.size == 0) {
              throw new MultiWriteBlockException(ctx, adapters, List(), List(missingAdapters(0)))
            }
          }
        }
      }

      storeViaTierClusters(ctx, adapters)
    }
  }

  def storeViaTierClusters(ctx: BlockContext, adapters: List[IndexedAdapter]) {
    val containsAdapters = adapters.filter(_.contains(ctx))
    val missingAdapterGroups = adapters.diff(containsAdapters).sortBy(_.Tier).groupBy(_.Tier)

    missingAdapterGroups foreach {
      case (tier, missingAdapters) =>
        val containsTierAdapters = adapters.filter(_.contains(ctx)).filter(_.Tier <= tier)
        storeViaFanFold(ctx, containsTierAdapters, missingAdapters)
    }
  }

  def storeViaFanFold(ctx: BlockContext, containsAdapters: List[IndexedAdapter], missingAdapters: List[IndexedAdapter]) {
    if (containsAdapters.size == 0) {
      throw new IllegalArgumentException("containsAdapters.size == 0")
    }

    var failedAdapters = List[IndexedAdapter]()

    val pushedCount = new AtomicInteger()

    var currentContainsAdapters = containsAdapters
    var currentMissingAdapters = missingAdapters

    while (currentMissingAdapters.size > 0) {
      val foldSize = Math.min(currentContainsAdapters.size, currentMissingAdapters.size)
      (0 until foldSize).par foreach { idx =>
        val srcAdapter = currentContainsAdapters(idx)
        val destAdapter = currentMissingAdapters(idx)

        val is: InputStream = srcAdapter.load(ctx)._1
        try {
          log.debug("storing %s to adapter %s".format(ctx.getId(), destAdapter.getSignature))
          destAdapter.store(ctx, is)
          pushedCount.incrementAndGet()
          currentMissingAdapters = currentMissingAdapters.diff(List(destAdapter))
          currentContainsAdapters = currentContainsAdapters ++ List(destAdapter)
        }
        catch {
          case e: Exception => {
            log.error(String.format("failed to sync block %s to %s", ctx, String.valueOf(destAdapter.URI)), e)
            failedAdapters = failedAdapters ++ List(destAdapter)
          }
        }
        finally {
          FileUtil.SafeClose(is)
        }
      }
    }

    if (pushedCount.get() != missingAdapters.size) {
      val missingCount = missingAdapters.size - pushedCount.get()
      log.error("failed to store block %s on %d of %d adapters".format(ctx, missingCount, missingAdapters.size))
      val successAdapters = currentContainsAdapters.diff(containsAdapters)
      throw new MultiWriteBlockException(ctx, containsAdapters, successAdapters, failedAdapters)
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
    } catch {
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
            log.debug(String.format("block %s valid on adapter %s", ctx, adapter.URI))
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