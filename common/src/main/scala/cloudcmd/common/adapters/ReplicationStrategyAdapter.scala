package cloudcmd.common.adapters

import cloudcmd.common._
import java.io._
import cloudcmd.common.engine.ReplicationStrategy
import org.json.JSONObject
import scala.collection.mutable

class ReplicationStrategyAdapter(adapters: List[IndexedAdapter], storage: ReplicationStrategy) extends IndexedContentAddressableStorage {

  def getAdaptersAccepts(ctx: BlockContext) : List[IndexedAdapter] = {
    adapters.filter(_.accepts(ctx)).toList
  }

  private def getHashProviders(ctx: BlockContext) : List[IndexedAdapter] = {
    adapters.par.filter(_.contains(ctx)).toList
  }

  def describe() : Set[String] = {
    Set() ++ adapters.par.flatMap(a => a.describe().toSet)
  }

  override def contains(ctx: BlockContext) : Boolean = {
    adapters.find(_.contains(ctx)) != None
  }

  def containsAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ ctx => Map(ctx -> contains(ctx)) }
  }

  override def ensure(ctx: BlockContext, blockLevelCheck: Boolean) : Boolean = {
    try {
      storage.ensure(ctx, getHashProviders(ctx), getAdaptersAccepts(ctx), blockLevelCheck)
    } catch {
      case e: DataNotFoundException => {
        false
      }
    }
  }

  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean) : Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap( ctx =>
      Map(ctx -> storage.ensure(ctx, getHashProviders(ctx), getAdaptersAccepts(ctx), blockLevelCheck))
    )
  }

  def store(ctx: BlockContext, is: InputStream) {
    storage.store(ctx, is, getAdaptersAccepts(ctx))
  }

  def load(ctx: BlockContext) : (InputStream, Int) = {
    storage.load(ctx, getHashProviders(ctx))
  }

  def removeAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap( ctx => Map(ctx -> storage.remove(ctx, getHashProviders(ctx))) )
  }

  /** *
    * Refresh the storage index, which may be time consuming
    */
  def reindex(cas: ContentAddressableStorage) {
    // sort in ascending tier so that the lower tiered adapters can provide for the higher tiers
    adapters groupBy(x => x.Tier) foreach {
      case (tier: Int, group: List[IndexedAdapter]) => {
        group.par foreach {
          adapter => adapter.reindex(cas)
        }
      }
    }
  }

  /** *
    * Flush the index cache that may be populated during a series of modifications (e.g. store)
    */
  def flushIndex() {
    adapters.par.foreach(_.flushIndex())
  }

  /**
   * Find a set of meta blocks based on a filter.
   * @param filter
   * @return a set of meta blocks
   */
  def find(filter: JSONObject): Set[FileMetaData] = {
    val results = new mutable.HashSet[FileMetaData] with mutable.SynchronizedSet[FileMetaData]
    adapters.par.foreach { adapter =>
      adapter.find(filter) foreach { results.add }
    }
    results.toSet
  }
}