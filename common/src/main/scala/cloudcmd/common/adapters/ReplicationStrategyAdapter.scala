package cloudcmd.common.adapters

import cloudcmd.common._
import java.io._
import cloudcmd.common.engine.ReplicationStrategy

class ReplicationStrategyAdapter(adapters: List[Adapter], storage: ReplicationStrategy) extends ContentAddressableStorage {

  def getAdaptersAccepts(ctx: BlockContext) : List[Adapter] = {
    adapters.filter(_.accepts(ctx)).toList
  }

  private def getHashProviders(ctx: BlockContext) : List[Adapter] = {
    adapters.par.filter(_.contains(ctx)).toList
  }

  def refreshCache() {
    adapters.par.foreach(_.refreshCache())
  }

  def describe() : Set[BlockContext] = {
    Set() ++ adapters.par.flatMap(a => a.describe().toSet)
  }

  def describeHashes() : Set[String] = {
    Set() ++ adapters.par.flatMap(a => a.describeHashes().toSet)
  }

  override def contains(ctx: BlockContext) : Boolean = {
    adapters.find(_.contains(ctx)) != None
  }

  def containsAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ ctx => Map(ctx -> contains(ctx)) }
  }

  override def ensure(ctx: BlockContext, blockLevelCheck: Boolean) : Boolean = {
    storage.ensure(ctx, getHashProviders(ctx), getAdaptersAccepts(ctx), blockLevelCheck)
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
}