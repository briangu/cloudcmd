package cloudcmd.common.engine

import cloudcmd.common._
import adapters.Adapter
import java.io._
import org.apache.log4j.Logger
import config.ConfigStorage

class ParallelCloudEngine(configService: ConfigStorage) extends CloudEngine {

  private val log = Logger.getLogger(classOf[ParallelCloudEngine])

  private var _storage : ReplicationStrategy = null
  private var _adapters : List[Adapter] = null

  def init() {
    _storage = configService.getReplicationStrategy
    _adapters = configService.getAdapters
  }

  def run {}

  def shutdown {}

  def filterAdapters(minTier: Int, maxTier: Int) {
    _adapters = configService.getAdapters.filter(a => a.Tier >= minTier && a.Tier <= maxTier && a.IsOnLine() && !a.IsFull()).toList
  }

  def describeMeta() : Set[BlockContext] = {
    Set() ++ _adapters.par.flatMap(a => a.describe.filter(_.hash.endsWith(".meta")).toSet)
  }

  def getAdaptersAccepts(ctx: BlockContext) : List[Adapter] = {
    _adapters.filter(_.accepts(ctx)).toList
  }

  private def getHashProviders(ctx: BlockContext) : List[Adapter] = {
    _adapters.par.filter(_.contains(ctx)).toList
  }

  def refreshCache() {
    _adapters.par.foreach(_.refreshCache())
  }

  def describe() : Set[BlockContext] = {
    Set() ++ _adapters.par.flatMap(a => a.describe.toSet)
  }

  def describeHashes() : Set[String] = {
    Set() ++ _adapters.par.flatMap(a => a.describeHashes.toSet)
  }

  override def contains(ctx: BlockContext) : Boolean = {
    _adapters.find(_.contains(ctx)) != None
  }

  def containsAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ ctx => Map(ctx -> contains(ctx)) }
  }

  override
  def ensure(ctx: BlockContext, blockLevelCheck: Boolean) : Boolean = {
    _storage.ensure(ctx, getHashProviders(ctx), getAdaptersAccepts(ctx), blockLevelCheck)
  }

  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean) : Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap( ctx =>
      Map(ctx -> _storage.ensure(ctx, getHashProviders(ctx), getAdaptersAccepts(ctx), blockLevelCheck))
    )
  }

  def store(ctx: BlockContext, is: InputStream) {
    _storage.store(ctx, is, getAdaptersAccepts(ctx))
  }

  def load(ctx: BlockContext) : (InputStream, Int) = {
    _storage.load(ctx, getHashProviders(ctx))
  }

  def removeAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap( ctx => Map(ctx -> _storage.remove(ctx, getHashProviders(ctx))) )
  }
}