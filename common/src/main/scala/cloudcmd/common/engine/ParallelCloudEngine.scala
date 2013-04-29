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

  def run() {}

  def shutdown() {}

  def filterAdapters(minTier: Int, maxTier: Int) {
    _adapters = configService.getAdapters.filter(a => a.Tier >= minTier && a.Tier <= maxTier && a.IsOnLine() && !a.IsFull()).toList
  }

  def describeMeta() : Set[String] = {
    Set() ++ _adapters.par.flatMap(a => a.describe().filter(_.endsWith(".meta")).toSet)
  }

  def getAdaptersAccepts(hash: String) : List[Adapter] = {
    _adapters.filter(_.accepts(hash)).toList
  }

  private def getHashProviders(hash: String) : List[Adapter] = {
    _adapters.par.filter(_.contains(hash)).toList
  }

  def refreshCache() {
    _adapters.par.foreach(_.refreshCache())
  }

  def describe() : Set[String] = {
    Set() ++ _adapters.par.flatMap(a => a.describe().toSet)
  }

  override def contains(hash: String) : Boolean = {
    _adapters.find(_.contains(hash)) != None
  }

  def containsAll(hashes: Set[String]) : Map[String, Boolean] = {
    Map() ++ hashes.par.flatMap{ hash => Map(hash -> contains(hash)) }
  }

  override
  def ensure(hash: String, blockLevelCheck: Boolean) : Boolean = {
    _storage.ensure(hash, getHashProviders(hash), getAdaptersAccepts(hash), blockLevelCheck)
  }

  def ensureAll(hashes: Set[String], blockLevelCheck: Boolean) : Map[String, Boolean] = {
    Map() ++ hashes.par.flatMap( hash =>
      Map(hash -> _storage.ensure(hash, getHashProviders(hash), getAdaptersAccepts(hash), blockLevelCheck))
    )
  }

  def store(hash: String, is: InputStream) {
    _storage.store(hash, is, getAdaptersAccepts(hash))
  }

  def load(hash: String) : (InputStream, Int) = {
    _storage.load(hash, getHashProviders(hash))
  }

  def removeAll(hashes: Set[String]) : Map[String, Boolean] = {
    Map() ++ hashes.par.flatMap( hash => Map(hash -> _storage.remove(hash, getHashProviders(hash))) )
  }
}