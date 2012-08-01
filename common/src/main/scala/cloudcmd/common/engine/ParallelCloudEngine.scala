package cloudcmd.common.engine

import cloudcmd.common._
import adapters.Adapter
import java.io._
import org.apache.log4j.Logger
import config.ConfigStorage

class ParallelCloudEngine extends CloudEngine with EventSource {

  private val log = Logger.getLogger(classOf[ParallelCloudEngine])

  private var _storage: ReplicationStrategy = null
  private var _configService: ConfigStorage = null
  private var _adapters : List[Adapter] = null

  def init(configService: ConfigStorage) {
    import scala.collection.JavaConversions._
    _configService = configService
    _adapters = _configService.getAdapters.toList
    _storage = _configService.getReplicationStrategy
  }

  def run {}
  def shutdown {}

  def filterAdapters(minTier: Int, maxTier: Int) {
    import scala.collection.JavaConversions._
    _adapters = _configService.getAdapters.filter(a => a.Tier >= minTier && a.Tier <= maxTier && a.IsOnLine() && !a.IsFull()).toList
  }

  def refreshAdapterCaches() {
    _adapters.par.foreach(_.refreshCache())
  }

  def getMetaHashSet() : Set[String] = {
    Set() ++ _adapters.flatMap(a => a.describe.toSet).par.filter(hash => hash.endsWith(".meta"))
  }

  def getHashProviders(): Map[String, List[Adapter]] = {
    Map() ++ _adapters.flatMap(a => a.describe.toSet).par.flatMap {
      hash => Map(hash -> _adapters.filter(_.describe().contains(hash)).toList)
    }
  }

  def getHashProviders(hash: String) : List[Adapter] = {
    _adapters.par.filter(_.describe().contains(hash)).toList
  }

  def sync(hash: String) {
    syncAll(Set(hash))
  }

  def syncAll(hashes : Set[String]) {
    hashes.par.filter(_storage.isReplicated(_, _adapters)).par.foreach {
      hash => _storage.sync(hash, getHashProviders(hash), _adapters)
    }
  }

  def verify(hash: String, deleteOnInvalid: Boolean) {
    verifyAll(Set(hash), deleteOnInvalid)
  }

  def verifyAll(hashes: Set[String], deleteOnInvalid: Boolean) {
    hashes.par.foreach(hash => _storage.verify(hash, getHashProviders(hash), deleteOnInvalid))
  }

  def store(hash: String, is: InputStream) {
    _storage.store(hash, is, _adapters.toList)
  }

  def load(hash: String) : InputStream = {
    _storage.load(hash, getHashProviders(hash))
  }

  def remove(hash: String) {
    removeAll(Set(hash))
  }

  def removeAll(hashes: Set[String]) {
    hashes.par.foreach(hash => _storage.remove(hash, getHashProviders(hash)))
  }
}