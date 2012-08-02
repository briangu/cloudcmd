package cloudcmd.common.engine

import cloudcmd.common._
import adapters.Adapter
import java.io._
import org.apache.log4j.Logger
import config.ConfigStorage
import util.FileMetaData

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

  def refreshAdapterCaches() {
    _adapters.par.foreach(_.refreshCache())
  }

  def getMetaHashSet() : Set[String] = {
    Set() ++ _adapters.flatMap(a => a.describe.toSet).par.filter(hash => hash.endsWith(".meta"))
  }

  def getAdaptersAccepts(fmd: FileMetaData) : List[Adapter] = {
    _adapters.par.filter(_.accepts(fmd.getTags)).toList
  }

  def getHashProviders(hash: String) : List[Adapter] = {
    _adapters.par.filter(_.contains(hash)).toList
  }

  def sync(hash: String, fmd: FileMetaData) {
    syncAll(Map(hash -> fmd))
  }

  def syncAll(hashes : Map[String, FileMetaData]) {
    hashes.par.foreach{ case (hash, fmd) => _storage.sync(hash, getHashProviders(hash), getAdaptersAccepts(fmd)) }
  }

  def verify(hash: String, fmd: FileMetaData, deleteOnInvalid: Boolean) {
    verifyAll(Map(hash -> fmd), deleteOnInvalid)
  }

  def verifyAll(hashes: Map[String, FileMetaData], deleteOnInvalid: Boolean) {
    hashes.par.foreach{ case (hash, fmd) => _storage.verify(hash, getAdaptersAccepts(fmd), deleteOnInvalid) }
  }

  def store(hash: String, is: InputStream, fmd: FileMetaData) {
    _storage.store(hash, is, getAdaptersAccepts(fmd))
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