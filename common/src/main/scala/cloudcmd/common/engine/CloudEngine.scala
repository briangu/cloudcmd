package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import java.io.InputStream
import cloudcmd.common.util.FileMetaData

trait CloudEngine extends EventSource {

  def init()
  def run()
  def shutdown()

  def filterAdapters(minTier: Int, maxTier: Int)

  def sync(hash : String, fmd: FileMetaData)
  def syncAll(hashes : Map[String, FileMetaData])

  def verify(hash: String, fmd: FileMetaData, deleteOnInvalid: Boolean)
  def verifyAll(hashes: Map[String, FileMetaData], deleteOnInvalid: Boolean)

  def load(hash: String) : InputStream

  def store(hash: String, is: InputStream, fmd: FileMetaData)

  def remove(hash: String)
  def removeAll(hashes: Set[String])

  def getHashProviders(hash: String) : List[Adapter]
  def getMetaHashSet() : Set[String]
  def refreshAdapterCaches()
}