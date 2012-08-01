package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import java.io.InputStream

trait CloudEngine extends EventSource {

  def init()
  def run()
  def shutdown()

  def filterAdapters(minTier: Int, maxTier: Int)

  def sync(hash : String)
  def syncAll(hashes : Set[String])

  def verify(hash: String, deleteOnInvalid: Boolean)
  def verifyAll(hashes: Set[String], deleteOnInvalid: Boolean)

  def load(hash: String) : InputStream

  def store(hash: String, is: InputStream)

  def remove(hash: String)
  def removeAll(hashes: Set[String])

  def getHashProviders(): Map[String, List[Adapter]]
  def getHashProviders(hash: String) : List[Adapter]
  def getMetaHashSet() : Set[String]
  def refreshAdapterCaches()
}