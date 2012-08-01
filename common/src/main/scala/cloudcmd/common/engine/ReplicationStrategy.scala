package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import java.io.InputStream

trait ReplicationStrategy {
  def isReplicated(hash: String, adapters: List[Adapter]): Boolean

  def sync(hash: String, hashProviders: List[Adapter], adapters: List[Adapter])

  def store(hash: String, is: InputStream, adapters: List[Adapter])

  def load(hash: String, adapters: List[Adapter]): InputStream

  def remove(hash: String, adapters: List[Adapter])

  def verify(hash: String, adapters: List[Adapter], deleteOnInvalid: Boolean) : Boolean

  def registerListener(listener: CloudEngineListener)
}