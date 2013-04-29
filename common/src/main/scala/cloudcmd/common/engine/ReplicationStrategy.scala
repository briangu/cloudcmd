package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import java.io.InputStream

trait ReplicationStrategy extends EventSource {
  def isReplicated(hash: String, adapters: List[Adapter]): Boolean

  def store(hash: String, is: InputStream, adapters: List[Adapter])

  def load(hash: String, adapters: List[Adapter]): (InputStream, Int)

  def remove(hash: String, adapters: List[Adapter]) : Boolean

  def ensure(hash: String, hashProviders: List[Adapter], adapters: List[Adapter], blockLevelCheck: Boolean) : Boolean
}