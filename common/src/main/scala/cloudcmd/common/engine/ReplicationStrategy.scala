package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import java.io.InputStream

abstract trait ReplicationStrategy {
  def isReplicated(adapters: Set[Adapter], hash: String): Boolean

  def push(listener: CloudEngineListener, adapters: Set[Adapter], hash: String, hashProviders: List[Adapter])

  def load(hash: String, hashProviders: List[Adapter]): InputStream
}