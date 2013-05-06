package cloudcmd.common.engine

import cloudcmd.common.adapters.IndexedAdapter
import java.io.InputStream
import cloudcmd.common.BlockContext

trait ReplicationStrategy extends EventSource {
  def isReplicated(ctx: BlockContext, adapters: List[IndexedAdapter]): Boolean

  def store(ctx: BlockContext, is: InputStream, adapters: List[IndexedAdapter])

  def load(ctx: BlockContext, adapters: List[IndexedAdapter]): (InputStream, Int)

  def remove(ctx: BlockContext, adapters: List[IndexedAdapter]) : Boolean

  def ensure(ctx: BlockContext, hashProviders: List[IndexedAdapter], adapters: List[IndexedAdapter], blockLevelCheck: Boolean) : Boolean
}