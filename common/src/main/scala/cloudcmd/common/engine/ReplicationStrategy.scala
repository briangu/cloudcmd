package cloudcmd.common.engine

import cloudcmd.common.adapters.Adapter
import java.io.InputStream
import cloudcmd.common.BlockContext

trait ReplicationStrategy extends EventSource {
  def isReplicated(ctx: BlockContext, adapters: List[Adapter]): Boolean

  def store(ctx: BlockContext, is: InputStream, adapters: List[Adapter])

  def load(ctx: BlockContext, adapters: List[Adapter]): (InputStream, Int)

  def remove(ctx: BlockContext, adapters: List[Adapter]) : Boolean

  def ensure(ctx: BlockContext, hashProviders: List[Adapter], adapters: List[Adapter], blockLevelCheck: Boolean) : Boolean
}