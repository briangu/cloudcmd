package cloudcmd.common.engine

import cloudcmd.common.adapters.DirectAdapter
import java.io.InputStream
import cloudcmd.common.BlockContext

trait ReplicationStrategy {
  def isReplicated(ctx: BlockContext, adapters: List[DirectAdapter]): Boolean

  def store(ctx: BlockContext, is: InputStream, adapters: List[DirectAdapter])

  def load(ctx: BlockContext, adapters: List[DirectAdapter]): (InputStream, Int)

  def remove(ctx: BlockContext, adapters: List[DirectAdapter]) : Boolean

  def ensure(ctx: BlockContext, hashProviders: List[DirectAdapter], adapters: List[DirectAdapter], blockLevelCheck: Boolean) : Boolean
}