package cloudcmd.common.engine

import cloudcmd.common.ContentAddressableStorage

trait CloudEngine extends ContentAddressableStorage with EventSource {

  def init()
  def run()
  def shutdown()

  def filterAdapters(minTier: Int, maxTier: Int) // TODO: move to init?
}