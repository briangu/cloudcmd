package cloudcmd.common.engine

trait CloudEngineListener {
  def onMessage(msg: String)
}