package cloudcmd.common.engine

trait EngineEventListener {
  def onMessage(msg: String)
}