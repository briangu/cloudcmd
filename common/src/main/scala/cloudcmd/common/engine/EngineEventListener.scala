package cloudcmd.common.engine

trait EngineEventListener {
  def onMessage(msg: String)
}

trait EventSource {

  protected var _listeners : List[EngineEventListener] = List()

  def registerListener(listener: EngineEventListener) {
    _listeners = _listeners ++ List(listener)
  }

  protected def onMessage(msg: String) {
    _listeners.foreach(_.onMessage(msg))
  }
}
