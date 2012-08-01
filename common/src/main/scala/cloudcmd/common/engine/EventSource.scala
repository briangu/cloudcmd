package cloudcmd.common.engine

trait EventSource {

  protected var _listeners : List[EngineEventListener] = List()

  def registerListener(listener: EngineEventListener) {
    _listeners = _listeners ++ List(listener)
  }

  protected def onMessage(msg: String) {
    _listeners.foreach(_.onMessage(msg))
  }
}
