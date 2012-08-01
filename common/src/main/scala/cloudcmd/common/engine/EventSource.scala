package cloudcmd.common.engine

trait EventSource {

  protected var _listeners : List[CloudEngineListener] = List()

  def registerListener(listener: CloudEngineListener) {
    _listeners = _listeners ++ List(listener)
  }

  protected def onMessage(msg: String) {
    _listeners.foreach(_.onMessage(msg))
  }
}
