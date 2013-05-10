package cloudcmd.common.engine

import scala.collection.mutable

object NotificationCenter {

  val _default = new NotificationCenter

  def defaultCenter: NotificationCenter = {
    _default
  }

}

class NotificationCenter {

  class Subscription(name: String, source: Any) {

    def removeObserverForSource(observer: Any, source: Any) {

    }

    def removeObserver(observer: Any) {

    }

    def addObserverForSource(observer: Any, source: Any) {

    }

    def addObserver(observer: Any) {

    }

  }

  // name -> (name, obj) -> subscribers

  val _center = new mutable.HashMap[String, Subscription] with mutable.SynchronizedMap[String, Subscription]

  def removeObserverForName(observer: Any, name: String, source: Any) {
    _center.get(name) match {
      case Some(subscription) => {
        subscription.removeObserverForSource(observer, source)
      }
      case None => ;
    }
  }

  def removeObserver(observer: Any) {
    _center.values.foreach(_.removeObserver(observer))
  }

  def addObserverForName(observer: Any, name: String, source: Option[Any]) {
    _center.get(name) match {
      case Some(subscription) => {
        source match {
          case Some(_) => {
            subscription.addObserverForSource(observer, source)
          }
          case None => {
            subscription.addObserver(observer)
          }
        }
      }
    }
  }

  def postNotification(name: String, source: Option[Any] = None, userInfo: Option[Map[String, Any]] = None) {

  }

}
