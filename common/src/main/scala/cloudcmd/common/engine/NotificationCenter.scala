package cloudcmd.common.engine

import scala.collection.mutable
import java.util.concurrent.SynchronousQueue

object NotificationCenter {

  val _default = new NotificationCenter

  def defaultCenter: NotificationCenter = {
    _default
  }

}

class NotificationCenter {

  case class Message(name: String, source: Option[Any], userInfo: Option[Map[String, Any]])

  val event: Array[Boolean] = new Array[Boolean](1)
  event(0) = false

  val queue = new SynchronousQueue[Message]

  val msgPump: Thread = new Thread(new Runnable {
    def run() {
      while (!event(0)) {
        try {
          val msg = queue.take
          _center.get(msg.name) match {
            case Some(subscription) => {
              subscription.postNotification(msg.source, msg.userInfo)
            }
          }
          System.err.println(msg)
        }
        catch {
          case e: InterruptedException => ;
        }
      }
    }
  })

  class Subscriptions(name: String) {

    case class Subscription(observer: Any, source: Any, fn: Option[Map[String, Any]] => Unit)

    var observers = new mutable.HashSet[Subscription] with mutable.SynchronizedSet[Subscription]

    def removeObserver(observer: Any, source: Option[Any]) {
      val toRemove = source match {
        case Some(_) => {
          observers filter { s => s.observer == observer && s.source == source }
        }
        case None => {
          observers filter { _.observer == observer }
        }
      }
      toRemove.foreach(observers.remove)
    }

    def addObserver(observer: Any, source: Option[Any], fn: Option[Map[String, Any]] => Unit) {
      observers.add(new Subscription(observer, source, fn))
    }

    def postNotification(source: Option[Any] = None, userInfo: Option[Map[String, Any]] = None) {
      source match {
        case Some(_) => {
          observers filter { _.source == source } foreach { observer => observer.fn(userInfo) }
        }
        case None => {
          observers foreach { observer => observer.fn(userInfo) }
        }
      }
    }

  }

  val _center = new mutable.HashMap[String, Subscriptions] with mutable.SynchronizedMap[String, Subscriptions]

  def removeObserverForName(observer: Any, name: String, source: Option[Any]) {
    _center.get(name) match {
      case Some(subscription) => {
        subscription.removeObserver(observer, source)
      }
      case None => ;
    }
  }

  def removeObserver(observer: Any) {
    _center.values.foreach(_.removeObserver(observer, None))
  }

  def addObserverForName(observer: Any, name: String, source: Option[Any], fn: Option[Map[String, Any]] => Unit) {
    _center.get(name) match {
      case Some(subscription) => {
        subscription.addObserver(observer, source, fn)
      }
      case None => {
        val subscription = new Subscriptions(name)
        _center.put(name, subscription)
        subscription.addObserver(observer, source, fn)
      }
    }
  }

  def postNotification(name: String, source: Option[Any] = None, userInfo: Option[Map[String, Any]] = None) {
    queue.add(new Message(name, source, userInfo))
  }

}
