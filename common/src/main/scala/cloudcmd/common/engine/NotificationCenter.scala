package cloudcmd.common.engine

import scala.collection.mutable
import java.util.concurrent.SynchronousQueue
import org.apache.log4j.Logger

object NotificationCenter {

  val _default = new NotificationCenter

  def start() {
    _default.start()
  }

  def shutdown() {
    _default.shutdown()
  }

  def defaultCenter: NotificationCenter = {
    _default
  }
}

class NotificationCenter {

  private val log = Logger.getLogger(classOf[NotificationCenter])

  private case class MessageCommand(name: String, source: Option[Any], userInfo: Option[Map[String, Any]])
  private case class AddObserver(observer: Any, name: String, source: Option[Any], fn: Option[Map[String, Any]] => Unit)
  private case class RemoveObserver(observer: Any, name: Option[String], source: Option[Any])

  private val event: Array[Boolean] = new Array[Boolean](1)
  private val queue = new SynchronousQueue[Any]

  private val operationPump: Thread = new Thread(new Runnable {
    def run() {
      while (!event(0)) {
        try {
          queue.take match {
            case MessageCommand(name, sourceOption, userInfoOption) => {
              _center.get(name) match {
                case Some(subscription) => {
                  subscription.postNotification(sourceOption, userInfoOption)
                }
                case None => {
                  log.warn("notification name not found: %s".format(name))
                }
              }
            }
            case AddObserver(observer, name, sourceOption, fn) => {
              _center.get(name) match {
                case Some(subscriptions) => {
                  subscriptions.addObserver(observer, sourceOption, fn)
                }
                case None => {
                  val subscriptions = new Subscriptions(name)
                  _center.put(name, subscriptions)
                  subscriptions.addObserver(observer, sourceOption, fn)
                }
              }
            }
            case RemoveObserver(observer, nameOption, sourceOption) => {
              nameOption match {
                case Some(name) => {
                  _center.get(name) match {
                    case Some(subscriptions) => {
                      subscriptions.removeObserver(observer, sourceOption)
                      if (subscriptions.isEmpty) {
                        _center.remove(name)
                      }
                    }
                    case None => ;
                  }
                }
                case None => {
                  val namesToRemove = _center.values flatMap {
                    subscriptions => {
                      subscriptions.removeObserver(observer, sourceOption)
                      if (subscriptions.isEmpty) {
                        List(subscriptions.name)
                      } else {
                        Nil
                      }
                    }
                  }
                  namesToRemove.foreach(_center.remove)
                }
              }
            }
          }
        }
        catch {
          case e: InterruptedException => ;
          case e: Exception => {
            log.error(e)
          }
        }
      }
    }
  })

  def start() {
    event(0) = false
    operationPump.start()
  }

  def shutdown() {
    event(0) = true
    operationPump.interrupt()
    operationPump.join()
  }

  private class Subscriptions(val name: String) {

    private case class Subscription(observer: Any, sourceOption: Option[Any], fn: Option[Map[String, Any]] => Unit)

    private var observers = Set[Subscription]()

    def isEmpty: Boolean = {
      observers.isEmpty
    }

    def removeObserver(observer: Any, sourceOption: Option[Any]) {
      sourceOption match {
        case Some(_) => {
          observers = observers filter { s => s.observer != observer && s.sourceOption != sourceOption }
        }
        case None => {
          observers = observers filter { _.observer != observer }
        }
      }
    }

    def addObserver(observer: Any, sourceOption: Option[Any], fn: Option[Map[String, Any]] => Unit) {
      observers = observers ++ Set(new Subscription(observer, sourceOption, fn))
    }

    def postNotification(sourceOption: Option[Any] = None, userInfo: Option[Map[String, Any]] = None) {
      observers filter {
        observer => observer.sourceOption == sourceOption || observer.sourceOption == None
      } foreach {
        observer => observer.fn(userInfo)
      }
    }
  }

  private val _center = new mutable.HashMap[String, Subscriptions]

  def removeObserver(observer: Any, nameOption: Option[String], sourceOption: Option[Any]) {
    queue.add(new RemoveObserver(observer, nameOption, sourceOption))
  }

  def addObserverForName(observer: Any, name: String, sourceOption: Option[Any], fn: (Option[Map[String, Any]]) => Unit) {
    queue.add(new AddObserver(observer, name, sourceOption, fn))
  }

  def postNotification(name: String, sourceOption: Option[Any] = None, userInfo: Option[Map[String, Any]] = None) {
    queue.add(new MessageCommand(name, sourceOption, userInfo))
  }

  def postNotification(name: String, source: AnyRef) {
    queue.add(new MessageCommand(name, Option(source), None))
  }

  def postNotification(name: String, source: AnyRef, userInfo: Map[String, Any]) {
    queue.add(new MessageCommand(name, Option(source), Option(userInfo)))
  }
}
