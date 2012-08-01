package cloudcmd.common.engine

import cloudcmd.common.FileUtil
import cloudcmd.common.adapters.{DataNotFoundException, Adapter}
import org.apache.log4j.Logger
import java.io.InputStream
import util.Random
import java.util.concurrent.atomic.AtomicInteger

class MirrorReplicationStrategy extends ReplicationStrategy {

  private val log: Logger = Logger.getLogger(classOf[MirrorReplicationStrategy])

  private var _listener: CloudEngineListener = null

  def registerListener(listener: CloudEngineListener) {
    _listener = listener
  }

  private def onMessage(msg: String) {
    if (_listener != null) _listener.onMessage(msg)
  }

  def isReplicated(hash: String, adapters: List[Adapter]): Boolean = {
    adapters.filterNot(_.describe.contains(hash)).size > 0
  }

  def sync(hash: String, hashProviders: List[Adapter], adapters: List[Adapter]) {
    var is: InputStream = null
    try {
      is = load(hash, hashProviders)
      store(hash, is, adapters)
    }
    catch {
      case e: DataNotFoundException => {
        onMessage(String.format("no adapter has block %s"))
        log.error(hash, e)
      }
      case e: Exception => {
        onMessage(String.format("failed to sync block %s", hash))
        log.error(hash, e)
      }
    }
    finally {
      FileUtil.SafeClose(is)
    }
  }

  def contains(hash: String, adapters: List[Adapter]) : Boolean = {
    adapters.filter(_.describe.contains(hash)).size > 0
  }

  def store(hash: String, is: InputStream, adapters: List[Adapter]) {
    var containsAdapters = adapters.par.filter(_.describe.contains(hash)).toList
    if (containsAdapters.size == 0) {
      adapters(0).store(is, hash)
      containsAdapters = containsAdapters ++ List(adapters(0))
    }

    val missingAdapters = adapters -- containsAdapters
    val pushedCount = new AtomicInteger(containsAdapters.size)

    missingAdapters.par.foreach{ adapter =>
      var is: InputStream = null
      try {
        is = load(hash, containsAdapters)
        adapter.store(is, hash)
        pushedCount.incrementAndGet()
        containsAdapters = containsAdapters ++ List(adapter)
      }
      catch {
        case e: Exception => {
          onMessage(String.format("failed to sync block %s to %s", hash, adapter.URI.toString))
          log.error(hash, e)
        }
      }
      finally {
        FileUtil.SafeClose(is)
      }
    }

    if (pushedCount.get() != adapters.size) {
      onMessage("failed to store block %s on %d of %d adapters".format(hash, pushedCount.get, adapters.size))
    }
  }

  def load(hash: String, hashProviders: List[Adapter]): InputStream = {
    if (hashProviders.size == 0) throw new DataNotFoundException(hash)
    Random.shuffle(hashProviders).sortBy(x => x.Tier).toList(0).load(hash)
  }

  def remove(hash: String, hashProviders: List[Adapter]) {
    hashProviders.par.foreach {
      adapter =>
        try {
          val deleteSuccess = adapter.remove(hash)
          if (deleteSuccess) {
            onMessage(String.format("successfully deleted block %s found on adapter %s", hash, adapter.URI))
          } else {
            onMessage(String.format("failed to delete block %s found on adapter %s", hash, adapter.URI))
          }
        } catch {
          case e: Exception => {
            onMessage(String.format("failed to delete block %s on adapter %s", hash, adapter.URI))
            log.error(hash, e)
          }
        }
    }
  }

  def verify(hash: String, hashProviders: List[Adapter], deleteOnInvalid: Boolean) : Boolean = {
    var valid = false

    val replicated = isReplicated(hash, hashProviders)
    if (!replicated) {
      onMessage(String.format("block %s is not fully replicated", hash))
    }

    hashProviders.par.foreach {
      adapter =>
        try {
          val isValid = adapter.verify(hash)
          if (isValid) {
            // TODO: enable verbose flag
            //_wm.make(new MemoryElement("msg", "body", String.format("successfully validated block %s is on adapter %s", hash, adapter.URI)))
          } else {
            valid = false

            onMessage(String.format("bad block %s found on adapter %s", hash, adapter.URI))
            if (deleteOnInvalid) {
              try {
                val deleteSuccess = adapter.remove(hash)
                if (deleteSuccess) {
                  onMessage(String.format("successfully deleted block %s found on adapter %s", hash, adapter.URI))
                } else {
                  onMessage(String.format("failed to delete block %s found on adapter %s", hash, adapter.URI))
                }
              } catch {
                case e: Exception => {
                  onMessage(String.format("failed to delete block %s on adapter %s", hash, adapter.URI))
                  log.error(hash, e)
                }
              }
            }
          }
        } catch {
          case e: Exception => {
            onMessage(String.format("failed to verify block %s on adapter %s", hash, adapter.URI))
            log.error(hash, e)
          }
        }
    }

    valid && replicated
  }
}