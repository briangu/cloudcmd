package cloudcmd.common.engine

import cloudcmd.common.FileUtil
import cloudcmd.common.adapters.Adapter
import org.apache.log4j.Logger
import java.io.InputStream
import util.Random
import java.util.concurrent.atomic.AtomicInteger

class MirrorReplicationStrategy extends ReplicationStrategy {

  private[engine] var log: Logger = Logger.getLogger(classOf[MirrorReplicationStrategy])

  def isReplicated(adapters: Set[Adapter], hash: String): Boolean = {
    adapters.filterNot(_.describe.contains(hash)).size > 0
  }

  def push(listener: CloudEngineListener, adapters: Set[Adapter], hash: String, hashProviders: List[Adapter]) {
    val pushedCount = new AtomicInteger

    adapters.par.foreach{ adapter =>
      if (adapter.describe.contains(hash)) {
        pushedCount.incrementAndGet()
      } else {
        var is: InputStream = null
        try {
          is = load(hash, hashProviders)
          if (is != null) {
            adapter.store(is, hash)
            pushedCount.incrementAndGet()
          } else {
            listener.onMessage(String.format("no adapter has block %s"))
          }
        }
        catch {
          case e: Exception => {
            listener.onMessage(String.format("failed to push block %s to %s", hash, adapter.URI.toString))
            log.error(hash, e)
          }
        }
        finally {
          FileUtil.SafeClose(is)
        }
      }
    }

    if (pushedCount.get() != adapters.size) {
      listener.onMessage("failed to push block: " + hash)
    }
  }

  // TODO: this should provide an iterator over a list of streams that we can interrupt once we succeed
  def load(hash: String, hashProviders: List[Adapter]): InputStream = {
    if (hashProviders.contains(hash)) {
      val adapters = Random.shuffle(hashProviders).sortBy(x => x.Tier)
      if (adapters.size > 0) {
        adapters(0).load(hash)
      } else {
        null
      }
    } else {
      null
    }
  }
}