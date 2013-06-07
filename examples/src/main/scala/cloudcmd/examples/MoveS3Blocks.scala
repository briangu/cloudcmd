package cloudcmd.examples

import cloudcmd.common.adapters.DirectS3Adapter
import java.net.URI
import cloudcmd.common.BlockContext

object MoveS3Blocks {

  def main(args: Array[String]) {
    if (args.size != 2) {
      println("mvblocks <src adapter uri> <dest adapter uri>")
      return
    }

    val srcAdapter = new DirectS3Adapter()
    val destAdapter = new DirectS3Adapter()
    val adapters = List(srcAdapter, destAdapter)

    try {
      srcAdapter.init(".", 1, "", Set(), new URI(args(0)))
      if (!srcAdapter.IsOnLine) {
        println("src adapter %s is not online!".format(srcAdapter.getSignature))
        return
      }

      destAdapter.init(".", 1, "", Set(), new URI(args(1)))
      if (!destAdapter.IsOnLine) {
        println("dest adapter %s is not online!".format(destAdapter.getSignature))
        return
      }

      val srcBlocks = srcAdapter.describe()
      println("src has %d blocks".format(srcBlocks.size))
      val destBlocks = destAdapter.describe()
      println("dest has %d blocks".format(destBlocks.size))
      val missingDestBlocks = srcBlocks.diff(destBlocks)
      println("moving %d blocks...".format(missingDestBlocks.size))
      missingDestBlocks foreach { srcBlock =>
        val ctx = new BlockContext(srcBlock)
        try {
          println("copying block %s to destination".format(ctx))
          destAdapter.store(ctx, srcAdapter.load(ctx)._1)
          println("removing block %s from source".format(ctx))
          srcAdapter.remove(ctx)
        } catch {
          case e: Exception => {
            println("failed to move block: %s".format(ctx))
          }
        }
      }
    } finally {
      adapters.foreach(_.shutdown())
    }
  }
}
