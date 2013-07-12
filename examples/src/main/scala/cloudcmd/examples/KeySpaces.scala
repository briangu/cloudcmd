package cloudcmd.examples

import cloudcmd.common.config.AdapterFactory
import java.net.URI
import cloudcmd.common.BlockContext
import java.io.{ByteArrayInputStream, File}
import java.util.UUID
import scala.util.Random

object KeySpaces {

  def main(args: Array[String]) {
    if (args.size < 1) {
      println("keyspace <adapter uri> [ownerId]")
      return
    }

    val adapter = AdapterFactory.createDirectAdapter(new URI(args(0)))
    val ownerId = if (args.size >= 2) {
      Some(args(1))
    } else {
      None
    }

    try {
      if (!adapter.IsOnLine) {
        println("adapter %s is not online!".format(adapter.getSignature))
        return
      }

      val data = "hello, world"

      val prefix = Math.abs(Random.nextInt()).toString

      val ctx = new BlockContext("%s_%s".format(prefix, UUID.randomUUID().toString), Set(), ownerId)
      adapter.store(ctx, new ByteArrayInputStream(data.getBytes("UTF-8")))

      val blocks = adapter.describe(ownerId)
      println("adapter has %d blocks".format(blocks.size))
      blocks foreach { block =>
        val ctx = new BlockContext(block, Set(), ownerId)
        try {
          println(ctx)
        } catch {
          case e: Exception => {
            println("failed to move block: %s".format(ctx))
          }
        }
      }
    } finally {
      adapter.shutdown()
    }
  }
}
