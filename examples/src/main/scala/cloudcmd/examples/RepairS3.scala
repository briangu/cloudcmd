package cloudcmd.examples

import java.net.URI
import cloudcmd.common.BlockContext
import cloudcmd.common.config.AdapterFactory
import org.jets3t.service.model.S3Object
import org.jets3t.service.io.RepeatableInputStream
import org.jets3t.service.security.AWSCredentials
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import cloudcmd.common.adapters.DirectS3Adapter

object RepairS3 {

  def main(args: Array[String]) {
    if (args.size != 2) {
      println("mvblocks <src adapter uri> <dest adapter uri>")
      return
    }

    val srcAdapter = AdapterFactory.createDirectAdapter(new URI(args(0)))
    val destAdapter = AdapterFactory.createDirectAdapter(new URI(args(1)))
    val adapters = List(srcAdapter, destAdapter)

    try {
      if (!srcAdapter.IsOnLine) {
        println("src adapter %s is not online!".format(srcAdapter.getSignature))
        return
      }

      if (!destAdapter.IsOnLine) {
        println("dest adapter %s is not online!".format(destAdapter.getSignature))
        return
      }

      val srcBlocks = srcAdapter.describe()
      println("src has %d blocks".format(srcBlocks.size))
      val malformed = srcBlocks.filter(hash => hash.startsWith("/") || hash.contains("//"))
      val repaired = malformed flatMap { hash =>
        var fixed = if (hash.startsWith("/")) {
          hash.substring(1)
        } else {
          hash
        }
        fixed = if (fixed.contains("//")) {
          fixed.replace("//", "/")
        } else {
          fixed
        }

        if (srcBlocks.contains(fixed)) {
          Nil
        } else {
          Map(hash -> fixed)
        }
      }

      repaired.foreach(println)

      val (awsKey, awsSecret, awsBucketName, objectPrefix, useRRS) = DirectS3Adapter.parseAwsInfo(new URI(args(0)))

      val creds = new AWSCredentials(awsKey, awsSecret)
      val s3Service = new RestS3Service(creds)

      repaired.par foreach { case (src, dest) =>
        val ctx = new BlockContext(src)
        try {
          println("renaming block %s to destination".format(ctx))

          val s3Object = new S3Object(dest)
          s3Object.setBucketName(awsBucketName)
          s3Object.setStorageClass(S3Object.STORAGE_CLASS_REDUCED_REDUNDANCY)
          s3Service.renameObject(awsBucketName, src, s3Object)
        } catch {
          case e: Exception => {
            println("failed to move block: %s %s".format(ctx, e))
          }
        }
      }
    } finally {
      adapters.foreach(_.shutdown())
    }
  }
}
