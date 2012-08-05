package cloudcmd.srv

import io.viper.common.{NestServer, ViperServer}
import io.viper.core.server.router._
import java.util
import cloudcmd.common.{FileChannelBuffer, BlockContext, FileUtil}
import java.io._
import cloudcmd.common.engine.CloudEngine
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.channel.{ChannelFutureListener, MessageEvent, ChannelHandlerContext}
import org.jboss.netty.handler.codec.http.HttpHeaders._
import org.json.JSONArray
import org.jboss.netty.buffer.ChannelBufferInputStream
import java.net.URI

object CloudServer {
  def main(args: Array[String]) {
    var configRoot: String = FileUtil.findConfigDir(FileUtil.getCurrentWorkingDirectory, ".cld")
    if (configRoot == null) {
      configRoot = System.getenv("HOME") + File.separator + ".cld"
      new File(configRoot).mkdir
    }

    CloudServices.ConfigService.init(configRoot)
    CloudServices.CloudEngine.init

    try {
      NestServer.run(8080, new CloudServer(CloudServices.CloudEngine))
    } finally {
      CloudServices.shutdown
    }
  }
}

class StoreHandler(route: String, cloudEngine: CloudEngine) extends Route(route) {

  final val THUMBNAIL_CREATE_THRESHOLD = 128 * 1024 // TODO: come from config

  override
  def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val msg = e.getMessage

    if (!(msg.isInstanceOf[HttpMessage]) && !(msg.isInstanceOf[HttpChunk])) {
      ctx.sendUpstream(e)
      return
    }

    val request = e.getMessage.asInstanceOf[org.jboss.netty.handler.codec.http.HttpRequest]
    if (request.getMethod != HttpMethod.POST) {
      ctx.sendUpstream(e)
      return
    }

    val path = RouteUtil.parsePath(request.getUri)
    val args = RouteUtil.extractPathArgs(_route, path)
    args.putAll(RouteUtil.extractQueryParams(new URI(request.getUri)))
    if (!args.containsKey("hash")) return new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
    if (!args.containsKey("tags")) return new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
    val blockContext = new BlockContext(args.get("hash"), args.get("tags").split(",").toSet)
    val is = new ChannelBufferInputStream(request.getContent)

    val response = try {
      cloudEngine.store(blockContext, is)
      new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CREATED)
    } finally {
      is.close()
    }

    if (isKeepAlive(request)) {
      response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
    } else {
      response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE)
    }

//    setContentLength(response, response.getContent().readableBytes())

    val writeFuture = e.getChannel().write(response)
    if (!isKeepAlive(request)) {
      writeFuture.addListener(ChannelFutureListener.CLOSE)
    }
  }
}

class CloudServer(cloudEngine: CloudEngine) extends ViperServer("res:///cloudserver") {

  override
  def addRoutes {
    get("/blocks/$hash/$tags", new RouteHandler {
      def exec(args: util.Map[String, String]): RouteResponse = {
        val ctx = new BlockContext(args.get("hash"), args.get("tags").split(",").toSet)
        if (cloudEngine.contains(ctx)) {
          val (is, length) = cloudEngine.load(ctx)
          val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
          response.setContent(new FileChannelBuffer(is, length))
          response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, length)
          new RouteResponse(response)
        } else {
          new StatusResponse(HttpResponseStatus.NOT_FOUND)
        }
      }
    })

    addRoute(new StoreHandler("/blocks/$hash/$tags", cloudEngine))

    delete("/blocks/$hash/$tags", new RouteHandler {
      def exec(args: util.Map[String, String]): RouteResponse = {
        val ctx = new BlockContext(args.get("hash"), args.get("tags").split(",").toSet)
        val success = cloudEngine.remove(ctx)
        if (success) {
          new StatusResponse(HttpResponseStatus.NO_CONTENT)
        } else {
          new StatusResponse(HttpResponseStatus.NOT_FOUND)
        }
      }
    })

    // ACTIONS

    post("/cache/refresh", new RouteHandler {
      def exec(args: util.Map[String, String]): RouteResponse = {
        cloudEngine.refreshCache()
        new StatusResponse(HttpResponseStatus.OK)
      }
    })

    get("/blocks", new RouteHandler {
      def exec(args: util.Map[String, String]): RouteResponse = {
        val arr = new JSONArray
        cloudEngine.describe.foreach(ctx => arr.put(ctx.toJson))
        new JsonResponse(arr)
      }
    })

    get("/blocks/meta", new RouteHandler {
      def exec(args: util.Map[String, String]): RouteResponse = {
        val arr = new JSONArray
        cloudEngine.describeMeta.foreach(ctx => arr.put(ctx.toJson))
        new JsonResponse(arr)
      }
    })

    get("/blocks/hashes", new RouteHandler {
      def exec(args: util.Map[String, String]): RouteResponse = {
        val arr = new JSONArray
        cloudEngine.describeHashes.foreach(arr.put)
        new JsonResponse(arr)
      }
    })

    post("/blocks/containsAll", new RouteHandler {
      def exec(args: util.Map[String, String]): RouteResponse = {
        val ctxs = fromJsonArray(new JSONArray(args.get("ctxs")))
        val res = cloudEngine.containsAll(ctxs)
        val arr = new JSONArray
        res.map{ case (ctx: BlockContext, status: Boolean) =>
          val obj = ctx.toJson
          obj.put("_status", status)
          arr.put(obj)
        }
        new JsonResponse(arr)
      }
    })

    post("/blocks/ensureAll", new RouteHandler {
      def exec(args: util.Map[String, String]): RouteResponse = {
        val ctxs = fromJsonArray(new JSONArray(args.get("ctxs")))
        val blockLevelCheck = if (args.containsKey("blockLevelCheck")) args.get("blockLevelCheck").toBoolean else false
        val res = cloudEngine.ensureAll(ctxs, blockLevelCheck)
        val arr = new JSONArray
        res.map{ case (ctx: BlockContext, status: Boolean) =>
          val obj = ctx.toJson
          obj.put("_status", status)
          arr.put(obj)
        }
        new JsonResponse(arr)
      }
    })

    post("/blocks/removeAll", new RouteHandler {
      def exec(args: util.Map[String, String]): RouteResponse = {
        val ctxs = fromJsonArray(new JSONArray(args.get("ctxs")))
        val res = cloudEngine.removeAll(ctxs)
        val arr = new JSONArray
        res.map{ case (ctx: BlockContext, status: Boolean) =>
          val obj = ctx.toJson
          obj.put("_status", status)
          arr.put(obj)
        }
        new JsonResponse(arr)
      }
    })
  }

  private def fromJsonArray(arr: JSONArray) : Set[BlockContext] = {
    Set() ++ (0 until arr.length).par.map(idx => BlockContext.fromJson(arr.getJSONObject(idx)))
  }
}
