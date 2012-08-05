package cloudcmd.srv

import io.viper.common.{NestServer, ViperServer}
import io.viper.core.server.router._
import java.util
import cloudcmd.common.{FileChannelBuffer, BlockContext, FileUtil}
import java.io._
import cloudcmd.common.engine.CloudEngine
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.channel.{ChannelFutureListener, MessageEvent, ChannelHandlerContext}
import org.jboss.netty.handler.codec.http2.HttpPostRequestDecoder
import org.jboss.netty.handler.codec.http2.DefaultHttpDataFactory
import org.jboss.netty.handler.codec.http2.FileUpload
import org.jboss.netty.handler.codec.http2.DiskAttribute
import org.jboss.netty.handler.codec.http.HttpHeaders._
import org.json.{JSONException, JSONArray}

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

    val convertedRequest = convertRequest(request)
    val decoder = new HttpPostRequestDecoder(new DefaultHttpDataFactory(true), convertedRequest)
    val response = try {
      if (decoder.isMultipart) {
        if (decoder.getBodyHttpDatas.size() == 2) {
          // TODO: use hybrid mode
          val fileData = decoder.getBodyHttpData("files[]")
          val ctxData = decoder.getBodyHttpData("ctx")
          if (fileData != null && ctxData != null) {
            val upload  = fileData.asInstanceOf[FileUpload]
            val ctx = BlockContext.fromJson(FileUtils.readFile(ctxData.asInstanceOf[DiskAttribute].getFile))
            processFile(upload, ctx)
          } else {
            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
          }
        } else {
          new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
        }
      } else {
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
      }
    } finally {
      decoder.cleanFiles()
    }

    if (isKeepAlive(request)) {
      response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
    } else {
      response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE)
    }
    setContentLength(response, response.getContent().readableBytes())

    val writeFuture = e.getChannel().write(response)
    if (!isKeepAlive(request)) {
      writeFuture.addListener(ChannelFutureListener.CLOSE)
    }
  }

  def convertRequest(request: HttpRequest) : org.jboss.netty.handler.codec.http2.HttpRequest = {
    val convertedRequest =
      new org.jboss.netty.handler.codec.http2.DefaultHttpRequest(
        org.jboss.netty.handler.codec.http2.HttpVersion.HTTP_1_0,
        org.jboss.netty.handler.codec.http2.HttpMethod.POST,
        request.getUri)
    convertedRequest.setContent(request.getContent)
    convertedRequest.setChunked(request.isChunked)
    import collection.JavaConversions._
    request.getHeaders.foreach { entry =>
      convertedRequest.setHeader(entry.getKey, entry.getValue)
    }
    convertedRequest
  }

  def processFile(upload: FileUpload, ctx: BlockContext) : HttpResponse = {
    var fis: InputStream = null
    try {
      fis = new FileInputStream(upload.getFile)
      cloudEngine.store(ctx, fis)
      new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CREATED)
    }
    catch {
      case e: JSONException => {
        e.printStackTrace()
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR)
      }
      case e: UnsupportedEncodingException => {
        e.printStackTrace()
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR)
      }
    } finally {
      if (fis != null) {
        fis.close()
      }
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

    addRoute(new StoreHandler("/blocks", cloudEngine))

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
