package cloudcmd.common.srv

import org.jboss.netty.channel.{ChannelFutureListener, MessageEvent, ChannelHandlerContext}
import org.jboss.netty.handler.codec.http._
import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import cloudcmd.common._
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.handler.codec.http.HttpHeaders._
import io.viper.core.server.router.RouteResponse.RouteResponseDispose
import org.json.{JSONObject, JSONArray}
import io.viper.common.ViperServer
import io.viper.core.server.router._
import cloudcmd.common.util.{CryptoUtil, StreamUtil}
import java.nio.ByteBuffer
import java.util

class StoreHandler(config: OAuthRouteConfig, route: String, cas: IndexedContentAddressableStorage) extends Route(route) {

  val MAX_UPLOAD_SIZE = 64 * 1024 * 1024
  val BUFFER_SIZE = 32 * 1024 * 1024
  val READ_BUFFER_SIZE = 1024 * 1024

  private val readBuffer = new ThreadLocal[ByteBuffer] {
    override def initialValue = ByteBuffer.allocate(READ_BUFFER_SIZE)
  }
  private val buffer = new ThreadLocal[ByteBuffer] {
    override def initialValue = ByteBuffer.allocate(BUFFER_SIZE)
  }

  override def isMatch(request: HttpRequest) : Boolean = {
    (super.isMatch(request) && request.getMethod.equals(HttpMethod.POST))
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val msg = e.getMessage

    if (!(msg.isInstanceOf[HttpMessage]) && !(msg.isInstanceOf[HttpChunk])) {
      ctx.sendUpstream(e)
      return
    }

    val request = e.getMessage.asInstanceOf[org.jboss.netty.handler.codec.http.HttpRequest]
    if (!super.isMatch(request) || !request.getMethod.equals(HttpMethod.POST)) {
      ctx.sendUpstream(e)
      return
    }

    val (isValid, session, args) = OAuthRestRoute.validate(config, request)
    val response = if (isValid) {
      import scala.collection.JavaConversions._
      val path = RouteUtil.parsePath(request.getUri)
      val handlerArgs = args ++ RouteUtil.extractPathArgs(_route, path)

      if (handlerArgs.contains("key")) {
        val ctx = CloudAdapter.getBlockContext(handlerArgs, Some(session))
        val contentLength = request.getHeader(HttpHeaders.Names.CONTENT_LENGTH).toInt

        if (contentLength > MAX_UPLOAD_SIZE) {
          return new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
        }

        try {
          if (ctx.isMeta) {
            if (contentLength <= BUFFER_SIZE) {
              storeViaSpooledMemory(ctx, request)
            } else {
              new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
            }
          } else {
            if (contentLength <= BUFFER_SIZE) {
              storeViaSpooledMemory(ctx, request)
            } else {
              storeViaSpooledFile(ctx, request)
            }
          }
        } catch {
          case e: Exception => {
            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
          }
        }
      } else {
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
      }
    } else {
      new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.FORBIDDEN)
    }

    if (isKeepAlive(request)) {
      response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
    } else {
      response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE)
    }

    val writeFuture = e.getChannel.write(response)
    if (!isKeepAlive(request)) {
      writeFuture.addListener(ChannelFutureListener.CLOSE)
    }
  }

  private def storeViaSpooledMemory(ctx: BlockContext, request: HttpRequest): HttpResponse = {
    val contentLength = request.getHeader(HttpHeaders.Names.CONTENT_LENGTH).toInt

    var is: InputStream = null
    val (hash, array, length) = try {
      is = new ChannelBufferInputStream(request.getContent, contentLength)
      StreamUtil.spoolStreamToByteBuffer(is, readBuffer.get(), buffer.get())
      val array = buffer.get().array()
      val length = buffer.get().limit()
      val hash = CryptoUtil.computeHashAsString(new ByteArrayInputStream(array, 0, length))
      (hash, array, length)
    } finally {
      FileUtil.SafeClose(is)
    }

    if (ctx.hashEquals(hash) && (length == contentLength)) {
      if (ctx.isMeta) {
        validateAndStoreMeta(ctx, request, array, length)
      } else {
        store(ctx, new ByteArrayInputStream(array, 0, length))
      }
    } else {
      new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
    }
  }

  def validateAndStoreMeta(ctx: BlockContext, request: HttpRequest, array: Array[Byte], length: Int): HttpResponse = {
    try {
      val data = util.Arrays.copyOfRange(array, 0, length)
      val metaJson = new JSONObject(new String(data, "UTF-8"))
      val fmd = FileMetaData.create(ctx.hash, metaJson)
      if (!fmd.hasProperties || !fmd.getProperties.has("ownerId")) {
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
      } else {
        val ownerId = fmd.getProperties.getString("ownerId")
        val isValid = ctx.ownerId match {
          case Some(id) => ownerId == id
          case None => false
        }
        if (isValid) {
          store(ctx, new ByteArrayInputStream(array, 0, length))
        } else {
          new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
        }
      }
    } catch {
      case e: Exception => {
        // TODO: log
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
      }
    }
  }

  def storeViaSpooledFile(ctx: BlockContext, request: HttpRequest): HttpResponse = {
    var is: InputStream = null
    val (hash, file) = try {
      val contentLength = request.getHeader(HttpHeaders.Names.CONTENT_LENGTH).toInt
      is = new ChannelBufferInputStream(request.getContent, contentLength)
      StreamUtil.spoolStream(is)
    } finally {
      FileUtil.SafeClose(is)
    }

    try {
      if (ctx.hashEquals(hash)) {
        val is =new FileInputStream(file)
        try {
          store(ctx, is)
        } finally {
          FileUtil.SafeClose(is)
        }
      } else {
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
      }
    } finally {
      file.delete
    }
  }

  def store(ctx: BlockContext, is: InputStream): HttpResponse = {
    try {
      cas.store(ctx, is)
      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CREATED)
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0)
      response
    } catch {
      case e: Exception => {
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
      }
    }
  }
}

object CloudAdapter {
  def getBlockContext(args: Map[String, String], session: Option[OAuthSession]) : BlockContext = {
    val (hash, tags) = args.get("key").get.split(",").toList.splitAt(1)
    val ownerId = session match {
      case Some(sess) => Some(sess.getAsRequestToken.getKey())
      case None => None
    }
    new BlockContext(hash(0), tags.filter(_.length > 0).toSet, ownerId)
  }
}

class CloudAdapter(cas: IndexedContentAddressableStorage, config: OAuthRouteConfig) {

  def addRoutes(server: ViperServer) {

    // WARNING: order of routes matters, more specific must come first

    server.addRoute(new OAuthPostRestRoute(config, "/blocks/containsAll", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val ctxs = fromJsonArray(new JSONArray(args.getOrElse("ctxs", "[]")), session)
        val res = cas.containsAll(ctxs)
        val arr = new JSONArray
        res.map {
          case (ctx: BlockContext, status: Boolean) =>
            val obj = ctx.toJson
            obj.put("_status", status)
            arr.put(obj)
        }
        new JsonResponse(arr)
      }
    }))

    server.addRoute(new OAuthPostRestRoute(config, "/blocks/ensureAll", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val ctxs = fromJsonArray(new JSONArray(args.getOrElse("ctxs", "[]")), session)
        val blockLevelCheck = if (args.contains("blockLevelCheck")) args.get("blockLevelCheck").get.toBoolean else false
        val res = cas.ensureAll(ctxs, blockLevelCheck)
        val arr = new JSONArray
        res.map {
          case (ctx: BlockContext, status: Boolean) =>
            val obj = ctx.toJson
            obj.put("_status", status)
            arr.put(obj)
        }
        new JsonResponse(arr)
      }
    }))

    server.addRoute(new OAuthPostRestRoute(config, "/blocks/removeAll", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val ctxs = fromJsonArray(new JSONArray(args.getOrElse("ctxs", "[]")), session)
        val res = cas.removeAll(ctxs)
        val arr = new JSONArray
        res.map {
          case (ctx: BlockContext, status: Boolean) =>
            val obj = ctx.toJson
            obj.put("_status", status)
            arr.put(obj)
        }
        new JsonResponse(arr)
      }
    }))

    server.addRoute(new OAuthGetRestRoute(config, "/blocks/$key", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val ctx = CloudAdapter.getBlockContext(args, Some(session))
        if (cas.contains(ctx)) {
          val (is, length) = cas.load(ctx)
          val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
          response.setContent(new FileChannelBuffer(is, length))
          response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, length)
          new RouteResponse(response, new RouteResponseDispose {
            def dispose() {
              is.close()
            }
          })
        } else {
          new StatusResponse(HttpResponseStatus.NOT_FOUND)
        }
      }
    }))

    server.addRoute(new StoreHandler(config, "/blocks/$key", cas))

    server.addRoute(new OAuthDeleteRestRoute(config, "/blocks/$key", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val ctx = CloudAdapter.getBlockContext(args, Some(session))
        val success = cas.remove(ctx)
        if (success) {
          new StatusResponse(HttpResponseStatus.NO_CONTENT)
        } else {
          new StatusResponse(HttpResponseStatus.NOT_FOUND)
        }
      }
    }))

    server.addRoute(new OAuthGetRestRoute(config, "/blocks", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val arr = new JSONArray
        cas.describe(Some(session.getAsRequestToken.getKey())).foreach(arr.put)
        new JsonResponse(arr)
      }
    }))

    server.addRoute(new OAuthPostRestRoute(config, "/find", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val filter = new JSONObject(args.getOrElse("filter", "{}"))

        if (!filter.has("count")) {
          filter.put("count", 500)
        }

        // NOTE: we currently assume keys are longs
        filter.put("PROPERTIES__OWNERID", session.getAsRequestToken.getKey.toLong)

        val fmds = cas.find(filter)
        new JsonResponse(FileMetaData.toJsonArray(fmds))
      }
    }))

    // required:
    // src meta hash id
    // dest ownerId
    // operation:
    //  load meta from cas
    //  verify creatorId of meta == current session ownerId
    //  create new fmd with dest as ownerId, src ownerId as creatorId
    //  create new block context with dest ownerId
    //  store new fmd in dest store
    server.addRoute(new OAuthPostRestRoute(config, "/share", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val srcCreatorId = session.getAsRequestToken.getKey()
        val destOwnerId = args.get("destOwnerId") match {
          case Some(id) => {
            // verify that the dest user exists
            if (SimpleOAuthSessionService.instance.isValidKey(id)) {
              id
            } else {
              return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            }
          }
          case None => {
            return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
          }
        }

        val srcFmd = args.get("metaHashId") match {
          case Some(hashId) => {
            val ctx = new BlockContext(hashId, ownerId = Some(srcCreatorId))
            try {
              val fmd = FileMetaData.create(hashId, new JSONObject(StreamUtil.spoolStreamToString(cas.load(ctx)._1)))
              if (fmd.isCreator(srcCreatorId)) {
                Some(fmd)
              } else {
                return new StatusResponse(HttpResponseStatus.UNAUTHORIZED)
              }
            } catch {
              case e: Exception => {
                None
              }
            }
          }
          case None => {
            None
          }
        }

        srcFmd match {
          case Some(fmd) => {
            try {
              fmd.getProperties.put("ownerId", destOwnerId)
              fmd.getProperties.put("creatorId", srcCreatorId)
              val rawBytes = new ByteArrayInputStream(fmd.getRawData.toString.getBytes("UTF-8"))
              rawBytes.mark(0)
              val shareFmd = FileMetaData.create(rawBytes, fmd.getRawData)
              val shareCtx = new BlockContext(shareFmd.getHash, fmd.getTags, Some(destOwnerId))
              rawBytes.reset()
              cas.store(shareCtx, rawBytes)
              new StatusResponse(HttpResponseStatus.OK)
            } catch {
              case e: Exception => {
                new StatusResponse(HttpResponseStatus.BAD_REQUEST)
              }
            }
          }
          case None => {
            new StatusResponse(HttpResponseStatus.NOT_FOUND)
          }
        }
      }
    }))

    server.addRoute(new OAuthGetRestRoute(config, "/ping", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        new StatusResponse(HttpResponseStatus.OK)
      }
    }))
  }

  private def fromJsonArray(arr: JSONArray, session: OAuthSession): Set[BlockContext] = {
    Set() ++ (0 until arr.length).map(idx => BlockContext.fromJson(arr.getJSONObject(idx), Some(session.getAsRequestToken.getKey())))
  }
}
