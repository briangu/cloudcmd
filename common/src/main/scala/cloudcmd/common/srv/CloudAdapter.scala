package cloudcmd.common.srv

import org.jboss.netty.channel.{ChannelFutureListener, MessageEvent, ChannelHandlerContext}
import org.jboss.netty.handler.codec.http._
import java.io.{FileInputStream, InputStream}
import cloudcmd.common._
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.handler.codec.http.HttpHeaders._
import io.viper.core.server.router.RouteResponse.RouteResponseDispose
import org.json.JSONArray
import io.viper.common.ViperServer
import io.viper.core.server.router._
import cloudcmd.common.util.StreamUtil

class StoreHandler(config: OAuthRouteConfig, route: String, cas: IndexedContentAddressableStorage) extends Route(route) {

  override
  def isMatch(request: HttpRequest) : Boolean = {
    (super.isMatch(request) && request.getMethod.equals(HttpMethod.POST))
  }

  override
  def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
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

      var is: InputStream = null
      try {
        if (handlerArgs.contains("key")) {
          val ctx = CloudAdapter.getBlockContext(handlerArgs)
          is = new ChannelBufferInputStream(request.getContent, request.getHeader(HttpHeaders.Names.CONTENT_LENGTH).toInt)
          val (hash, file) = StreamUtil.spoolStream(is)
          try {
            if (ctx.hashEquals(hash)) {
              is.close
              is = new FileInputStream(file)
              cas.store(ctx, is)
              val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CREATED)
              response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0)
              response
            } else {
              new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
            }
          } finally {
            file.delete
          }
        } else {
          new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
        }
      } finally {
        if (is != null) is.close()
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
}

object CloudAdapter {
  def getBlockContext(args: Map[String, String]) : BlockContext = {
    val (hash, tags) = args.get("key").get.split(",").toList.splitAt(1)
    new BlockContext(hash(0), tags.filter(_.length > 0).toSet)
  }
}

class CloudAdapter(cas: IndexedContentAddressableStorage, config: OAuthRouteConfig) {

  def addRoutes(server: ViperServer) {
    server.addRoute(new OAuthGetRestRoute(config, "/blocks/$key", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val ctx = CloudAdapter.getBlockContext(args)
        if (cas.contains(ctx)) {
          val (is, length) = cas.load(ctx)
          val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
          response.setContent(new FileChannelBuffer(is, length))
          response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, length)
          new RouteResponse(response, new RouteResponseDispose {
            def dispose() {
              is.close
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
        val ctx = CloudAdapter.getBlockContext(args)
        val success = cas.remove(ctx)
        if (success) {
          new StatusResponse(HttpResponseStatus.NO_CONTENT)
        } else {
          new StatusResponse(HttpResponseStatus.NOT_FOUND)
        }
      }
    }))

    // ACTIONS

    server.addRoute(new OAuthPostRestRoute(config, "/cache/refresh", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        cas.reindex()
        new StatusResponse(HttpResponseStatus.OK)
      }
    }))

    server.addRoute(new OAuthGetRestRoute(config, "/blocks", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val arr = new JSONArray
        cas.describe.foreach(arr.put)
        new JsonResponse(arr)
      }
    }))

    server.addRoute(new OAuthPostRestRoute(config, "/blocks/containsAll", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val ctxs = fromJsonArray(new JSONArray(args.get("ctxs")))
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
        val ctxs = fromJsonArray(new JSONArray(args.get("ctxs")))
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
        val ctxs = fromJsonArray(new JSONArray(args.get("ctxs")))
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
  }

  private def fromJsonArray(arr: JSONArray): Set[BlockContext] = {
    Set() ++ (0 until arr.length).par.map(idx => BlockContext.fromJson(arr.getJSONObject(idx)))
  }
}
