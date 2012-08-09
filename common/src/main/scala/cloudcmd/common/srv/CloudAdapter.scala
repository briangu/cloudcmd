package cloudcmd.common.srv

import org.jboss.netty.channel.{ChannelFutureListener, MessageEvent, ChannelHandlerContext}
import org.jboss.netty.handler.codec.http._
import java.net.URI
import java.io.InputStream
import cloudcmd.common.{ContentAddressableStorage, FileChannelBuffer, BlockContext}
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.handler.codec.http.HttpHeaders._
import io.viper.core.server.router.RouteResponse.RouteResponseDispose
import org.json.JSONArray
import io.viper.common.ViperServer
import io.viper.core.server.router._

class StoreHandler(config: OAuthRouteConfig, route: String, cas: ContentAddressableStorage) extends Route(route) {

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

    val (isValid, session) = OAuthRestRoute.isValid(config, request)
    val response = if (isValid) {
      val path = RouteUtil.parsePath(request.getUri)
      val args = RouteUtil.extractPathArgs(_route, path)
      args.putAll(RouteUtil.extractQueryParams(new URI(request.getUri)))

      var is: InputStream = null
      try {
        if (args.containsKey("hash") && args.containsKey("tags")) {
          val blockContext = new BlockContext(args.get("hash"), args.get("tags").split(",").filter(_.length > 0).toSet)
          is = new ChannelBufferInputStream(request.getContent)
          cas.store(blockContext, is)
          new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CREATED)
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

    val writeFuture = e.getChannel().write(response)
    if (!isKeepAlive(request)) {
      writeFuture.addListener(ChannelFutureListener.CLOSE)
    }
  }
}

class CloudAdapter(cas: ContentAddressableStorage, config: OAuthRouteConfig) {

  def addRoutes(server: ViperServer) {
    server.addRoute(new OAuthGetRestRoute(config, "/blocks/$key", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: java.util.Map[String, String]): RouteResponse = {
        val (hash, tags) = args.get("key").split(",").toList.splitAt(1)
        val ctx = new BlockContext(hash(0), tags.toSet)
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

    server.addRoute(new StoreHandler(config, "/blocks/$hash/$tags", cas))

    server.addRoute(new OAuthDeleteRestRoute(config, "/blocks/$hash,$tags", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: java.util.Map[String, String]): RouteResponse = {
        val (hash, tags) = args.get("key").split(",").toList.splitAt(1)
        val ctx = new BlockContext(hash(0), tags.toSet)
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
      def exec(session: OAuthSession, args: java.util.Map[String, String]): RouteResponse = {
        cas.refreshCache()
        new StatusResponse(HttpResponseStatus.OK)
      }
    }))

    server.addRoute(new OAuthGetRestRoute(config, "/blocks", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: java.util.Map[String, String]): RouteResponse = {
        val arr = new JSONArray
        cas.describe.foreach(ctx => arr.put(ctx.toJson))
        new JsonResponse(arr)
      }
    }))

    server.addRoute(new OAuthGetRestRoute(config, "/blocks/hashes", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: java.util.Map[String, String]): RouteResponse = {
        val arr = new JSONArray
        cas.describeHashes.foreach(arr.put)
        new JsonResponse(arr)
      }
    }))

    server.addRoute(new OAuthPostRestRoute(config, "/blocks/containsAll", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: java.util.Map[String, String]): RouteResponse = {
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
      def exec(session: OAuthSession, args: java.util.Map[String, String]): RouteResponse = {
        val ctxs = fromJsonArray(new JSONArray(args.get("ctxs")))
        val blockLevelCheck = if (args.containsKey("blockLevelCheck")) args.get("blockLevelCheck").toBoolean else false
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
      def exec(session: OAuthSession, args: java.util.Map[String, String]): RouteResponse = {
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
