package cloudcmd.common.srv

import org.jboss.netty.channel.{ChannelFutureListener, MessageEvent, ChannelHandlerContext}
import org.jboss.netty.handler.codec.http._
import java.io.{FileInputStream, InputStream}
import cloudcmd.common._
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.handler.codec.http.HttpHeaders._
import io.viper.core.server.router.RouteResponse.RouteResponseDispose
import org.json.{JSONObject, JSONArray}
import io.viper.common.ViperServer
import io.viper.core.server.router._
import cloudcmd.common.engine.IndexStorage
import cloudcmd.common.util.StreamUtil

class StoreHandler(config: OAuthRouteConfig, route: String, cas: ContentAddressableStorage, indexStorage: IndexStorage) extends Route(route) {

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
        handlerArgs.get("key") match {
          case Some(hash) => {
            is = new ChannelBufferInputStream(request.getContent, request.getHeader(HttpHeaders.Names.CONTENT_LENGTH).toInt)
            val (streamHash, file) = StreamUtil.spoolStream(is)
            try {
              if (streamHash.equals(hash)) {
                is.close()
                is = new FileInputStream(file)
                cas.store(hash, is)
                if (hash.endsWith(".meta")) {
                  indexStorage.add(FileMetaData.create(hash, new JSONObject(FileUtil.readFile(file))))
                }
                val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CREATED)
                response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0)
                response
              } else {
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
              }
            } finally {
              file.delete
            }
          }
          case None => {
            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
          }
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

class CloudAdapter(cas: ContentAddressableStorage, indexStorage: IndexStorage, config: OAuthRouteConfig) {

  def addRoutes(server: ViperServer) {
    server.addRoute(new OAuthGetRestRoute(config, "/blocks/$key", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        args.get("key") match {
          case Some(hash) => {
            if (cas.contains(hash)) {
              val (is, length) = cas.load(hash)
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
          case _ => {
            new StatusResponse(HttpResponseStatus.NOT_FOUND)
          }
        }
      }
    }))

    server.addRoute(new StoreHandler(config, "/blocks/$key", cas, indexStorage))

    server.addRoute(new OAuthDeleteRestRoute(config, "/blocks/$key", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        args.get("key") match {
          case Some(hash) => {
            val success = cas.remove(hash)
            if (success) {
              new StatusResponse(HttpResponseStatus.NO_CONTENT)
            } else {
              new StatusResponse(HttpResponseStatus.NOT_FOUND)
            }
          }
          case None => {
            new StatusResponse(HttpResponseStatus.NOT_FOUND)
          }
        }
      }
    }))

    // ACTIONS

    server.addRoute(new OAuthPostRestRoute(config, "/cache/refresh", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        cas.refreshCache()
        new StatusResponse(HttpResponseStatus.OK)
      }
    }))

    server.addRoute(new OAuthGetRestRoute(config, "/blocks", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val arr = new JSONArray
        cas.describe().foreach(hash => arr.put(hash))
        new JsonResponse(arr)
      }
    }))

    server.addRoute(new OAuthGetRestRoute(config, "/blocks/hashes", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val arr = new JSONArray
        cas.describe().foreach(arr.put)
        new JsonResponse(arr)
      }
    }))

    server.addRoute(new OAuthPostRestRoute(config, "/blocks/containsAll", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val obj = new JSONObject
        args.get("hashes") match {
          case Some(hashes) => {
            val res = cas.containsAll(hashes.split(",").toSet)
            res.map {
              case (hash: String, status: Boolean) => obj.put(hash, status)
            }
          }
          case None => ;
        }
        new JsonResponse(obj)
      }
    }))

    server.addRoute(new OAuthPostRestRoute(config, "/blocks/ensureAll", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val obj = new JSONObject
        args.get("hashes") match {
          case Some(hashes) => {
            val blockLevelCheck = if (args.contains("blockLevelCheck")) args.get("blockLevelCheck").get.toBoolean else false
            val res = cas.ensureAll(hashes.split(",").toSet, blockLevelCheck)
            res.map {
              case (hash: String, status: Boolean) => obj.put(hash, status)
            }
          }
          case None => ;
        }
        new JsonResponse(obj)
      }
    }))

    server.addRoute(new OAuthPostRestRoute(config, "/blocks/removeAll", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val obj = new JSONObject
        args.get("hashes") match {
          case Some(hashes) => {
            val res = cas.removeAll(hashes.split(",").toSet)
            val obj = new JSONObject
            res.map {
              case (hash: String, status: Boolean) => obj.put(hash, status)
            }
          }
          case None => ;
        }
        new JsonResponse(obj)
      }
    }))
  }
}
