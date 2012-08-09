package cloudcmd.common.srv

import io.viper.core.server.router.{RouteResponse, RouteUtil, Route}
import org.jboss.netty.channel.{ChannelFutureListener, MessageEvent, ChannelEvent, ChannelHandlerContext}
import org.jboss.netty.handler.codec.http.HttpVersion._
import java.net.URI
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.handler.codec.http.HttpHeaders._
import org.jboss.netty.handler.codec.http
import http._
import org.json.JSONObject
import cloudcmd.common.util.CryptoUtil
import java.io.{File, ByteArrayInputStream}
import cloudcmd.common.FileUtil

/*
{
  "<appKey>": {
    "key": "<appKey",
    "secret": "<appSecret>",
    "userId": "1234"
  }
}
*/

class AuthSession(session: JSONObject) {
  def getAppKey(): String = session.getString("key")

  def getSecret(): String = session.getString("secret")

  def hasProperty(name: String): Boolean = session.has(name)
}

trait AuthSessionService {
  def getSession(key: String): AuthSession

  def setSession(key: String, session: AuthSession)

  def deleteSession(key: String)
}

object SimpleAuthSessionService {
  val sessions = {
    val file = new File("auth.json") // TODO: get path from config
    if (file.exists()) {
      new JSONObject(FileUtil.readFile(file))
    } else {
      new JSONObject()
    }
  }
  val instance = new SimpleAuthSessionService
}

class SimpleAuthSessionService extends AuthSessionService {
  def getSession(key: String): AuthSession = {
    if (SimpleAuthSessionService.sessions.has(key)) {
      new AuthSession(SimpleAuthSessionService.sessions.getJSONObject(key))
    } else {
      null
    }
  }

  def setSession(key: String, session: AuthSession) = SimpleAuthSessionService.sessions.put(key, session)

  def deleteSession(key: String) = SimpleAuthSessionService.sessions.remove(key)
}

class HmacRouteConfig(val sessions: AuthSessionService) {}

trait HmacRouteHandler {
  def exec(session: AuthSession, args: java.util.Map[String, String]): RouteResponse
}

class HmacGetRestRoute(config: HmacRouteConfig, route: String, handler: HmacRouteHandler) extends HmacAuthRestRoute(route, handler, HttpMethod.GET, config) {}

class HmacPostRestRoute(config: HmacRouteConfig, route: String, handler: HmacRouteHandler) extends HmacAuthRestRoute(route, handler, HttpMethod.POST, config) {}

class HmacPutRestRoute(config: HmacRouteConfig, route: String, handler: HmacRouteHandler) extends HmacAuthRestRoute(route, handler, HttpMethod.PUT, config) {}

class HmacDeleteRestRoute(config: HmacRouteConfig, route: String, handler: HmacRouteHandler) extends HmacAuthRestRoute(route, handler, HttpMethod.DELETE, config) {}

object HmacAuthRestRoute {

  val APP_KEY_HEADER = "X-APPKEY"
  val HMAC_MD5_SIGNATURE_HEADER = "X-HMAC-MD5"

  // NOTE: this only does a partial validation of the request and doesn't check the post body
  // TODO: in cases where the post body isn't a block, we should validate it with an md5sum header
  //       otherwise, we rely on the SHA-256 hash that comes with the block as it's id
  def isValid(config: HmacRouteConfig, request: HttpRequest): (Boolean, AuthSession) = {
    // get app key from header
    val appKey = request.getHeader(APP_KEY_HEADER)

    // attempt to get session from app key
    val session = config.sessions.getSession(appKey)
    if (session != null) {
      // build canonical HMAC string
      val contentLength = request.getHeader(HttpHeaders.Names.CONTENT_LENGTH)
      val requestDate = request.getHeader(HttpHeaders.Names.DATE)
      val canonical = request.getUri + appKey + contentLength + requestDate

      val challengeMd5 = request.getHeader(HMAC_MD5_SIGNATURE_HEADER)

      // hash with secret
      val bais = new ByteArrayInputStream(canonical.getBytes("UTF-8"))
      try {
        val localHmac = CryptoUtil.computeMD5HashAsString(bais)
        (localHmac.equals(challengeMd5), session)
      } finally {
        bais.close
      }
    } else {
      (false, null)
    }
  }

  def signRequest(appKey: String, appSecret: String, uri: String, dateHeader: String, contentLengthHeader: String) : String = {
    val canonical = uri + appKey + contentLengthHeader + dateHeader
    val bais = new ByteArrayInputStream(canonical.getBytes("UTF-8"))
    try {
      CryptoUtil.computeMD5HashAsString(bais)
    } finally {
      bais.close
    }
  }
}

class HmacAuthRestRoute(route: String, handler: HmacRouteHandler, method: HttpMethod, protected val config: HmacRouteConfig) extends Route(route) {

  override
  def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    if (!(e.isInstanceOf[MessageEvent]) || !(e.asInstanceOf[MessageEvent].getMessage.isInstanceOf[HttpRequest])) {
      super.handleUpstream(ctx, e)
      return
    }

    val request = e.asInstanceOf[MessageEvent].getMessage.asInstanceOf[HttpRequest]

    val (isValid, session) = HmacAuthRestRoute.isValid(config, request)

    val response = if (isValid) {
      try {
        val path = RouteUtil.parsePath(request.getUri())
        val args = if (request.getMethod == HttpMethod.POST || request.getMethod == HttpMethod.PUT) {
          RouteUtil.extractArgs(request, _route, path)
        } else {
          val args = RouteUtil.extractPathArgs(_route, path)
          args.putAll(RouteUtil.extractQueryParams(new URI(request.getUri())))
          args
        }

        val routeResponse = handler.exec(session, args)
        if (routeResponse.HttpResponse == null) {
          val response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK)
          response.setContent(wrappedBuffer("{\"status\": true}".getBytes()))
          response
        } else {
          val response = routeResponse.HttpResponse
          if (response.getHeader(HttpHeaders.Names.CONTENT_LENGTH) == null) {
            setContentLength(response, response.getContent().readableBytes())
          }
          if (isKeepAlive(request)) {
            response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
          } else {
            response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE)
          }

          response
        }
      } catch {
        case e: Exception => new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
      }
    } else {
      new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.FORBIDDEN)
    }

    if (response != null) writeResponse(request, response, e)
  }

  def writeResponse(request: HttpRequest, response: HttpResponse, e: ChannelEvent) {
    val writeFuture = e.getChannel().write(response)
    if (response.getStatus != HttpResponseStatus.OK || !isKeepAlive(request)) {
      writeFuture.addListener(ChannelFutureListener.CLOSE)
    }
  }
}
