package cloudcmd.common.srv

import io.viper.core.server.router.{RouteResponse, RouteUtil, Route}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http.HttpVersion._
import java.net.URLDecoder
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.handler.codec.http.HttpHeaders._
import org.jboss.netty.handler.codec.http
import http._
import org.json.JSONObject
import java.io.File
import cloudcmd.common.FileUtil
import com.ning.http.client.oauth.{RequestToken, ConsumerKey, OAuthSignatureCalculator}
import com.ning.http.client.FluentStringsMap

/*
{
  "<appKey>": {
    "key": "<appKey",
    "secret": "<appSecret>",
    "userId": "1234"
  }
}
*/

class OAuthSession(session: JSONObject) {
  def getAppKey(): String = session.getString("key")
  def getSecret(): String = session.getString("secret")

  def getAsConsumerKey() : ConsumerKey = new ConsumerKey(getAppKey, getSecret)
  def getAsRequestToken() : RequestToken = new RequestToken(getAppKey, getSecret)

  def hasProperty(name: String): Boolean = session.has(name)
}

trait OAuthSessionService {
  def getSession(key: String): OAuthSession

  def setSession(key: String, session: OAuthSession)

  def deleteSession(key: String)
}

object SimpleOAuthSessionService {
  val sessions = {
    val file = new File("auth.json") // TODO: get path from config
    if (file.exists()) {
      new JSONObject(FileUtil.readFile(file))
    } else {
      new JSONObject()
    }
  }
  val instance = new SimpleOAuthSessionService
}

class SimpleOAuthSessionService extends OAuthSessionService {
  def getSession(key: String): OAuthSession = {
    if (key != null && SimpleOAuthSessionService.sessions.has(key)) {
      new OAuthSession(SimpleOAuthSessionService.sessions.getJSONObject(key))
    } else {
      null
    }
  }

  def setSession(key: String, session: OAuthSession) = SimpleOAuthSessionService.sessions.put(key, session)

  def deleteSession(key: String) = SimpleOAuthSessionService.sessions.remove(key)
}

class OAuthRouteConfig(val baseHostPort: String, val sessions: OAuthSessionService) {}

trait OAuthRouteHandler {
  def exec(session: OAuthSession, args: Map[String, String]): RouteResponse
}

class OAuthGetRestRoute(config: OAuthRouteConfig, route: String, handler: OAuthRouteHandler) extends OAuthRestRoute(route, handler, HttpMethod.GET, config) {}

class OAuthPostRestRoute(config: OAuthRouteConfig, route: String, handler: OAuthRouteHandler) extends OAuthRestRoute(route, handler, HttpMethod.POST, config) {}

class OAuthPutRestRoute(config: OAuthRouteConfig, route: String, handler: OAuthRouteHandler) extends OAuthRestRoute(route, handler, HttpMethod.PUT, config) {}

class OAuthDeleteRestRoute(config: OAuthRouteConfig, route: String, handler: OAuthRouteHandler) extends OAuthRestRoute(route, handler, HttpMethod.DELETE, config) {}

object OAuthRestRoute {

  val APP_KEY_HEADER = "X-APPKEY"
  val OAuth_MD5_SIGNATURE_HEADER = "X-OAuth-MD5"

  // Authorization=OAuth oauth_consumer_key="cloudcmd", oauth_token="13257392", oauth_signature_method="OAuth-SHA1", oauth_signature="PdeSBeSYKCFxHeNqkojH32IjJP4%3D", oauth_timestamp="1344481924", oauth_nonce="oE9Tkskhi3gqCADgN4AZ3w%3D%3D", oauth_version="1.0"
  // Authorization=OAuth
  //  oauth_consumer_key="cloudcmd",
  //  oauth_token="13257392",
  //  oauth_signature_method="OAuth-SHA1",
  //  oauth_signature="PdeSBeSYKCFxHeNqkojH32IjJP4%3D",
  //  oauth_timestamp="1344481924",
  //  oauth_nonce="oE9Tkskhi3gqCADgN4AZ3w%3D%3D",
  //  oauth_version="1.0"

  // TODO: should we return FluentStringsMap instead of a Map[String, String]
  def validate(config: OAuthRouteConfig, request: HttpRequest): (Boolean, OAuthSession, Map[String, String]) = {
    // get app key from header
    val authorization = request.getHeader(HttpHeaders.Names.AUTHORIZATION)
    if (!authorization.startsWith("OAuth")) return (false, null, null)

    val authKeyPairs = authorization.substring(6).split(",")

    val authMap = Map() ++ authKeyPairs.flatMap{ pair =>
      val parts = pair.split("=")
      if (parts.size != 2) return (false, null, null)
      Map(parts(0).trim -> parts(1).trim.replace("\"", ""))
    }

    if (!authMap.contains("oauth_consumer_key")) return (false, null, null)
    if (!authMap.contains("oauth_token")) return (false, null, null)
    if (!authMap.contains("oauth_timestamp")) return (false, null, null)
    if (!authMap.contains("oauth_nonce")) return (false, null, null)
    if (!authMap.contains("oauth_signature")) return (false, null, null)

    val consumerKey = authMap.get("oauth_consumer_key").getOrElse(null)
    val consumerSession = config.sessions.getSession(consumerKey)
    if (consumerSession == null) return (false, null, null)

    val userKey = authMap.get("oauth_token").getOrElse(null)
    val userSession = config.sessions.getSession(userKey)
    if (userSession == null) return (false, null, null)

    val sigCalc = new OAuthSignatureCalculator(consumerSession.getAsConsumerKey, userSession.getAsRequestToken)

    val method = request.getMethod.getName
    val baseURL = config.baseHostPort + request.getUri
    val timestamp = authMap.get("oauth_timestamp").get.toLong
    val nonce = URLDecoder.decode(authMap.get("oauth_nonce").get, "UTF-8")
    val queryParams = getQueryParams(request)
    val formParams = getFormParams(request)

    val signature = sigCalc.calculateSignature(method, baseURL, timestamp, nonce, queryParams, formParams)
    if (signature == URLDecoder.decode(authMap.get("oauth_signature").get, "UTF-8")) {
      import scala.collection.JavaConversions._
      val args = Map() ++ queryParams.entrySet.flatMap{ entry => Map(entry.getKey -> entry.getValue.get(0)) } ++ formParams.entrySet.flatMap{  entry => Map(entry.getKey -> entry.getValue.get(0)) }
      (true, userSession, args)
    } else {
      (false, null, null)
    }
  }

  def getQueryParams(request: HttpRequest) : FluentStringsMap = {
    val queryParams = RouteUtil.extractQueryParams(request.getUri)
    val map = new FluentStringsMap()
    import scala.collection.JavaConversions._
    queryParams.foreach{ case (key, value) => map.put(key, List(value)) }
    map
  }

  def getFormParams(request: HttpRequest) : FluentStringsMap = {
    val map = new FluentStringsMap()
    if (request.getMethod == HttpMethod.POST || request.getMethod == HttpMethod.PUT) {
      if (request.getHeader("x-streampost") == null) {
        val content = request.getContent()
        if (content.hasArray()) {
          val body = content.array()
          val contentLengthHeader = request.getHeader(HttpHeaders.Names.CONTENT_LENGTH)
          val contentLength = if (contentLengthHeader != null) contentLengthHeader.toInt else body.length
          if (contentLength == body.length) {
            val rawContent = new String(body, "UTF-8")
            val formParams = RouteUtil.extractQueryParams(rawContent)
            import scala.collection.JavaConversions._
            formParams.foreach{ case (key, value) => map.put(key, List(value))}
          }
        }
      }
    }
    map
  }
}

class OAuthRestRoute(route: String, handler: OAuthRouteHandler, method: HttpMethod, protected val config: OAuthRouteConfig) extends Route(route) {

  override
  def isMatch(request: HttpRequest) : Boolean = {
    (super.isMatch(request) && request.getMethod().equals(method))
  }

  override
  def handleUpstream(hash: ChannelHandlerContext, e: ChannelEvent) {
    if (!(e.isInstanceOf[MessageEvent]) || !(e.asInstanceOf[MessageEvent].getMessage.isInstanceOf[HttpRequest])) {
      super.handleUpstream(hash, e)
      return
    }

    val request = e.asInstanceOf[MessageEvent].getMessage.asInstanceOf[HttpRequest]
    if (!super.isMatch(request) || !request.getMethod().equals(method)) {
      super.handleUpstream(hash, e)
      return
    }

    val (isValid, session, args) = OAuthRestRoute.validate(config, request)

    val response = if (isValid) {
      try {
        val path = RouteUtil.parsePath(request.getUri())

        import scala.collection.JavaConversions._
        val handlerArgs = args ++ RouteUtil.extractPathArgs(_route, path)

        val routeResponse = handler.exec(session, handlerArgs)
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

          writeResponse(request, routeResponse, e)
          null
        }
      } catch {
        case e: Exception => new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
      }
    } else {
      new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.FORBIDDEN)
    }

    if (response != null) writeResponse(request, response, e)
  }

  def writeResponse(request: HttpRequest, response: HttpResponse, e: ChannelEvent) : ChannelFuture = {
    val writeFuture = e.getChannel().write(response)
    if (response.getStatus != HttpResponseStatus.OK || !isKeepAlive(request)) {
      writeFuture.addListener(ChannelFutureListener.CLOSE)
    }
    writeFuture
  }

  def writeResponse(request: HttpRequest, routeResponse: RouteResponse, e: ChannelEvent) {
    val writeFuture = writeResponse(request, routeResponse.HttpResponse, e)
    writeFuture.addListener(new ChannelFutureListener {
      def operationComplete(channelFuture: ChannelFuture) {
        routeResponse.dispose
      }
    })
  }
}
