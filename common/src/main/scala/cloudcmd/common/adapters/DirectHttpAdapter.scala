package cloudcmd.common.adapters

import cloudcmd.common.BlockContext
import java.io.InputStream
import com.ning.http.client.AsyncHttpClient
import org.jboss.netty.handler.codec.http.{HttpResponseStatus, HttpHeaders}
import org.json.JSONArray
import java.net.URI
import com.ning.http.client.oauth.{RequestToken, ConsumerKey, OAuthSignatureCalculator}

// http://consumerKey:consumerSecret:userKey:userSecret@host:port/<path>

class DirectHttpAdapter extends DirectAdapter {

  val asyncHttpClient = new AsyncHttpClient()

  var _host: String = null
  var _urlPing : String = null
  var _urlContainsAll : String = null
  var _urlRemoveAll : String = null
  var _urlEnsureAll : String = null
  var _urlDescribe : String = null

  protected def buildUrls() {
    _urlPing = "%s/ping".format(_host)
    _urlContainsAll = "%s/blocks/containsAll".format(_host)
    _urlRemoveAll = "%s/blocks/removeAll".format(_host)
    _urlEnsureAll = "%s/blocks/ensureAll".format(_host)
    _urlDescribe = "%s/blocks".format(_host)
  }

  override def init(configDir: String, tier: Int, adapterType: String, acceptsTags: Set[String], uri: URI) {
    super.init(configDir, tier, adapterType, acceptsTags, uri)
    _host = "http://%s:%d".format(URI.getHost, URI.getPort)
    initOAuthInfo(URI)
    buildUrls()

    _isOnline = try {
      ping
    } catch {
      case e: Exception => {
        e.printStackTrace()
        false
      }
    }
  }

  def ping: Boolean = {
    val response = asyncHttpClient
      .prepareGet(_urlPing)
      .execute
      .get
    response.getStatusCode == HttpResponseStatus.OK.getCode
  }

  def initOAuthInfo(adapterUri: URI): Boolean = {
    val parts = adapterUri.getAuthority.split("@")
    if (parts.length != 2) return false
    val keys = parts(0).split(':')
    if (keys.length != 4) return false
    val consumerKey = new ConsumerKey(keys(0), keys(1))
    val requestToken = new RequestToken(keys(2), keys(3))
    asyncHttpClient.setSignatureCalculator(new OAuthSignatureCalculator(consumerKey, requestToken))
    true
  }

  def shutdown() {
    asyncHttpClient.close()
  }

  /***
   * Gets if the CAS contains the specified blocks.
   * @param ctxs
   * @return
   */
  def containsAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    val arr = new JSONArray
    ctxs.foreach(ctx => arr.put(ctx.toJson))

    val response = asyncHttpClient
      .preparePost(_urlContainsAll)
      .addParameter("ctxs", arr.toString)
      .execute
      .get

    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("failed calling containsAll")
    ctxsResponseArrToMap(new JSONArray(response.getResponseBody("UTF-8")))
  }

  /***
   * Removes the specified blocks.
   * @param ctxs
   * @return
   */
  def removeAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    val arr = new JSONArray
    ctxs.foreach(ctx => arr.put(ctx.toJson))

    val response = asyncHttpClient
      .preparePost(_urlRemoveAll)
      .addParameter("ctxs", arr.toString)
      .execute
      .get

    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("failed calling removeAll")
    ctxsResponseArrToMap(new JSONArray(response.getResponseBody("UTF-8")))
  }

  /***
   * Ensure block level consistency with respect to the CAS implementation
   * @param ctxs
   * @param blockLevelCheck
   * @return
   */
  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean) : Map[BlockContext, Boolean] = {
    val arr = new JSONArray
    ctxs.foreach(ctx => arr.put(ctx.toJson))

    val response = asyncHttpClient
      .preparePost(_urlEnsureAll)
      .addParameter("ctxs", arr.toString)
      .execute
      .get

    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("failed calling ensureAll")
    ctxsResponseArrToMap(new JSONArray(response.getResponseBody("UTF-8")))
  }

  protected def ctxsResponseArrToMap(arr: JSONArray) : Map[BlockContext, Boolean] = {
    Map() ++ (0 until arr.length).par.flatMap{
      idx =>
        val obj = arr.getJSONObject(idx)
        Map(BlockContext.fromJson(obj) -> obj.getBoolean("_status"))
    }
  }

  /***
   * Store the specified block in accordance with the CAS implementation.
   * @param ctx
   * @param is
   * @return
   */
  def store(ctx: BlockContext, is: InputStream) {
    val response = asyncHttpClient
      .preparePost("%s/blocks/%s,%s".format(_host, ctx.hash, ctx.routingTags.mkString(",")))
      .setHeader("x-streampost", "true")
      .setBody(is)
      .execute
      .get
    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.CREATED.getCode) throw new RuntimeException("failed to store " + ctx)
  }

  /***
   * Load the specified block from the CAS.
   * @param ctx
   * @return
   */
  def load(ctx: BlockContext) : (InputStream, Int) = {
    val response = asyncHttpClient
      .prepareGet("%s/blocks/%s,%s".format(_host, ctx.hash, ctx.routingTags.mkString(",")))
      .execute
      .get
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) {
      throw new DataNotFoundException(ctx)
    }
    (response.getResponseBodyAsStream, response.getHeader(HttpHeaders.Names.CONTENT_LENGTH).toInt)
  }

  /***
   * List all hashes stored in the CAS without regard to block context.  There may be hashes stored in the CAS which are
   * not returned in describe(), so this method can help identify unreferenced blocks.
   * @return
   */
  def describe(ownerId: Option[String] = None): Set[String] = {
    val response = asyncHttpClient
      .prepareGet(_urlDescribe)
      .execute
      .get
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("unable to describe")
    val arr = new JSONArray(response.getResponseBody("UTF-8"))
    Set() ++ (0 until arr.length).par.map(idx => arr.getString(idx))
  }
}
