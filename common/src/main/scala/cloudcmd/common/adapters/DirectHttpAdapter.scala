package cloudcmd.common.adapters

import java.io.InputStream
import com.ning.http.client.AsyncHttpClient
import org.jboss.netty.handler.codec.http.{HttpResponseStatus, HttpHeaders}
import org.json.{JSONArray, JSONObject}
import java.net.URI
import com.ning.http.client.oauth.{RequestToken, ConsumerKey, OAuthSignatureCalculator}

// http://consumerKey:consumerSecret:userKey:userSecret@host:port/<path>

class DirectHttpAdapter extends Adapter {

  val asyncHttpClient = new AsyncHttpClient()

  var _host: String = null
  var _urlRefreshCache : String = null
  var _urlContainsAll : String = null
  var _urlRemoveAll : String = null
  var _urlEnsureAll : String = null
  var _urlDescribe : String = null
  var _urldescribe : String = null

  private def buildUrls {
    _urlRefreshCache = "%s/refreshCache".format(_host)
    _urlContainsAll = "%s/blocks/containsAll".format(_host)
    _urlRemoveAll = "%s/blocks/removeAll".format(_host)
    _urlEnsureAll = "%s/blocks/ensureAll".format(_host)
    _urlDescribe = "%s/blocks".format(_host)
    _urldescribe = "%s/blocks/hashes".format(_host)
  }

  override def init(configDir: String, tier: Int, adapterType: String, acceptsTags: Set[String], uri: URI) {
    super.init(configDir, tier, adapterType, acceptsTags, uri)
    _host = "http://%s:%d".format(uri.getHost, uri.getPort)
    initOAuthInfo(uri)
    buildUrls
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

  def shutdown {
    asyncHttpClient.close()
  }

  /***
   * Refresh the internal cache, which may be time consuming
   */
  def refreshCache() {
    /*
    val response = asyncHttpClient
      .preparePost(_urlRefreshCache)
      .execute
      .get
    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("failed to refresh cache")
    */
  }

  /***
   * Gets if the CAS contains the specified blocks.
   * @param hashes
   * @return
   */
  def containsAll(hashes: Set[String]) : Map[String, Boolean] = {
    val arr = new JSONArray
    hashes.foreach(hash => arr.put(hash))

    val response = asyncHttpClient
      .preparePost(_urlContainsAll)
      .addParameter("hashes", arr.toString)
      .execute
      .get

    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("failed calling containsAll")
    hashesResponseObjectToBooleanMap(new JSONObject(response.getResponseBody("UTF-8")))
  }

  /***
   * Removes the specified blocks.
   * @param hashes
   * @return
   */
  def removeAll(hashes: Set[String]) : Map[String, Boolean] = {
    val arr = new JSONArray
    hashes.foreach(hash => arr.put(hash))

    val response = asyncHttpClient
      .preparePost(_urlRemoveAll)
      .addParameter("hashes", arr.toString)
      .execute
      .get

    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("failed calling removeAll")
    hashesResponseObjectToBooleanMap(new JSONObject(response.getResponseBody("UTF-8")))
  }

  /***
   * Ensure block level consistency with respect to the CAS implementation
   * @param hashes
   * @param blockLevelCheck
   * @return
   */
  def ensureAll(hashes: Set[String], blockLevelCheck: Boolean) : Map[String, Boolean] = {
    val arr = new JSONArray
    hashes.foreach(hash => arr.put(hash))

    val response = asyncHttpClient
      .preparePost(_urlEnsureAll)
      .addParameter("hashes", arr.toString)
      .execute
      .get

    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("failed calling ensureAll")
    hashesResponseObjectToBooleanMap(new JSONObject(response.getResponseBody("UTF-8")))
  }

  private def hashesResponseObjectToBooleanMap(obj: JSONObject) : Map[String, Boolean] = {
    import scala.collection.JavaConversions._
    Map() ++ obj.keys().toList.asInstanceOf[List[String]].flatMap{key => Map(key -> obj.getBoolean(key))}
  }

  /***
   * Store the specified block in accordance with the CAS implementation.
   * @param hash
   * @param is
   * @return
   */
  def store(hash: String, is: InputStream) {
    val response = asyncHttpClient
      .preparePost("%s/blocks/%s".format(_host, hash))
      .setHeader("x-streampost", "true")
      .setBody(is)
      .execute
      .get
    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.CREATED.getCode) throw new RuntimeException("failed to store " + hash)
  }

  /***
   * Load the specified block from the CAS.
   * @param hash
   * @return
   */
  def load(hash: String) : (InputStream, Int) = {
    val response = asyncHttpClient
      .prepareGet("%s/blocks/%s".format(_host, hash))
      .execute
      .get
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) {
      throw new DataNotFoundException(hash)
    }
    (response.getResponseBodyAsStream, response.getHeader(HttpHeaders.Names.CONTENT_LENGTH).toInt)
  }

  /***
   * List all the block hashes stored in the CAS.
   * @return
   */
  def describe() : Set[String] = {
    val response = asyncHttpClient
      .prepareGet(_urldescribe)
      .execute
      .get
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("unable to describe")
    val arr = new JSONArray(response.getResponseBody("UTF-8"))
    Set() ++ (0 until arr.length).par.map(idx => arr.getString(idx))
  }
}
