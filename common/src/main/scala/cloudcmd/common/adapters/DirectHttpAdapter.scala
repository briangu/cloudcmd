package cloudcmd.common.adapters

import cloudcmd.common.BlockContext
import java.io.InputStream
import com.ning.http.client.AsyncHttpClient
import org.jboss.netty.handler.codec.http.{HttpResponseStatus, HttpHeaders}
import org.json.JSONArray

class DirectHttpAdapter(host: String) extends Adapter {

  val asyncHttpClient = new AsyncHttpClient()

  /***
   * Refresh the internal cache, which may be time consuming
   */
  def refreshCache() {
    val response = asyncHttpClient
      .preparePost("/refreshCache")
      .execute
      .get
    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("failed to refresh cache")
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
      .preparePost("/blocks/containsAll")
      .addParameter("ctxs", arr.toString)
      .execute
      .get
    // TODO: use boolean or custom exception
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new RuntimeException("failed calling containsAll")

    val rex = new JSONArray(response.getResponseBody("UTF-8"))
    Map() ++ (0 until rex.length).par.flatMap{
      idx =>
        val obj = rex.getJSONObject(idx)
        Map(BlockContext.fromJson(obj), obj.getBoolean("_status"))
    }
  }

  /***
   * Removes the specified blocks.
   * @param ctxs
   * @return
   */
  def removeAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {
    null
  }

  /***
   * Ensure block level consistency with respect to the CAS implementation
   * @param ctxs
   * @param blockLevelCheck
   * @return
   */
  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean) : Map[BlockContext, Boolean] = {
    null
  }

  /***
   * Store the specified block in accordance with the CAS implementation.
   * @param ctx
   * @param is
   * @return
   */
  def store(ctx: BlockContext, is: InputStream) {
    val response = asyncHttpClient
      .preparePost("/blocks/%s/%s".format(ctx.hash, ctx.routingTags.mkString(",")))
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
      .prepareGet("/blocks/%s/%s".format(ctx.hash, ctx.routingTags.mkString(",")))
      .execute
      .get
    if (response.getStatusCode != HttpResponseStatus.OK.getCode) throw new DataNotFoundException(ctx)
    (response.getResponseBodyAsStream, response.getHeader(HttpHeaders.Names.CONTENT_LENGTH).toInt)
  }

  /***
   * List all the block hashes stored in the CAS.
   * @return
   */
  def describe() : Set[BlockContext] = {
    null
  }

  /***
   * List all hashes stored in the CAS without regard to block context.  There may be hashes stored in the CAS which are
   * not returned in describe(), so this method can help identify unreferenced blocks.
   * @return
   */
  def describeHashes() : Set[String] = {
    null
  }

  def shutdown() {}
}
