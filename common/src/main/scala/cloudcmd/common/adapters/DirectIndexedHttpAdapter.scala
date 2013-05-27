package cloudcmd.common.adapters

import org.json.{JSONArray, JSONObject}
import cloudcmd.common.{FileMetaData, ContentAddressableStorage}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import java.net.URI

class DirectIndexedHttpAdapter extends DirectHttpAdapter with IndexedAdapter {

  var _urlFind : String = null

  override def buildUrls() {
    super.buildUrls()
    _urlFind = "%s/find".format(_host)
  }

  /** *
    * Refresh the storage index, which may be time consuming
    */
  def reindex(cas: ContentAddressableStorage) {}

  /** *
    * Flush the index cache that may be populated during a series of modifications (e.g. store)
    */
  def flushIndex() {}

  /**
   * Find a set of meta blocks based on a filter.
   * @param filter
   * @return a set of meta blocks
   */
  def find(filter: JSONObject = new JSONObject()): Seq[FileMetaData] = {
    val response = asyncHttpClient
      .preparePost(_urlFind)
      .addParameter("filter", filter.toString)
      .execute
      .get

    if (response.getStatusCode != HttpResponseStatus.OK.getCode) {
      throw new RuntimeException("failed calling find with filter=%s".format(filter.toString))
    }

    val rawJson = response.getResponseBody("UTF-8")
    FileMetaData.fromJsonArray(new JSONArray(rawJson))
  }
}
