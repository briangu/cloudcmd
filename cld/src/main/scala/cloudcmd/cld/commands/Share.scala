package cloudcmd.cld.commands

import cloudcmd.common.util.JsonUtil
import jpbetz.cli.{Command, CommandContext, Opt, SubCommand}
import cloudcmd.cld.{AdapterUtil, CloudServices}
import cloudcmd.common.{IndexedContentAddressableStorage, FileMetaData}
import java.io.{File, FileInputStream, InputStream}
import com.ning.http.client.AsyncHttpClient
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import java.net.URI
import com.ning.http.client.oauth.{OAuthSignatureCalculator, RequestToken, ConsumerKey}

@SubCommand(name = "share", description = "Share files from storage.")
class Share extends Command {

  @Opt(opt = "b", longOpt = "blocks", description = "remove associated blocks", required = false) private var _removeBlockHashes: Boolean = false
  @Opt(opt = "i", longOpt = "input", description = "input file", required = false) private var _inputFilePath: String = null
  @Opt(opt = "d", longOpt = "sharee", description = "sharee ownerId", required = false) private var _shareeId: String = null

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) var _uri: String = null

  def exec(p1: CommandContext) {
    AdapterUtil.exec(p1, _uri, _minTier, _maxTier, doCommand)
  }

  def doCommand(adapter: IndexedContentAddressableStorage) {
    val urlPrefix = if (_uri == null) "http" else _uri

    CloudServices.ConfigService.findAdapterByBestMatch(urlPrefix) match {
      case Some(adapter) => {
        System.err.println("sharing with adapter: %s".format(adapter.getSignature))

        val selections = adapter.find() //getSelections

        val URI = adapter.URI
        val host = "http://%s:%d".format(URI.getHost, URI.getPort)
        val shareUrl = "%s/share".format(host)
        val asyncHttpClient = new AsyncHttpClient()
        try {
          initOAuthInfo(URI, asyncHttpClient)
          share(shareUrl, selections, asyncHttpClient)
        } finally {
          asyncHttpClient.close()
        }
      }
      case None => {
        System.err.println("could not find an adapter suitable for sharing.")
      }
    }
  }

  def initOAuthInfo(adapterUri: URI, asyncHttpClient: AsyncHttpClient): Boolean = {
    val parts = adapterUri.getAuthority.split("@")
    if (parts.length != 2) return false
    val keys = parts(0).split(':')
    if (keys.length != 4) return false
    val consumerKey = new ConsumerKey(keys(0), keys(1))
    val requestToken = new RequestToken(keys(2), keys(3))
    asyncHttpClient.setSignatureCalculator(new OAuthSignatureCalculator(consumerKey, requestToken))
    true
  }

  def share(url: String, selections: Iterable[FileMetaData], asyncHttpClient: AsyncHttpClient) {
    selections foreach { fmd =>
      try {
        val response = asyncHttpClient
          .preparePost(url)
          .addParameter("metaHashId", fmd.getHash)
          .addParameter("destOwnerId", _shareeId)
          .execute
          .get

        // TODO: use boolean or custom exception
        if (response.getStatusCode == HttpResponseStatus.OK.getCode) {
          System.err.println("shared %s to %s".format(fmd.getHash, _shareeId))
        } else {
          System.err.println("failed to share %s to %s".format(fmd.getHash, _shareeId))
        }
      } catch {
        case e: Exception => {
          System.err.println("failed to share %s to %s".format(fmd.getHash, _shareeId))
        }
      }
    }
  }

  def getSelections: Iterable[FileMetaData] = {
    var is: InputStream = null
    try {
      is = if ((_inputFilePath != null)) new FileInputStream(new File(_inputFilePath)) else System.in
      FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(System.in))
    } finally {
      if (is ne System.in) is.close()
    }
  }
}