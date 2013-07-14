package cloudcmd.cld.commands

import cloudcmd.common.util.{CryptoUtil, JsonUtil}
import jpbetz.cli.{Command, CommandContext, Opt, SubCommand}
import cloudcmd.cld.{AdapterUtil, CloudServices}
import cloudcmd.common.{FileUtil, IndexedContentAddressableStorage, FileMetaData}
import java.io.{File, FileInputStream, InputStream}
import com.ning.http.client.AsyncHttpClient
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import java.net.URI
import com.ning.http.client.oauth.{OAuthSignatureCalculator, RequestToken, ConsumerKey}
import cloudcmd.common.adapters.IndexedAdapter

@SubCommand(name = "share", description = "Share files from storage.")
class Share extends Command {

  @Opt(opt = "b", longOpt = "blocks", description = "remove associated blocks", required = false) private var _removeBlockHashes: Boolean = false
  @Opt(opt = "i", longOpt = "input", description = "input file", required = false) private var _inputFilePath: String = null
  @Opt(opt = "d", longOpt = "sharee", description = "sharee ownerId", required = false) private var _shareeId: String = null
  @Opt(opt = "g", longOpt = "metaHashId", description = "get shared file by meta hash id", required = false) private var _getShares: Boolean = false
  @Opt(opt = "t", longOpt = "thumb", description = "get thumbnail for shared file by meta hash id", required = false) private var _getThumb: Boolean = false

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) var _uri: String = null

  def exec(p1: CommandContext) {
    AdapterUtil.exec(p1, _uri, _minTier, _maxTier, doCommand)
  }

  def doCommand(adapter: IndexedContentAddressableStorage) {
    val urlPrefix = if (_uri == null) "http" else _uri

    CloudServices.ConfigService.findIndexedAdapterByBestMatch(urlPrefix) match {
      case Some(adapter) => {
        val asyncHttpClient = new AsyncHttpClient()
        try {
          val URI = adapter.URI
          initOAuthInfo(URI, asyncHttpClient)
          val host = "http://%s:%d".format(URI.getHost, URI.getPort)

          if (_shareeId != null) {
            doShareCommand(host, adapter, asyncHttpClient)
          } else if (_getShares) {
            doGetShareCommand(host, adapter, asyncHttpClient)
          } else if (_getThumb) {
            doGetThumbCommand(host, adapter, asyncHttpClient)
          }
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

  def doGetShareCommand(host: String, adapter: IndexedAdapter, asyncHttpClient: AsyncHttpClient) {
    val selections = getSelections
    val cwd = FileUtil.getCurrentWorkingDirectory

    if (selections.size > 0) {
      selections foreach { fmd =>
        System.err.println("getting share %s from adapter: %s".format(fmd.getFilename, adapter.getSignature))
        val url = "%s/files/%s".format(host, fmd.getHash)

        val response = asyncHttpClient
          .prepareGet(url)
          .execute
          .get

        // TODO: use boolean or custom exception
        if (response.getStatusCode == HttpResponseStatus.OK.getCode) {
          val blockHash = fmd.getBlockHashes(0)

          var remoteData: InputStream = null
          try {
            remoteData = response.getResponseBodyAsStream

            // prefix path to be cwd based
            var path = fmd.getURI.getPath
            path = cwd + (if (path.startsWith(File.separator)) path else File.separator + path)

            val destFile = new File(path)
            destFile.getParentFile.mkdirs

            val remoteDataHash = CryptoUtil.digestToString(CryptoUtil.writeAndComputeHash(remoteData, destFile))
            if (remoteDataHash.equals(blockHash)) {
              onMessage("retrieved: %s".format(fmd.getPath))
            } else {
              onMessage("%s is corrupted after download using block %s".format(fmd.getPath, blockHash))
              destFile.delete
            }
          } catch {
            case e: Exception => {
              onMessage("%s failed to read block %s".format(fmd.getPath, blockHash))
            }
          } finally {
            FileUtil.SafeClose(remoteData)
          }
        } else {
          System.err.println("failed to fetch %s (%s)".format(fmd.getFilename, fmd.getHash))
        }
      }
    } else {
      System.err.println("nothing to do.")
    }
  }

  def doGetThumbCommand(host: String, adapter: IndexedAdapter, asyncHttpClient: AsyncHttpClient) {
    val selections = getSelections
    val cwd = FileUtil.getCurrentWorkingDirectory

    if (selections.size > 0) {
      selections foreach { fmd =>
        System.err.println("getting thumb for %s from adapter: %s".format(fmd.getFilename, adapter.getSignature))
        val url = "%s/files/thumb/%s".format(host, fmd.getHash)

        val response = asyncHttpClient
          .prepareGet(url)
          .execute
          .get

        // TODO: use boolean or custom exception
        if (response.getStatusCode == HttpResponseStatus.OK.getCode) {
          val blockHash = fmd.getBlockHashes(0)

          var remoteData: InputStream = null
          try {
            remoteData = response.getResponseBodyAsStream

            // prefix path to be cwd based
            var path = fmd.getURI.getPath
            path = cwd + (if (path.startsWith(File.separator)) path else File.separator + path) + ".thumb.jpg"

            val destFile = new File(path)
            destFile.getParentFile.mkdirs

            FileUtil.writeFile(remoteData, destFile.getAbsolutePath)
            onMessage("retrieved: %s".format(path))
          } catch {
            case e: Exception => {
              onMessage("%s failed to read block %s".format(fmd.getPath, blockHash))
            }
          } finally {
            FileUtil.SafeClose(remoteData)
          }
        } else {
          System.err.println("failed to fetch %s (%s)".format(fmd.getFilename, fmd.getHash))
        }
      }
    } else {
      System.err.println("nothing to do.")
    }
  }

  def onMessage(msg: String) {
    System.err.println(msg)
  }

  def doShareCommand(host: String, adapter: IndexedAdapter, asyncHttpClient: AsyncHttpClient) {
    System.err.println("sharing with adapter: %s".format(adapter.getSignature))
    val url = "%s/files/share".format(host)
    val selections = getSelections
    share(url, selections, asyncHttpClient)
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
      is = if (_inputFilePath != null) {
        new FileInputStream(new File(_inputFilePath))
      } else {
        System.in
      }
      FileMetaData.fromJsonArray(JsonUtil.loadJsonArray(is))
    } finally {
      if (is ne System.in) is.close()
    }
  }
}