package cloudcmd.common.srv

import io.viper.core.server.router.{StatusResponse, RouteResponse}
import org.jboss.netty.handler.codec.http.{HttpHeaders, HttpVersion, DefaultHttpResponse, HttpResponseStatus}
import cloudcmd.common._
import io.viper.core.server.router.RouteResponse.RouteResponseDispose
import cloudcmd.common.adapters.DataNotFoundException
import io.viper.common.ViperServer
import scala.Some
import cloudcmd.common.util.{FileTypeUtil, StreamUtil}
import org.json.JSONObject
import java.io.{InputStream, ByteArrayOutputStream, ByteArrayInputStream}
import javax.imageio.ImageIO
import com.thebuzzmedia.imgscalr.{Scalr, AsyncScalr}

object ThumbnailService {
  def start() {
    AsyncScalr.setServiceThreadCount(2) // TODO: set via config
  }

  def shutdown() {
    if (AsyncScalr.getService != null) {
      AsyncScalr.getService.shutdownNow
    }
  }
}

class ThumbnailService(cas: ContentAddressableStorage, config: OAuthRouteConfig, thumbCAS: ContentAddressableStorage) {

  final val THUMBNAIL_CREATE_THRESHOLD = 128 * 1024 // TODO: come from config
  final val THUMB_WIDTH = 512
  final val THUMB_HEIGHT = 512

  private def getThumbMimeType(meta: FileMetaData): (String, BlockContext) = {
    val mimeType = FileTypeUtil.instance.getTypeFromExtension(meta.getFileExt)
    if (mimeType == null || !mimeType.startsWith("image")) {
      ("image/png", new BlockContext("default.thumb", Set()))
    } else {
      (mimeType, new BlockContext("%s.thumb".format(meta.getBlockHashes(0))))
    }
  }

  private def loadMeta(ctx: BlockContext): Option[FileMetaData] = {
    try {
      val rawString = StreamUtil.spoolStreamToString(cas.load(ctx)._1)
      val rawData = new JSONObject(rawString)
      Some(FileMetaData.create(ctx.hash, rawData))
    } catch {
      case e: Exception => {
        None
      }
    }
  }

  private def buildDownloadResponse(cas: ContentAddressableStorage, ctx: BlockContext, contentType: String, fileName: String = null) : RouteResponse = {
    try {
      val (is, length) = cas.load(ctx)
      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      response.setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType)
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, length)
      response.setHeader(HttpHeaders.Names.EXPIRES, "Expires: Thu, 29 Oct 2020 17:04:19 GMT")
      if (fileName != null) {
        response.setHeader("Content-disposition", "attachment; filename=%s".format(fileName))
      }
      response.setContent(new FileChannelBuffer(is, length))
      new RouteResponse(response, new RouteResponseDispose {
        def dispose() {
          is.close
        }
      })
    } catch {
      case e: DataNotFoundException => {
        new StatusResponse(HttpResponseStatus.NOT_FOUND)
      }
    }
  }

  private def createNewThumbnail(srcCAS: ContentAddressableStorage, shareCtx: BlockContext, thumbCAS: ContentAddressableStorage, thumbCtx: BlockContext): Option[BlockContext] = {
    try {
      val (is, length) = srcCAS.load(shareCtx)

      try {
        createThumbnail(is, THUMB_HEIGHT, THUMB_WIDTH) match {
          case Some(thumbBytes) => {
            thumbCAS.store(thumbCtx, new ByteArrayInputStream(thumbBytes))
            Some(thumbCtx)
          }
          case None => {
            // failed to create a thumbnail
            None
          }
        }
      } finally {
        is.close()
      }
    }
  }

  def createThumbnail(is : InputStream, thumbWidth: Int, thumbHeight: Int) : Option[Array[Byte]] = {
    val os = new ByteArrayOutputStream()
    try {
      val image = ImageIO.read(is)
      if (image != null) {
        val future =
          AsyncScalr.resize(
            image,
            Scalr.Method.BALANCED,
            Scalr.Mode.FIT_TO_WIDTH,
            thumbWidth,
            thumbHeight,
            Scalr.OP_ANTIALIAS)

        val thumbnail = future.get()
        if (thumbnail != null) {
          ImageIO.write(thumbnail, "jpg", os)
        }
        Some(os.toByteArray)
      } else {
        None
      }
    }
    catch {
      case e: Exception => {
        //        log.info("failed to create thumbnail for " + srcFile.getPath)
        null
      }
    }
  }

  def addRoutes(server: ViperServer) {
    server.addRoute(new OAuthGetRestRoute(config, "/files/thumb/$key", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val ownerId = session.getAsRequestToken.getKey()

        val srcFmd = args.get("key") match {
          case Some(hashId) => loadMeta(new BlockContext(hashId, ownerId = Some(ownerId)))
          case None => None
        }

        srcFmd match {
          case Some(fmd) => {
            try {
              if (fmd.isOwner(ownerId) || fmd.isPublic) {
                fmd.getCreatorId match {
                  case Some(creatorId) => {
                    if (fmd.getBlockHashes.length == 1) {
                      val (mimeType, thumbCtx) = getThumbMimeType(fmd)

                      val downloadCxt = if (thumbCAS.contains(thumbCtx)) {
                        Some(thumbCtx)
                      } else {
                        val blockHash = fmd.getBlockHashes(0)
                        val shareCtx = new BlockContext(blockHash, fmd.getTags, Some(creatorId))
                        createNewThumbnail(cas, shareCtx, thumbCAS, thumbCtx)
                      }

                      downloadCxt match {
                        case Some(ctx) => buildDownloadResponse(thumbCAS, ctx, mimeType, fmd.getFilename)
                        case None => new StatusResponse(HttpResponseStatus.BAD_REQUEST)
                      }
                    } else {
                      // multi-blocks are not yet supported
                      new StatusResponse(HttpResponseStatus.BAD_REQUEST)
                    }
                  }
                  case None => {
                    return new StatusResponse(HttpResponseStatus.UNAUTHORIZED)
                  }
                }
              } else {
                return new StatusResponse(HttpResponseStatus.UNAUTHORIZED)
              }
            } catch {
              case e: Exception => {
                new StatusResponse(HttpResponseStatus.BAD_REQUEST)
              }
            }
          }
          case None => {
            new StatusResponse(HttpResponseStatus.NOT_FOUND)
          }
        }
      }
    }))
  }
}
