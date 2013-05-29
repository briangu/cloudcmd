package cloudcmd.common.srv

import cloudcmd.common._
import io.viper.common.ViperServer
import io.viper.core.server.router.{StatusResponse, RouteResponse}
import org.jboss.netty.handler.codec.http.{HttpHeaders, HttpVersion, DefaultHttpResponse, HttpResponseStatus}
import cloudcmd.common.util.StreamUtil
import org.json.JSONObject
import java.io.ByteArrayInputStream
import io.viper.core.server.router.RouteResponse.RouteResponseDispose
import scala.Some
import cloudcmd.common.adapters.DataNotFoundException

class FileServices(cas: IndexedContentAddressableStorage, config: OAuthRouteConfig) {

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

  def addRoutes(server: ViperServer) {

    // required:
    // src meta hash id
    // dest ownerId
    // operation:
    //  load meta from cas
    //  verify creatorId of meta == current session ownerId
    //  create new fmd with dest as ownerId, src ownerId as creatorId
    //  create new block context with dest ownerId
    //  store new fmd in dest store
    server.addRoute(new OAuthPostRestRoute(config, "/files/share", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        val srcCreatorId = session.getAsRequestToken.getKey()
        val destOwnerId = args.get("destOwnerId") match {
          case Some(id) => {
            // verify that the dest user exists
            if (SimpleOAuthSessionService.instance.isValidKey(id)) {
              id
            } else {
              return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            }
          }
          case None => {
            return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
          }
        }

        val srcFmd = args.get("metaHashId") match {
          case Some(hashId) => loadMeta(new BlockContext(hashId, ownerId = Some(srcCreatorId)))
          case None => None
        }

        srcFmd match {
          case Some(fmd) => {
            try {
              if (fmd.isCreator(srcCreatorId) || fmd.isPublic) {
                fmd.getProperties.put("ownerId", destOwnerId)
                fmd.getProperties.put("creatorId", srcCreatorId)
                val rawBytes = new ByteArrayInputStream(fmd.getRawData.toString.getBytes("UTF-8"))
                rawBytes.mark(0)
                val shareFmd = FileMetaData.create(rawBytes, fmd.getRawData)
                val shareCtx = new BlockContext(shareFmd.getHash, fmd.getTags, Some(destOwnerId))
                rawBytes.reset()
                cas.store(shareCtx, rawBytes)
                new StatusResponse(HttpResponseStatus.OK)
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

    server.addRoute(new OAuthGetRestRoute(config, "/files/$key", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        new StatusResponse(HttpResponseStatus.NOT_FOUND)
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
                      val blockHash = fmd.getBlockHashes(0)
                      val shareCtx = new BlockContext(blockHash, fmd.getTags, Some(creatorId))
                      val mimeType = "application/octet-stream" // for downloading
                      buildDownloadResponse(cas, shareCtx, mimeType, fmd.getFilename)
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

    server.addRoute(new OAuthGetRestRoute(config, "/files/thumb/$key", new OAuthRouteHandler {
      def exec(session: OAuthSession, args: Map[String, String]): RouteResponse = {
        new StatusResponse(HttpResponseStatus.NOT_FOUND)
      }
    }))
  }
}
