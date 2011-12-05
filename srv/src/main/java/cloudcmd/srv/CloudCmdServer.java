package cloudcmd.srv;


import cloudcmd.common.MetaUtil;
import cloudcmd.common.StringUtil;
import cloudcmd.common.engine.CloudEngineService;
import cloudcmd.common.index.IndexStorageService;
import io.viper.core.server.Util;
import io.viper.core.server.file.FileContentInfoProvider;
import io.viper.core.server.file.StaticFileContentInfoProvider;
import io.viper.core.server.file.StaticFileServerHandler;
import io.viper.core.server.router.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;


public class CloudCmdServer
{
  private ServerBootstrap _bootstrap;

  private final static int MAX_FILE_SIZE = (1024 * 1024) * 1024;

  public static CloudCmdServer create(
    String localhostName,
    int port,
    String staticFileRoot)
    throws IOException, JSONException
  {
    CloudCmdServer cloudCmdServer = new CloudCmdServer();

    cloudCmdServer._bootstrap =
      new ServerBootstrap(
        new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool()));

    ChannelPipelineFactory channelPipelineFactory =
      new CloudCmdServerChannelPipelineFactory(MAX_FILE_SIZE, staticFileRoot, localhostName);

    cloudCmdServer._bootstrap.setPipelineFactory(channelPipelineFactory);
    cloudCmdServer._bootstrap.bind(new InetSocketAddress(port));

    return cloudCmdServer;
  }

  public void shutdown()
  {
  }

  private static class CloudCmdServerChannelPipelineFactory implements ChannelPipelineFactory
  {
    private final int _maxContentLength;
    private final String _staticFileRoot;
    private final FileContentInfoProvider _staticFileProvider;
    private final String _downloadHostname;

    public CloudCmdServerChannelPipelineFactory(
      int maxContentLength,
      String staticFileRoot,
      String downloadHostname)
      throws IOException, JSONException
    {
      _maxContentLength = maxContentLength;
      _staticFileRoot = staticFileRoot;
      _downloadHostname = downloadHostname;

      _staticFileProvider = StaticFileContentInfoProvider.create(_staticFileRoot);
    }

    @Override
    public ChannelPipeline getPipeline()
      throws Exception
    {
      List<Route> routes = new ArrayList<Route>();

      routes.add(new PostRoute("/cas/", new RouteHandler()
      {
        @Override
        public HttpResponse exec(Map<String, String> args) throws Exception
        {
          if (!args.containsKey("rawFile"))
          {
            return Util.createJsonResponse("status", "false", "error", "missing rawFile argument");
          }

          return Util.createJsonResponse("status", "true", "rawFile", args.get("rawFile"));
        }
      }));

      routes.add(new GetRoute("/cas/$var", new RouteHandler()
      {
        @Override
        public HttpResponse exec(Map<String, String> args) throws Exception
        {
          JSONObject obj = new JSONObject();
          obj.put("status", "woot!");
          obj.put("var", args.get("var"));
          HttpResponse response = Util.createJsonResponse(obj);
          return response;
        }
      }));

      routes.add(new GetRoute("/cld/ls", new RouteHandler()
      {
        @Override
        public HttpResponse exec(Map<String, String> args) throws Exception
        {
          JSONObject filter = new JSONObject();

/*
          if (_tags != null)
          {
            Set<String> tags = MetaUtil.prepareTags(_tags);
            if (tags.size() > 0) filter.put("tags", StringUtil.join(tags, " "));
          }

          if (_path != null) filter.put("path", _path);
          if (_filename != null) filter.put("filename", _filename);
          if (_fileext != null) filter.put("fileext", _fileext);
*/

          JSONArray result = IndexStorageService.instance().find(filter);

          JSONObject jsonResponse = new JSONObject();
          jsonResponse.put("status", "true");
          jsonResponse.put("array", result);

          HttpResponse response = Util.createJsonResponse(jsonResponse);
          return response;
        }
      }));

//      routes.add(new GetRoute("/d/$path", new StaticFileServerHandler(_fileStorageProvider)));
      routes.add(new GetRoute("/$path", new StaticFileServerHandler(_staticFileProvider)));

      ChannelPipeline lhPipeline = new DefaultChannelPipeline();
      lhPipeline.addLast("decoder", new HttpRequestDecoder());
      lhPipeline.addLast("encoder", new HttpResponseEncoder());
      lhPipeline.addLast("router", new RouterMatcherUpstreamHandler("uri-handlers", routes));

      return lhPipeline;
    }
  }
}
