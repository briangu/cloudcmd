package io.viper.app.photon;


import io.viper.core.server.Util;
import io.viper.core.server.file.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import io.viper.core.server.router.*;
import ops.OPS;
import ops.OpsFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.json.JSONException;
import org.json.JSONObject;


public class CloudCmdServer
{
  private ServerBootstrap _bootstrap;

  private final static int MAX_FILE_SIZE = (1024*1024)*1024;

  public static CloudCmdServer create(
    String localhostName,
    int port,
    String staticFileRoot,
    String fileStorageRoot)
    throws IOException, JSONException
  {
    CloudCmdServer cloudCmdServer = new CloudCmdServer();

    cloudCmdServer._bootstrap =
        new ServerBootstrap(
            new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));

    String localhost = String.format("http://%s:%s", localhostName, port);

    new File(fileStorageRoot).mkdir();

    ChannelPipelineFactory channelPipelineFactory =
      new CloudCmdServerChannelPipelineFactory(
        MAX_FILE_SIZE,
        staticFileRoot,
        fileStorageRoot,
        localhostName);

    cloudCmdServer._bootstrap.setPipelineFactory(channelPipelineFactory);
    cloudCmdServer._bootstrap.bind(new InetSocketAddress(port));

    return cloudCmdServer;
  }

  private static void initializeOps()
  {
    Map<String, ops.Command> registry = OpsFactory.getDefaultRegistry();

    registry.put("process", new ops.Command() {

      @Override
      public void exec(ops.CommandContext context, Object[] args) {
        File file = (File)args[0];
        System.out.println("processing: " + file.getAbsolutePath());
      }
    });

    final OPS ops = OpsFactory.create(registry, OpsLoader.load("index.ops"));

    ops.run();
  }

  private static class CloudCmdServerChannelPipelineFactory implements ChannelPipelineFactory
  {
    private final int _maxContentLength;
    private final String _fileStorageRoot;
    private final String _staticFileRoot;
    private final FileContentInfoProvider _staticFileProvider;
    private final FileContentInfoProvider _fileStorageProvider;
    private final String _downloadHostname;

    public CloudCmdServerChannelPipelineFactory(
      int maxContentLength,
      String staticFileRoot,
      String uploadFileRoot,
      String downloadHostname)
        throws IOException, JSONException
    {
      _maxContentLength = maxContentLength;
      _fileStorageRoot = uploadFileRoot;
      _staticFileRoot = staticFileRoot;
      _downloadHostname = downloadHostname;

      _staticFileProvider = StaticFileContentInfoProvider.create(_staticFileRoot);
      _fileStorageProvider = new StaticFileContentInfoProvider(_fileStorageRoot);
    }

    @Override
    public ChannelPipeline getPipeline()
        throws Exception
    {
      List<Route> routes = new ArrayList<Route>();

      routes.add(new PostRoute("/cas/", new RouteHandler() {
        @Override
        public HttpResponse exec(Map<String, String> args) {
          return null;
        }
      }));

      routes.add(new GetRoute("/cas/$var", new RouteHandler() {
        @Override
        public HttpResponse exec(Map<String, String> args) throws Exception {
          JSONObject obj = new JSONObject();
          obj.put("status", "woot!");
          obj.put("var", args.get("var"));
          HttpResponse response = Util.createResponse(obj);
          return response;
        }
      }));

      routes.add(new GetRoute("/d/$path", new StaticFileServerHandler(_fileStorageProvider)));
      routes.add(new GetRoute("/$path", new StaticFileServerHandler(_staticFileProvider)));

      ChannelPipeline lhPipeline = new DefaultChannelPipeline();
      lhPipeline.addLast("decoder", new HttpRequestDecoder());
      lhPipeline.addLast("encoder", new HttpResponseEncoder());
      lhPipeline.addLast("router", new RouterMatcherUpstreamHandler("uri-handlers", routes));

      return lhPipeline;
    }
  }
}
