package cloudcmd.srv;


import cloudcmd.common.ResourceUtil;
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
import ops.MemoryElement;
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

  private final static int MAX_FILE_SIZE = (1024 * 1024) * 1024;

  private Thread _opsThread = null;

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

    new File(fileStorageRoot).mkdir();

    final OPS ops = initializeOps();

    cloudCmdServer._opsThread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        ops.run();
      }
    });
    cloudCmdServer._opsThread.start();

    ChannelPipelineFactory channelPipelineFactory =
      new CloudCmdServerChannelPipelineFactory(
        ops,
        MAX_FILE_SIZE,
        staticFileRoot,
        fileStorageRoot,
        localhostName);

    cloudCmdServer._bootstrap.setPipelineFactory(channelPipelineFactory);
    cloudCmdServer._bootstrap.bind(new InetSocketAddress(port));

    return cloudCmdServer;
  }

  public void shutdown()
  {
    if (_opsThread != null)
    {
      _opsThread.interrupt();
      try
      {
        _opsThread.join(1000);
      }
      catch (InterruptedException e)
      {
      }
    }
  }

  private static OPS initializeOps() throws IOException, JSONException
  {
    Map<String, ops.Command> registry = OpsFactory.getDefaultRegistry();

    // TODO:
    //
    //  extract metadata from file and add back into ops
    //    calculate hash, get filename and extension, path, size
    //  add filetype specific processors in ops
    //  write two files:
    //    original file data with the name of the file as the hash
    //    meta data file
    //  write to log
    //
    // add log endpoint in server
    // add log follower in client
    // add reindex command cli/srv
    // add search in cli/srv
    // add fetch cmd in cli/srv
    //

    registry.put("index", new ops.Command()
    {
      @Override
      public void exec(ops.CommandContext context, Object[] args)
      {
        File file = (File) args[0];
        System.out.println("srv processing: " + file.getAbsolutePath());
      }
    });

    OPS ops = OpsFactory.create(registry, ResourceUtil.loadOps("index.ops"));
    ops.waitForWork(true);

    return ops;
  }

  private static class CloudCmdServerChannelPipelineFactory implements ChannelPipelineFactory
  {
    private final int _maxContentLength;
    private final String _fileStorageRoot;
    private final String _staticFileRoot;
    private final FileContentInfoProvider _staticFileProvider;
    private final FileContentInfoProvider _fileStorageProvider;
    private final String _downloadHostname;
    private final OPS _ops;

    public CloudCmdServerChannelPipelineFactory(
      OPS ops,
      int maxContentLength,
      String staticFileRoot,
      String uploadFileRoot,
      String downloadHostname)
      throws IOException, JSONException
    {
      _ops = ops;
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

      routes.add(new PostRoute("/cas/", new RouteHandler()
      {
        @Override
        public HttpResponse exec(Map<String, String> args) throws Exception
        {
          if (!args.containsKey("rawFile"))
          {
            return Util.createJsonResponse("status", "false", "error", "missing rawFile argument");
          }

          File file = new File(args.get("rawFile"));
          System.out.println("adding: " + file.getAbsolutePath());
          _ops.make(new MemoryElement("rawFile", "name", file.getName(), "file", file));

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
