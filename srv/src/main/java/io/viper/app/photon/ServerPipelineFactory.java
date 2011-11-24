package io.viper.app.photon;


import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import io.viper.net.server.router.HostRouterHandler;

import static org.jboss.netty.channel.Channels.pipeline;

public class ServerPipelineFactory implements ChannelPipelineFactory
{
  private final HostRouterHandler _hostRouterHandler;

  public ServerPipelineFactory(HostRouterHandler hostRouterHandler)
  {
    _hostRouterHandler = hostRouterHandler;
  }

  public ChannelPipeline getPipeline()
    throws Exception
  {
    ChannelPipeline pipeline = pipeline();
    pipeline.addLast("decoder", new HttpRequestDecoder());
    pipeline.addLast("encoder", new HttpResponseEncoder());
    pipeline.addLast("hostrouter", _hostRouterHandler);
    return pipeline;
  }
}
