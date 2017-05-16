package com.bytegriffin.get4j.net.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.core.ExceptionCatcher;
import com.bytegriffin.get4j.send.EmailSender;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;

public class RpcServer {

	private static final Logger logger = LogManager.getLogger(RpcServer.class);

	static final ServerBootstrap bootstrap = new ServerBootstrap();
	private static int port;

	private RpcServer(){
		InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
	}

	static RpcServer create(int port){
		RpcServer server = new RpcServer();
		server.setPort(port);
		return server;
	}

	public  void setPort(int port) {
		RpcServer.port = port;
	}

	public void run() {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_BACKLOG, 128) 
				.childOption(ChannelOption.SO_KEEPALIVE, true) 
				.childOption(ChannelOption.TCP_NODELAY, true)
				.handler(new LoggingHandler(LogLevel.TRACE))
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline().addLast(new IdleStateHandler(Session.READ_IDEL_TIME_OUT, Session.WRITE_IDEL_TIME_OUT, Session.IDEL_TIME_OUT))
								.addLast(new ProtobufVarint32FrameDecoder()) // 利用包头中的包含数组长度处理半包和粘包
								.addLast(new ProtobufDecoder(Message.Response.getDefaultInstance()))
								.addLast(new ProtobufVarint32LengthFieldPrepender()) // 包头，只包含序列化字节长度
								.addLast(new ProtobufEncoder())
								.addLast(new RpcServerHandler());
					}
			});
			ChannelFuture future = bootstrap.bind(port).sync();
			logger.info("服务端启动成功。");
			future.channel().closeFuture().sync();
		} catch (Exception e) {
			EmailSender.sendMail(e);
	        ExceptionCatcher.addException(e);
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}

	public static void main(String[] args) throws Exception {
		RpcServer.create(Session.server_port).run();
	}

}
