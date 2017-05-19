package com.bytegriffin.get4j.net.nio;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;

public final class RpcClient {

	private static final Logger logger = LogManager.getLogger(RpcClient.class);
	private String host;
	private int port;
	private static Bootstrap bootstrap;
	private static Channel channel;
	private static EventLoopGroup worker;
	private static AtomicInteger retryCount;

	private RpcClient(String host, int port) {
		InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
		this.host = host;
		this.port = port;
	}

	static void conn(String host, int port) {
		RpcClient client = new RpcClient(host, port);
		client.init();
	}

	void init() {
		bootstrap = new Bootstrap();
		worker = new NioEventLoopGroup();
		retryCount = new AtomicInteger();
		bootstrap.group(worker);
		bootstrap.channel(NioSocketChannel.class).handler(new LoggingHandler(LogLevel.TRACE));
		bootstrap.option(ChannelOption.SO_KEEPALIVE, true).option(ChannelOption.TCP_NODELAY, true);
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel channel) throws Exception {
				channel.pipeline()
						.addLast(new ProtobufVarint32FrameDecoder())
						.addLast(new ProtobufDecoder(Message.Request.getDefaultInstance()))
						.addLast(new ProtobufVarint32LengthFieldPrepender())
						.addLast(new ProtobufEncoder())
						.addLast(new RpcClientHandler());
			}
		});
		bootstrap.remoteAddress(host, port);
		retryConnection();
	}

	static void close() {
		worker.shutdownGracefully();
	}

	static void retryConnection() {
		if (channel != null && channel.isActive()) {
			return;
		}
		ChannelFuture future = bootstrap.connect().awaitUninterruptibly();
		channel = future.channel();
		future.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture futureListener) throws Exception {
				if (futureListener.isSuccess()) {
					if(retryCount.get() == 0){
						retryCount.incrementAndGet();
						logger.info("客户端连接服务器成功。");
					} else {
						retryCount.set(1);
						logger.info("客户端重新连接服务器成功。");
					}
				} else {
					if(retryCount.get() > 3){
						logger.info("客户端连接服务器失败，并且超出最大连接次数，准备关闭。。。");
						close();
					} else {
						logger.info("客户端连接服务器失败，1秒后将自动重连，重连次数为："+retryCount.get());
						futureListener.channel().eventLoop().schedule(new Runnable() {
							@Override
							public void run() {
								retryCount.incrementAndGet();
								retryConnection();
							}
						}, 1, TimeUnit.SECONDS);
					}
					
				}
			}
		});
	}

	static void send() {
		if(channel != null){
			Message.Request.Builder request = Message.Request.newBuilder();
			request.setRequestId("requestId");
			request.setSeedName("seedname");
			request.setDownloadDisk("/opt/");
			channel.writeAndFlush(request);
		}
	}

	public static void main(String[] args) throws Exception {
		RpcClient.conn(Session.server_ip, Session.server_port);
		for (int i = 0; i < 3; i++) {
			send();
		}
	}

}