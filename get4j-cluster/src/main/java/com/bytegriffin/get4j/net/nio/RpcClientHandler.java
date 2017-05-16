package com.bytegriffin.get4j.net.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.core.ExceptionCatcher;
import com.bytegriffin.get4j.send.EmailSender;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class RpcClientHandler extends SimpleChannelInboundHandler<Message.Request> {

	private static final Logger logger = LogManager.getLogger(RpcClientHandler.class);


	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Message.Request msg) throws Exception {
		logger.info("客户端["+Session.getIP(ctx)+"]接收数据["+msg.getRequestId()+"]成功。");
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("客户端["+Session.getIP(ctx)+"]连接成功。");
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
		RpcClient.retryConnection();
		logger.info("客户端["+Session.getIP(ctx)+"]连接失败。");
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("客户端发现异常：", cause);
		EmailSender.sendMail(cause);
        ExceptionCatcher.addException(cause);
		ctx.close();
	}

}