package com.bytegriffin.get4j.net.nio;

import java.util.List;

import com.google.common.collect.Lists;

import io.netty.channel.ChannelHandlerContext;

public class Session {

	
    static final int READ_IDEL_TIME_OUT = 0;  
    static final int WRITE_IDEL_TIME_OUT = 0;  
    static final int IDEL_TIME_OUT = 0;  
    static final String server_ip = "127.0.0.1";
    static final int server_port = 8080;

    static List<String> register_ips = Lists.newArrayList();

    static {
    	registerIP();
    }

    static void registerIP(){
    	register_ips.add("127.0.0.1");
    }

    /**
     * 验证客户端IP是否合法
     * @param ip
     * @return
     */
    static boolean authIP(String ip){
    	return register_ips.contains(ip);
    }

    /**
     * 获取IP
     * @param ctx
     * @return
     */
    static String getIP(ChannelHandlerContext ctx){
    	String address = ctx.channel().remoteAddress().toString();
		address = address.replace("/", "");
		if(address.contains(":")){
			address = address.split(":")[0]; 
		}
		return address;
    }

}
