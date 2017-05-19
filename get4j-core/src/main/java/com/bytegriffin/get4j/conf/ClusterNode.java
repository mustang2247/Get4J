package com.bytegriffin.get4j.conf;

import java.util.List;

import com.bytegriffin.get4j.core.Initializer;

/**
 * cluster-node.xml全局配置文件
 */
public class ClusterNode {

	private String nodeName;
    private String redisMode;
    private String redisAddress;
    private String redisAuth;
    private List<Initializer> initializers;

    public ClusterNode(){ 	
    }

    public ClusterNode(String nodeName){
    	this.nodeName = nodeName;
    }

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public String getRedisMode() {
		return redisMode;
	}

	public void setRedisMode(String redisMode) {
		this.redisMode = redisMode;
	}

	public String getRedisAddress() {
		return redisAddress;
	}

	public void setRedisAddress(String redisAddress) {
		this.redisAddress = redisAddress;
	}

	public String getRedisAuth() {
		return redisAuth;
	}

	public void setRedisAuth(String redisAuth) {
		this.redisAuth = redisAuth;
	}

	public List<Initializer> getInitializers() {
		return initializers;
	}

	public void setInitializers(List<Initializer> initializers) {
		this.initializers = initializers;
	}
}
