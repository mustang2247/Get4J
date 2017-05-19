package com.bytegriffin.get4j;

import java.lang.annotation.Annotation;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.annotation.Redis;
import com.bytegriffin.get4j.conf.ClusterNode;
import com.bytegriffin.get4j.conf.ClusterNodeXmlHandler;
import com.bytegriffin.get4j.conf.Configuration;
import com.bytegriffin.get4j.conf.ConfigurationXmlHandler;
import com.bytegriffin.get4j.conf.Context;
import com.bytegriffin.get4j.conf.CoreSeedsXmlHandler;
import com.bytegriffin.get4j.conf.DefaultConfig;
import com.bytegriffin.get4j.conf.DynamicField;
import com.bytegriffin.get4j.conf.DynamicFieldXmlHandler;
import com.bytegriffin.get4j.conf.ResourceSync;
import com.bytegriffin.get4j.conf.ResourceSyncYamlHandler;
import com.bytegriffin.get4j.conf.Seed;
import com.bytegriffin.get4j.core.Initializer;
import com.bytegriffin.get4j.core.SpiderEngine;
import com.bytegriffin.get4j.store.RedisStorage;
import com.bytegriffin.get4j.util.NetHelper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class Cluster {

	private static final Logger logger = LogManager.getLogger(Cluster.class);

	private static ClusterNode clusterNode;

	private Cluster() {
	}

	/**
	 * 实例化
	 * @return
	 */
	public static Cluster create(){
		 clusterNode = new ClusterNode();
		 return new Cluster();
	 }
	
    /**
     * 设置初始化加载
     * @return
     */
    private static List<Initializer> init(ClusterNode clusterNode){
    	RedisStorage redis = new RedisStorage(clusterNode);
    	return Lists.newArrayList(redis);
    }

	/**
	 * 创建一个集群节点实例
	 * @return
	 */
    public ClusterNode build() {
        // 自动生成node name
        if (Strings.isNullOrEmpty(clusterNode.getNodeName())) {
            clusterNode.setNodeName(NetHelper.getClusterNodeName());
        }
        if(Strings.isNullOrEmpty(clusterNode.getRedisAddress()) || Strings.isNullOrEmpty(clusterNode.getRedisMode()) 
        		|| Strings.isNullOrEmpty(clusterNode.getRedisAuth())){
        	logger.error("没有设置redis属性...");
        	System.exit(1);
        }
        clusterNode.setInitializers(init(clusterNode));
        return clusterNode;
    }

    /**
     * 设置集群节点名称
     * @param nodeName
     * @return
     */
    public Cluster nodeName(String nodeName){
    	clusterNode.setNodeName(nodeName);
    	return this;
    }

    /**
     * 设置redis
     * @param mode ：部署模式：one(单机)/sharded(分片)/cluster(集群)
     * @param address 地址格式：host1:port1|host2:port2
     * @param auth 密码，没有可设置null
     * @return
     */
    public Cluster redis(String mode, String address, String auth){
    	clusterNode.setRedisMode(mode);
    	clusterNode.setRedisAddress(address);
    	clusterNode.setRedisAuth(auth);
    	return this;
    }

    /**
     * 根据Annotation设置redis
     * @param clazz
     * @return
     */
    public Cluster redis(Class<?> clazz) {
        Annotation[] ans = clazz.getDeclaredAnnotations();
        if (ans == null || ans.length == 0) {
            logger.error("类[" + clazz.getName() + "]没有配置任何Annotation。");
            System.exit(1);
        }
        for (Annotation an : ans) {
            String type = an.annotationType().getSimpleName();
            if ("Redis".equalsIgnoreCase(type)) {
                if (clazz.isAnnotationPresent(Redis.class)) {
                	Redis redis = (Redis) clazz.getAnnotation(Redis.class);
                    this.redis(redis.mode(),redis.address(),redis.auth());
                }
            } 
        }
        return this;
    }

    /**
     * 配置文件调用入口
     * @param args
     */
	public static void main(String[] args) {
		DefaultConfig.closeHttpClientLog();
		Context context = new Context(new CoreSeedsXmlHandler());
		List<Seed> seeds = context.load();

		context = new Context(new ResourceSyncYamlHandler());
		ResourceSync synchronizer = context.load();

		context = new Context(new ConfigurationXmlHandler());
		Configuration configuration = context.load();

		context = new Context(new DynamicFieldXmlHandler());
		List<DynamicField> dynamicFields = context.load();

		context = new Context(new ClusterNodeXmlHandler());
		ClusterNode clusterNode = context.load();

		SpiderEngine.create().setClusterNode(clusterNode).setSeeds(seeds).setResourceSync(synchronizer)
				.setConfiguration(configuration).setDynamicFields(dynamicFields).build();
		logger.info("爬虫集群开始启动...");
	}

}
