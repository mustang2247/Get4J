package com.bytegriffin.get4j.store;

import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.conf.ClusterNode;
import com.bytegriffin.get4j.core.Initializer;
import com.bytegriffin.get4j.core.UrlQueue;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

/**
 * Redis分布式存储Url
 */
@SuppressWarnings("resource")
public class RedisStorage  extends Initializer {

	private static final Logger logger = LogManager.getLogger(RedisStorage.class);

	private static final String host_split = "|";
	private static final int default_port= 6379;

	public static JedisCommands jedis;

	private ClusterNode clusterNode;

	public RedisStorage(ClusterNode node) {
		clusterNode = node;
	}

	@Override
	public void init() {
		if(clusterNode == null){
			logger.error("Cluster模式下的组件RedisStorage的初始化完成。");
			System.exit(1);
		}
		if("one".equalsIgnoreCase(clusterNode.getRedisMode())){
			one(clusterNode.getRedisAddress(), clusterNode.getRedisAuth());
		} else if("sharded".equalsIgnoreCase(clusterNode.getRedisMode())){
			shard(clusterNode.getRedisAddress(), clusterNode.getRedisAuth());
		} else if("cluster".equalsIgnoreCase(clusterNode.getRedisMode())){
			cluster(clusterNode.getRedisAddress(), clusterNode.getRedisAuth());
		}
		UrlQueue.registerRedisQueue(new RedisQueue<String>(jedis));
		logger.info("Cluster模式下的组件RedisStorage的初始化完成。");
	}

	private JedisPoolConfig config() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(1000);
        config.setMaxIdle(5);
        config.setMinIdle(1);
        config.setBlockWhenExhausted(true);
        config.setMaxWaitMillis(60 * 1000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestWhileIdle(true);
        config.setTimeBetweenEvictionRunsMillis(60* 1000);
        return config;
    }

	/**
	 * 单机
	 */
	public void one(String address, String password){
		String firstAddress = Splitter.on(host_split).trimResults().split(address).iterator().next();
		List<String> list = Splitter.on(":").trimResults().splitToList(firstAddress);
		String host = "";
		Integer port = null;
		if (list.size() == 1) {
			host = Strings.isNullOrEmpty(list.get(0)) ? null : list.get(0) ;
			port = default_port;
		} else if (list.size() == 2) {
			host = Strings.isNullOrEmpty(list.get(0)) ? null : list.get(0) ;
			port = Strings.isNullOrEmpty(list.get(1)) ? null : Integer.valueOf(list.get(1)) ;
		} else {
			logger.error("Cluster模式下的组件RedisStorage在初始化时发现redis地址格式有问题："+ address);
			System.exit(1);
		}
		JedisPool jedisPool = null;
		if (Strings.isNullOrEmpty(password)){
			jedisPool = new JedisPool(config(), host, port);
		}  else {
			jedisPool = new JedisPool(config(), host, port, 2000, password);
		}
		try{
			jedis = jedisPool.getResource();
		}catch (Exception e){
			logger.error("Cluster模式下的组件RedisStorage初始化链接失败。", e);
			System.exit(1);
		}
	}

	/**
	 * 分片 客户端集群
	 */
	public void shard(String addresses, String password) {
		List<String> list = Splitter.on(host_split).trimResults().splitToList(addresses);
		List<JedisShardInfo> shards = Lists.newArrayList();
		for(String addr : list){
			List<String> hostAndPort = Splitter.on(":").trimResults().splitToList(addr);
			if (hostAndPort.size() == 1){
				JedisShardInfo node = new JedisShardInfo(hostAndPort.get(0), default_port);
				if(!Strings.isNullOrEmpty(password)){
					node.setPassword(password);
				}
				shards.add(node);
			}else if (hostAndPort.size() == 2){
				JedisShardInfo node = new JedisShardInfo(hostAndPort.get(0), Integer.valueOf(hostAndPort.get(1)));
				if(!Strings.isNullOrEmpty(password)){
					node.setPassword(password);
				}
				shards.add(node);
			}
		}
		if(shards == null || shards.isEmpty()){
			logger.error("Cluster模式下的组件RedisStorage在初始化时发现redis地址格式有问题。");
			System.exit(1);
		}
		ShardedJedisPool pool = new ShardedJedisPool(config(), shards);
		try{
			jedis = pool.getResource();
		}catch (Exception e){
			logger.error("Cluster模式下的组件RedisStorage初始化链接失败。", e);
			System.exit(1);
		}
	}

	/**
	 * 集群  redis 3以上支持cluster
	 */
    public void cluster(String addresses,String password) {
    	List<String> list = Splitter.on(host_split).trimResults().splitToList(addresses);
        Set<HostAndPort> nodes = Sets.newHashSet();
        for(String addr : list){
        	List<String> hostAndPort = Splitter.on(":").trimResults().splitToList(addr);
        	if (hostAndPort.size() == 1){
        		nodes.add(new HostAndPort(hostAndPort.get(0), default_port));
        	} else if (hostAndPort.size() == 2){
				nodes.add(new HostAndPort(hostAndPort.get(0), Integer.valueOf(hostAndPort.get(1))));
			}
        }
        if(nodes == null || nodes.isEmpty()){
        	logger.error("Cluster模式下的组件RedisStorage在初始化时发现redis地址格式有问题。");
			System.exit(1);
        }
        try{
        	if(Strings.isNullOrEmpty(password)){
            	jedis = new JedisCluster(nodes, 2000, 2000, 5, config() );
            } else {
            	jedis = new JedisCluster(nodes, 2000, 2000, 5, password, config() );
            }
		}catch (Exception e){
			logger.error("Cluster模式下的组件RedisStorage初始化链接失败。", e);
			System.exit(1);
		}
    }

}
