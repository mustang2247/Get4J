package com.bytegriffin.get4j;

import com.bytegriffin.get4j.conf.ClusterNode;
import com.bytegriffin.get4j.store.RedisStorage;

public class TestJedis {

	@SuppressWarnings("static-access")
	public static void main(String[] args){
		ClusterNode node = new ClusterNode();
		node.setRedisMode("one");
		node.setRedisAddress("192.168.1.13:6379");
		node.setRedisAuth("foobared");
		RedisStorage redis = new RedisStorage(node);
		redis.init();
		//获取总数
		System.out.println(redis.jedis.zcard("UN_VISITED_2LINKS_seed123"));
		//获取第一个
		System.out.println(redis.jedis.zrange("UN_VISITED_LINKS_seed123", 0, 0));
		//删除第一个
		System.out.println(redis.jedis.zremrangeByRank("UN_VISITED_LINKS_seed123", 8, 9));
		//增加一个
		//System.out.println(redis.jedis.zadd("UN_VISITED_LINKS_seed123", 0, "http://www.aaa.com"));
		// 判断元素是否存在于某集合内
		System.out.println(redis.jedis.zrank("UN_VISITED_LINKS_seed123", "http://www.baidu.com/index2.jsp"));
		// 返回集合
		System.out.println(redis.jedis.zrange("UN_VISITED_LINKS_seed123", 0, -1));
	}
	
}
