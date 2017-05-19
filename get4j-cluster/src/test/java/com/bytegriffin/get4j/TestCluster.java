package com.bytegriffin.get4j;

import com.bytegriffin.get4j.conf.ClusterNode;

public class TestCluster {

	public static void main(String[] args) throws Exception {
		ClusterNode node = Cluster.create().redis("one", "192.168.1.13:6379","foobared").build();
		Spider.single().cluster(node).fetchUrl("http://www.baidu.com").start();
	}

}
