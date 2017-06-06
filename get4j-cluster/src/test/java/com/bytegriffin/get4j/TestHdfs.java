package com.bytegriffin.get4j;

import com.bytegriffin.get4j.conf.ClusterNode;

public class TestHdfs {

	public static void main(String[] args) throws Exception {
		ClusterNode node = Cluster.create().enableHdfs().build();//只有cluster端才能开启hdfs功能
		Spider.list_detail().cluster(node).fetchUrl("http://bj.fang.anjuke.com/loupan/all/p{1}/").detailSelector("a.items-name[href]").downloadHdfs("hdfs://192.168.1.102:9000/").start();
	}

}
