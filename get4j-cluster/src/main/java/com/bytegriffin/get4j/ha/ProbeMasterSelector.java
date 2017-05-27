package com.bytegriffin.get4j.ha;

import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.probe.ProbeMasterChecker;
import com.bytegriffin.get4j.util.NetHelper;
import com.bytegriffin.get4j.util.Sleep;

/**
 * ProbeMaster选举策略：<br>
 * 当在Probe模式下抓取同一个seed时，集群中只允许出现一个Probe，<br>
 * 最先启动的Probe机器来充当Master，一旦Probe Master退出或宕机，<br>
 * 程序将自动在当前同一个seed集群中选举新的Probe充当Master。<br>
 * 注意：集群模式下需要每个节点配置probe选项。
 */
public final class ProbeMasterSelector  extends LeaderSelectorListenerAdapter implements ProbeMasterChecker {

	private static final Logger logger = LogManager.getLogger(ProbeMasterSelector.class);

	private static final ProbeMasterSelector me = new ProbeMasterSelector();
	private static final String zk_probe_path_prefix = "/probe/";
	private LeaderSelector leaderSelector;  
	private String seedName;
	private static Map<String, ProbeMasterSelector> probe_master_map = Maps.newHashMap();
	private static Map<String, LeaderSelector> leader_selector_map = Maps.newHashMap();

	private ProbeMasterSelector(){
	}

	public static ProbeMasterSelector single(){
		return me;
	}

	static ProbeMasterSelector create(CuratorFramework client, String seedName){
		ProbeMasterSelector pms = new ProbeMasterSelector(client, seedName);
		probe_master_map.put(seedName, pms);
		return pms;
	}

	static boolean checkMaster(String seedName){
		ProbeMasterSelector pms =  probe_master_map.get(seedName);
		if(pms == null){
			return false;
		}
		return pms.check(seedName);
	}

	/**
	 * 设置选举
	 * @param client
	 * @param seedName
	 */
	private ProbeMasterSelector(CuratorFramework client, String seedName) {
		this.seedName = seedName;
        leaderSelector = new LeaderSelector(client, zk_probe_path_prefix + seedName, this);  
        leaderSelector.autoRequeue();  
        leaderSelector.start(); 
        leader_selector_map.put(seedName, leaderSelector);
    }

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		logger.info("种子[{}]集群中将节点[{}]被选举为Probe Master。", seedName, NetHelper.getClusterNodeName());  
        //循环占用本机充当Master，除非宕机或者退出等情况发生才进行新一轮选举。
		while(true) {
        	Sleep.seconds(Integer.MAX_VALUE);
        }
	}

	/**
	 * 判断本机运行状态是否为Probe Master
	 */
	@Override
	public boolean check(String seedName) {
		return leader_selector_map.get(seedName).hasLeadership();
	}
}
