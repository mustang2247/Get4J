package com.bytegriffin.get4j.probe;

public interface ProbeMasterChecker {

	/**
	 * 是否处于Active状态
	 * @param seedName
	 * @return
	 */
	boolean isActive(String seedName);

}
