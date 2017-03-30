package com.bytegriffin.get4j.store;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.conf.Seed;
import com.bytegriffin.get4j.util.ConcurrentQueue;
import com.bytegriffin.get4j.util.FileUtil;
import com.bytegriffin.get4j.util.UrlQueue;

/**
 * 坏链存储器<br>
 * 负责爬虫在爬取过程中访问不了或者根本不是链接的坏链存储在本地文件中
 */
public final class FailUrlStorage {

	private static final Logger logger = LogManager.getLogger(FailUrlStorage.class);
	private static final String folder = System.getProperty("user.dir") + File.separator + "data" + File.separator
			+ "dump" + File.separator;
	private static final String filename = "fail_url";
	private static File failUrlFile = null;

	public static void init(Seed seed) {
		failUrlFile = FileUtil.makeDumpDir(folder, filename);
		logger.info("初始化坏链文件完成。");
	}

	public static void dumpFile(String seedName) {
		ConcurrentQueue<String> failurls = UrlQueue.getFailVisitedUrl(seedName);
		if (failurls != null && !failurls.isEmpty()) {
			FileWriter fw = null;
			try {
				fw = new FileWriter(failUrlFile, true);
				for (int i = 0; i < failurls.size(); i++) {
					Object obj = failurls.get(i);
					if (obj == null) {
						break;
					}
					String url = obj.toString();
					fw.write(url+System.getProperty("line.separator"));
				}
				fw.flush();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if(fw != null){
						fw.close();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			// FileUtil.append(failUrlFile, content);
			// content = "";
			logger.info("线程[" + Thread.currentThread().getName() + "]抓取种子[" + seedName + "]时一共有["
					+ failurls.size() + "]个坏链产生。");
		}

	}

}
