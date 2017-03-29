package com.bytegriffin.get4j.fetch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.conf.Seed;
import com.bytegriffin.get4j.core.Constants;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.core.Process;
import com.bytegriffin.get4j.net.http.HttpEngine;
import com.bytegriffin.get4j.net.http.UrlAnalyzer;
import com.bytegriffin.get4j.util.DateUtil;

/**
 * 单个页面抓取器
 */
public class SingleFetcher implements Process {

	private static final Logger logger = LogManager.getLogger(SingleFetcher.class);
	private HttpEngine http = null;

	/**
	 * 初始化抓取过滤器
	 */
	@Override
	public void init(Seed seed) {
		// 1.获取相应的http引擎
		http = Constants.HTTP_ENGINE_CACHE.get(seed.getSeedName());

		// 2.初始化资源选择器缓存
		FetchResourceSelector.init(seed);
		logger.info("Seed[" + seed.getSeedName() + "]的组件SingleFetcher的初始化完成。");
	}

	@Override
	public void execute(Page page) {
		// 1.获取并设置Page的HtmlContent或JsonContent属性、Cookies属性
		page = http.SetContentAndCookies(page);
		
		// 2.获取并设置Page的Resource属性
		UrlAnalyzer.custom(page).sniffAndSetResources();

		// 3.设置Page其它属性
		page.setFetchTime(DateUtil.getCurrentDate());
		if(page.isHtmlContent()){// Html格式
			page.setTitle(UrlAnalyzer.getTitle(page.getHtmlContent()));
		} else { // Json格式：程序不知道具体哪个字段是title字段
			
		}

		logger.info("线程[" + Thread.currentThread().getName() + "]抓取种子[" + page.getSeedName() + "]的url["+page.getUrl()+"]完成。");
	}

}
