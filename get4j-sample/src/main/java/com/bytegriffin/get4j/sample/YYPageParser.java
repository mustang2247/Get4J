package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;
import com.google.common.base.Strings;

/**
 * YY Live - music
 */
public class YYPageParser implements PageParser {

	@Override
	public void parse(Page page) {
		String src = page.jsoup("embed[src]").attr("src");
		if(!Strings.isNullOrEmpty(page.getTitle())){
			//该swf地址在http输入后会跳转到新的swf地址上
			System.err.println("直播间名称："+page.getTitle()+"直播swf地址  http:"+src);
		}

	}

	public static void main(String[] args) throws Exception {
		Spider.list_detail().fetchUrl("http://www.yy.com/music/").parser(YYPageParser.class).detailSelector("a.video-box[href]")
				.javascriptSupport(true).defaultUserAgent().thread(1).start();
	}

}