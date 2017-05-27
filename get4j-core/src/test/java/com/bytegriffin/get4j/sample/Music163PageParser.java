package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

public class Music163PageParser implements PageParser {

	@Override
	public void parse(Page page) {
		System.err.println("用户自定义专辑页面title："+page.getTitle()+"---用户头像地址："+page.getAvatar());
	}

	public static void main(String[] args) throws Exception {
		// 此地址是iframe地址，不是外部显示的地址
		Spider.list_detail().fetchUrl("http://music.163.com/discover/playlist/?order=hot&cat=全部&limit=35&offset=0")
				.totalPages("1").detailSelector("a.msk[href]").resourceSelector("div.u-cover.u-cover-1").parser(Music163PageParser.class)
				.thread(1).sleep(10).start();
	}

}
