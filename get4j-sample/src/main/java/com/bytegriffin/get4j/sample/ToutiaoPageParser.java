package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

/**
 * 今日头条爬虫
 */
public class ToutiaoPageParser implements PageParser {

    @Override
    public void parse(Page page) {
        System.err.println("页面标题："+page.getTitle()+" 页面链接:"+page.getUrl());
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("https://www.toutiao.com/api/pc/feed/?category=news_hot&utm_source=toutiao&widen=1&max_behot_time=0&max_behot_time_tmp=1509535975&tadrequire=true&as=A125B9EFA97E9FF&cp=59F90E29CFAFCE1")
        	.detailSelector("https://www.toutiao.com$.data[*].source_url").defaultUserAgent()
        	.parser(ToutiaoPageParser.class).thread(1).start();
    }

}