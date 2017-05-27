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
        System.err.println(page.getUrl());
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("http://toutiao.com/search_content/?offset=20&format=json&keyword=工作&autoload=true&count=20&cur_tab=1")
        .detailSelector("$.data[*].display_url")
        	.parser(ToutiaoPageParser.class).thread(1).start();
    }

}