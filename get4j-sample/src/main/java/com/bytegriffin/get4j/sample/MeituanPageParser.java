package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

/**
 * 美团
 */
public class MeituanPageParser implements PageParser {

    @Override
    public void parse(Page page) {
       System.err.println(page.getTitle() + "   " + page.getUrl() );
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("http://bj.meituan.com/category/all/all/page{1}").defaultUserAgent()
        			.detailSelector("a.lazy-img[href]").parser(MeituanPageParser.class).thread(1).start();
    }

}
