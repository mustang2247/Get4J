package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

/**
 * QQ新闻
 */
public class QQNewsPageParser implements PageParser {

    @Override
    public void parse(Page page) {
       System.err.println(page.getTitle() + "   " + page.getUrl() );
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("http://news.qq.com/c/2013ywList_{1}.htm").detailSelector("a.linkto[href]")
                .parser(AnjukePageParser.class)
                .thread(1).start();
    }

}
