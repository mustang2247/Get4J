package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.annotation.Field;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

/**
 * QQ新闻
 */
public class QQNewsPageParser implements PageParser {

	@Field("span.a_catalog>a[href]")
	private String catalog;

    @Override
    public void parse(Page page) {
       System.err.println(page.getField("catalog") + "   " + page.getUrl() );
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("http://news.qq.com/c/2013ywList_{1}.htm").detailSelector("a.linkto[href]")
                .parser(QQNewsPageParser.class).field(QQNewsPageParser.class)
                .thread(1).start();
    }

}