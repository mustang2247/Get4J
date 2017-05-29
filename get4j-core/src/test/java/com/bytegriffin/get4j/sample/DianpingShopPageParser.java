package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

/**
 * 点评网商铺信息
 */
public class DianpingShopPageParser implements PageParser {

    @Override
    public void parse(Page page) {
        System.err.println(page.getTitle() + "   " + page.getCharset() + "  " + page.getUrl() + "  ");
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("http://www.dianping.com/search/category/2/10/p{1}").parser(DianpingShopPageParser.class)
                .detailSelector("div.pic>a[href]").defaultUserAgent().defaultProxy()
                .thread(1).start();

    }

}
