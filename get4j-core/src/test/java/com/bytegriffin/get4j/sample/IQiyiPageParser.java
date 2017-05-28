package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

public class IQiyiPageParser  implements PageParser {

    @Override
    public void parse(Page page) {
       System.err.println(page.getTitle() + "   " + page.getUrl() );
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("http://list.iqiyi.com/www/2/-------------11-{1}-1-iqiyi--.html").detailSelector("p.site-piclist_info_title > a[href]")
                .parser(IQiyiPageParser.class).thread(1).start();
    }

}