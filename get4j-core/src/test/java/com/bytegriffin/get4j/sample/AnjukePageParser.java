package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

public class AnjukePageParser  implements PageParser {

    @Override
    public void parse(Page page) {
       System.err.println(page.getTitle() + "   " + page.getUrl() );
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("http://bj.fang.anjuke.com/loupan/all/p{1}/").detailSelector("a.items-name[href]")
                .parser(AnjukePageParser.class)
                .thread(1).start();
    }

}
