package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

/**
 * 安居客 - 楼盘
 */
public class AnjukePageParser  implements PageParser {

    @Override
    public void parse(Page page) {
       System.err.println("房源名称："+page.getTitle() + "   访问地址：" + page.getUrl() );
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("http://bj.fang.anjuke.com/loupan/all/p{1}/").detailSelector("a.items-name[href]")
                .parser(AnjukePageParser.class)
                .thread(1).start();
    }

}
