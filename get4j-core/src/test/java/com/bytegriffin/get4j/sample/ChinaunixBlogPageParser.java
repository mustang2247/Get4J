package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

/**
 * ChinaUnix博客页面解析
 */
public class ChinaunixBlogPageParser implements PageParser {

    @Override
    public void parse(Page page) {
        String title = page.getTitle();
        System.err.println(title + "  " + page.getAvatar());
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("http://blog.chinaunix.net/site/index/page/{1}.html")
                .totalPages(1).detailSelector("div.two_cont2_1>a[href]").parser(ChinaunixBlogPageParser.class).defaultUserAgent()
                .resourceSelector("div.classify_con1")
                //.defaultProbe()
                //.probe("div.classify_middle1", 30)
                //.jdbc("jdbc:mysql://localhost:3306/get4j?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8&user=root&password=root")
                .thread(3).start();
    }

}
