package com.bytegriffin.get4j.sample;

import com.bytegriffin.get4j.Spider;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.parse.PageParser;

/**
 * 携程网 --- 酒店搜索
 */
public class CtripPageParser implements PageParser {

    @Override
    public void parse(Page page) {
        System.err.println(page.getTitle() + "  " + page.getUrl());
    }

    public static void main(String[] args) throws Exception {
        Spider.list_detail().fetchUrl("http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx?cityId=1&cityName=北京&page={1}")
                .detailSelector("http://hotels.ctrip.com/$.hotelPositionJSON[*].url").parser(CtripPageParser.class)
                .thread(1).start();
    }

}
