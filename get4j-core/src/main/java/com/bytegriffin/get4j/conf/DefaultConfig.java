package com.bytegriffin.get4j.conf;

import java.io.File;

import com.bytegriffin.get4j.net.sync.Syncer;

/**
 * 默认配置常量：负责系统中所有的默认配置参数
 */
public class DefaultConfig {

    /**
     * 默认：比如下载目录为default时指的是/data/download/${seedname}目录下
     */
    public static final String default_value = "default";

    /**
     * 默认 线程数目
     */
    public static final int thread_num = 1;
    
    /**
     * 默认 抓取延迟为0秒
     */
    public static final int fetch_sleep = 0;

    /**
     * 默认 监控页面变化频率
     */
    public static final int probe_sleep = 30;
    
    /**
     * 默认 抓取延迟为0秒
     */
    public static final String fetch_total_pages = "1";
    
    /**
     * 默认 不支持javascript
     */
    public static final boolean fetch_javascript_support = false;

    /**
     * 默认 user agent 配置文件
     */
    public static final String user_agent = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "user_agent";
    
    /**
     * 默认 http proxy 配置文件
     */
    public static final String http_proxy = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "http_proxy";

    /**
     * 默认 dump文件存储位置
     */
    public static final String dump_folder = System.getProperty("user.dir") + File.separator + "data" + File.separator + "dump" + File.separator;

    /**
     * 默认 页面变化探测器文件夹位置
     */
    public static final String probe_folder = System.getProperty("user.dir") + File.separator + "data" + File.separator + "probe" + File.separator;
    
    
    /**
     * 获取相应种子的下载地址
     *
     * @param seedName 种子名称
     * @return String
     */
    public static String getDownloadDisk(String seedName) {
        return System.getProperty("user.dir") + File.separator + "data" + File.separator + "download" + File.separator + seedName;
    }

    /**
     * 获取相应种子的lucene index地址
     *
     * @param seedName String
     * @return String
     */
    public static String getLuceneIndexPath(String seedName) {
        return System.getProperty("user.dir") + File.separator + "data" + File.separator + "index" + File.separator + seedName;
    }

    /**
     * 默认 list url的左边字符串部分
     */
    public static final String fetch_list_url_left = "{";

    /**
     * 默认 list url的右边可变字符串部分
     */
    public static final String fetch_list_url_right = "}";

    /**
     * 当fetch.resource.selector和fetch.resource.filter有多个值的时候之间用逗号隔开
     */
    public static final String fetch_resource_split = ",";

    /**
     * JsonPath解析字符串的前缀，来判断是否是Jsoup的cssSelect还是JsonPath字符串
     */
    public static final String json_path_prefix = "$.";

    /**
     * 当fetch.detail.selector选择Json属性中内容是Html，并且Html中包含detail Link时，
     * 这种特殊情况需要先写Jsonpath再写Jsoup选择器字符串，中间用竖杠隔开，例如： $.data.*|a.class[href]
     */
    public static final String fetch_detail_json_html_split = "|";

    /**
     * 是否将下载后的文件保留其url前缀 <br>
     * false：表示自动补全文件名称<br>
     * true：表示用url补全文件名称<br>
     */
    public static boolean download_file_url_naming = false;

    /**
     * 默认home page名称
     */
    public static final String home_page_name = "index.html";

    /**
     * 默认ContentType页面格式
     */
    public static final String html_page_suffix = "html";

    /**
     * Json类型ContentType页面格式
     */
    public static final String json_page_suffix = "json";

    /**
     * Xml类型ConentType页面格式
     */
    public static final String xml_page_suffix = "xml";

    /**
     * 是否开启资源同步
     */
    public static boolean sync_open;

    /**
     * 全局资源同步器
     */
    public static Syncer resource_synchronizer;

    /**
     * 每次同步的最大值
     */
    public static int sync_batch_count = 10;

    /**
     * 每次同步的最大时间间隔，单位是秒
     */
    public static int sync_batch_time = 10;
}
