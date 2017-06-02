package com.bytegriffin.get4j.download;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.core.ExceptionCatcher;
import com.bytegriffin.get4j.core.Globals;
import com.bytegriffin.get4j.net.http.HttpClientEngine;
import com.bytegriffin.get4j.send.EmailSender;
import com.bytegriffin.get4j.store.HdfsStorage;
import com.bytegriffin.get4j.util.DateUtil;
import com.bytegriffin.get4j.util.FileUtil;

public class HttpDownloader extends HttpClientEngine {

	private static final Logger logger = LogManager.getLogger(HttpDownloader.class);

    /**
     * 下载大文件，默认设置超过10M大小的文件算是大文件
     * 文件太大会抛异常，所以特此添加一个下载大文件的方法
     *
     * @param seedName        String
     * @param url                        String
     * @param contentlength contentLength
     * @return boolean
     */
    public static boolean downloadBigFile(String seedName, String url, long contentLength,HdfsStorage hdfsStorage) {
        String start = DateUtil.getCurrentDate();
        String fileName = FileUtil.generateResourceName(url, "");
        fileName = Globals.DOWNLOAD_HDFS_DIR_CACHE.get(seedName) + fileName;
        if(hdfsStorage.isExistsHdfsFile(fileName, contentLength)){
        	logger.warn("线程[{}]下载种子[{}]的大文件[{}]时发现磁盘上已经存在此文件[{}]。",Thread.currentThread().getName(),  seedName, url, fileName);
        	return true;
        }
        CloseableHttpClient httpClient = null;
        HttpGet request = null;
        try {
            setHttpProxy(seedName);
            setUserAgent(seedName);
            httpClient = Globals.HTTP_CLIENT_BUILDER_CACHE.get(seedName).build();
            request = new HttpGet(url);
            //request.addHeader("Range", "bytes=" + offset + "-" + (this.offset + this.length - 1));//断点续传的话需要设置Range属性，当然前提是服务器支持
            HttpResponse response = httpClient.execute(request);
            hdfsStorage.writeBigFileToHdfs(fileName, contentLength, response.getEntity().getContent());
            String log = DateUtil.getCostDate(start);
            logger.info("线程[{}]下载大小为[{}]MB的文件[{}]总共花费时间为[{}]。",Thread.currentThread().getName() ,contentLength / (1024 * 1024),fileName, log);
        } catch (Exception e) {
            logger.error("线程[{}]下载种子[{}]的大文件[{}]时失败。", Thread.currentThread().getName() ,seedName, url, e);
            EmailSender.sendMail(e);
            ExceptionCatcher.addException(seedName, e);
            if (request != null) {
                request.abort();
            }
        } finally {
            if (request != null) {
                request.releaseConnection();
            }
        }
        return true;
    }

}
