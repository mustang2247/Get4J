package com.bytegriffin.get4j.download;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.conf.Seed;
import com.bytegriffin.get4j.core.Globals;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.core.PageMode;
import com.bytegriffin.get4j.core.Process;
import com.bytegriffin.get4j.net.http.HttpClientEngine;
import com.bytegriffin.get4j.net.http.UrlAnalyzer;
import com.bytegriffin.get4j.store.HdfsStorage;
import com.bytegriffin.get4j.util.FileUtil;
import com.google.common.base.Strings;

public class HdfsDownloader implements Process {

    private static final Logger logger = LogManager.getLogger(HdfsDownloader.class);
    private HdfsStorage hdfsStorage;

    @Override
    public void init(Seed seed) {
    	String hdfsAddress = seed.getDownloadHdfs();
        hdfsAddress = hdfsAddress.endsWith("/") ? hdfsAddress : hdfsAddress + "/";
        //初始化hdfs存储
        hdfsStorage = HdfsStorage.create(hdfsAddress, seed.getSeedName());

        if (UrlAnalyzer.isStartHttpUrl(hdfsAddress)) { //httpfs or webhdfs
        	
        } else if(hdfsAddress.startsWith("hdfs://")){
            if (hdfsAddress.contains(File.separator) || hdfsAddress.contains(":")) {
                if (!hdfsAddress.contains(seed.getSeedName())) {
                    hdfsAddress = hdfsAddress + seed.getSeedName();
                }
            } else {
                logger.error("下载文件夹[" + hdfsAddress + "]配置出错，请重新检查。");
                System.exit(1);
            }
        }
        Globals.DOWNLOAD_HDFS_DIR_CACHE.put(seed.getSeedName(), hdfsStorage.getDir());
        logger.info("种子[" + seed.getSeedName() + "]的组件HdfsDownloader的初始化完成。");
    }

    @Override
    public void execute(Page page) {
    	 // 1.在磁盘上生成页面
        PageMode fm = Globals.FETCH_PAGE_MODE_CACHE.get(page.getSeedName());
        if (!PageMode.list_detail.equals(fm)) {// 当启动list_detail模式，默认不会下载页面的
        	hdfsStorage.downloadPagesToHdfs(page);
        }

        // 2.下载页面中的资源文件
       String folderName =  Globals.DOWNLOAD_HDFS_DIR_CACHE.get(page.getSeedName());
        List<DownloadFile> list = HttpClientEngine.downloadResources(page, folderName);
        if(list != null && !list.isEmpty()){
        	  for(DownloadFile file : list){
              	hdfsStorage.writeFileToHdfs(file.getFileName(), file.getContent());
              }
        }

        //下载资源文件中的大文件
        downloadBigFile(page.getSeedName());

        // 3.判断是否包含avatar资源，有的话就下载
        if (!Strings.isNullOrEmpty(page.getAvatar())) {
        	DownloadFile downloadFile = HttpClientEngine.downloadAvatar(page, folderName);// 下载avatar资源
        	if(downloadFile != null){
        		FileUtil.writeFileToDisk(downloadFile.getFileName(), downloadFile.getContent());
        		 //下载大文件
        		downloadBigFile(page.getSeedName());
        	}
        }

        // 4.设置page的资源保存路径属性
        page.setResourceSavePath(Globals.DOWNLOAD_DISK_DIR_CACHE.get(page.getSeedName()));
        logger.info("线程[" + Thread.currentThread().getName() + "]下载种子[" + page.getSeedName() + "]的url[" + page.getUrl() + "]完成。");
    }

	/**
	 * 下载大文件
	 * @param seedName
	 */
	private void downloadBigFile(String seedName) {
		if (DownloadFile.isExist(seedName)) {
			List<DownloadFile> downlist = DownloadFile.get(seedName);
			for (DownloadFile file : downlist) {
				HttpDownloader.downloadBigFile(seedName, file.getUrl(), file.getContentLength(), hdfsStorage);
			}
			DownloadFile.clear(seedName);
		}
	}

}
