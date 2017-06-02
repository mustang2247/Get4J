package com.bytegriffin.get4j.store;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.conf.DefaultConfig;
import com.bytegriffin.get4j.core.ExceptionCatcher;
import com.bytegriffin.get4j.core.Globals;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.send.EmailSender;
import com.bytegriffin.get4j.util.FileUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public class HdfsStorage {

	private static Logger logger = LogManager.getLogger(HdfsStorage.class);

	private String dirPath;
	private FileSystem fileSystem;
	private static Map<String, HdfsStorage> map = null;
	//副本数
	private static final short replication = 1;

	public static HdfsStorage create(String hdfsAddress,String seedName) {
		map = Maps.newHashMap();
		HdfsStorage hs = new HdfsStorage(hdfsAddress, seedName);
		map.put(seedName, hs);
		return hs;
	}

	public static HdfsStorage get(String seedName){
		return map.get(seedName);
	}

	/**
	 * 初始化种子目录，如果没有则创建
	 * @param hdfsAddress
	 * @param seedName
	 */
	private HdfsStorage(String hdfsAddress, String seedName) {
		dirPath = hdfsAddress + seedName + File.separator;
		Path path = new Path(dirPath);
		try {
			this.fileSystem = path.getFileSystem(new HdfsConfiguration());
			this.fileSystem.setReplication(path, replication);
			this.fileSystem.mkdirs(path);
		} catch (Exception e) {
			logger.error("种子[{}]通过线程[{}]往Hdfs上写入名为[{}]时出错。", seedName, Thread.currentThread().getName(), dirPath, e);
			System.exit(0);;
		}
	}

	public String getDir() {
		return dirPath;
	}

	public void downloadPagesToHdfs(Page page) {
		String folderName = Globals.DOWNLOAD_HDFS_DIR_CACHE.get(page.getSeedName());
		String fileName = folderName ;
		if (page.isJsonContent()) {
			fileName += FileUtil.generatePageFileName(page.getUrl(), DefaultConfig.json_page_suffix);
		} else if (page.isHtmlContent()) {
			fileName += FileUtil.generatePageFileName(page.getUrl(), DefaultConfig.html_page_suffix);
		} else if (page.isXmlContent()) {
			fileName += FileUtil.generatePageFileName(page.getUrl(), DefaultConfig.xml_page_suffix);
		} else {// 这种情况为资源文件，直接返回
			return;
		}
		byte[] content = null;
		try {
			if (page.isHtmlContent()) {
				content = page.getHtmlContent().getBytes(page.getCharset());
			} else if (page.isJsonContent()) {
				content = page.getJsonContent().getBytes(page.getCharset());
			} else if (page.isXmlContent()) {
				content = page.getXmlContent().getBytes(page.getCharset());
			}
		} catch (UnsupportedEncodingException e) {
			logger.error("种子[{}]通过线程[{}]往Hdfs上写入名为[{}]时出错。", page.getSeedName(),Thread.currentThread().getName(), fileName, e);
			EmailSender.sendMail(e);
			ExceptionCatcher.addException(e);
		}
		writeFileToHdfs(fileName, content);
	}

	/**
	 * 往hdfs上写文件
	 *
	 * @param fileName  String
	 * @param content   byte[]
	 */
	public  void writeFileToHdfs(String fileName, byte[] content) {
		if (Strings.isNullOrEmpty(fileName) || content == null) {
			return;
		}
		try{
			if (isExistsHdfsFile(fileName, content.length)) {
				return;
			}
			Path path = new Path(fileName);
			OutputStream out = this.fileSystem.create(path, replication);
			out.write(content, 0, content.length);
			IOUtils.closeStream(out);
		} catch (IOException e) {
			logger.error("线程[{}]往Hdfs上写入页面时出错。", Thread.currentThread().getName(), e);
			EmailSender.sendMail(e);
			ExceptionCatcher.addException(e);
		}
	}

	/**
	 * 下载大文件到磁盘上
	 *
	 * @param fileName  String
	 * @param contentLength     Long
	 * @param content  InputStream
	 */
	public void writeBigFileToHdfs(String fileName, Long contentLength, InputStream content) {
		if (Strings.isNullOrEmpty(fileName) || content == null) {
			return;
		}
		try{
			if (isExistsHdfsFile(fileName, contentLength)) {
				return;
			}
			Path path = new Path(fileName);
			OutputStream out = this.fileSystem.create(path, replication);
			IOUtils.copyBytes(content, out, 4096, true);
			IOUtils.closeStream(content);
			IOUtils.closeStream(out);
		} catch (IOException e) {
			logger.error("线程[{}]往Hdfs上写入页面时出错。", Thread.currentThread().getName(), e);
			EmailSender.sendMail(e);
			ExceptionCatcher.addException(e);
		}
	}

	/**
	 * 判断文件是否已经存在磁盘上，有的话就无需下载
	 * @param fileName
	 * @param fileSize
	 * @return
	 */
	public boolean isExistsHdfsFile(String fileName, long fileSize){
		Path path = new Path(fileName);
		try {
			boolean isexist = this.fileSystem.exists(path);
			if (isexist && fileSize == fileSystem.getFileStatus(path).getLen()) {
				logger.warn("线程[{}]在往HDFS上写入名为[{}]的文件时发现此文件已存在。", Thread.currentThread().getName() , fileName);
				return true;
			}
		} catch (IOException e) {
			logger.error("线程[{}]往Hdfs上写入页面时出错。",Thread.currentThread().getName(), e);
			EmailSender.sendMail(e);
			ExceptionCatcher.addException(e);
		}
		return false;
	}

}
