package com.bytegriffin.get4j.store;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bytegriffin.get4j.conf.Seed;
import com.bytegriffin.get4j.core.Constants;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.core.Process;
import com.bytegriffin.get4j.util.DateUtil;
import com.bytegriffin.get4j.util.MD5Util;
import com.bytegriffin.get4j.util.StringUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * 关系型数据库存储器<br>
 * 增量式更新数据
 */
public class DBStorage implements Process {

    private static final Logger logger = LogManager.getLogger(DBStorage.class);

    private final static String insertsql = "insert into page (ID,SEED_NAME,FETCH_URL,SITE_HOST,TITLE,AVATAR,FETCH_CONTENT,COOKIES,RESOURCES_URL,FETCH_TIME,CREATE_TIME) values ( ";

    private final static String updatesql = "update page set ";

    @Override
    public void init(Seed seed) {
        String jdbc = seed.getStoreJdbc();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbc);
        config.setMaximumPoolSize(50);
        config.setConnectionTestQuery("SELECT 1");
        HikariDataSource datasource = new HikariDataSource(config);
        if (datasource.isClosed()) {
            logger.error("种子[" + seed.getSeedName() + "]的组件DBStorage没有连接成功。");
            System.exit(1);
        }
        Constants.DATASOURCE_CACHE.put(seed.getSeedName(), datasource);
        logger.info("种子[" + seed.getSeedName() + "]的组件DBStorage的初始化完成。");
    }


    @Override
    public void execute(Page page) {
        DataSource dataSource = Constants.DATASOURCE_CACHE.get(page.getSeedName());

        // 更新page数据
        Page dbpage = readOne(dataSource, page);
        if (dbpage == null) {
            String insertSql = insertsql + buildInsertSql(page);
            write(dataSource, insertSql);
        } else if (page.isRequireUpdate(dbpage)) {
            String updateSql = updatesql + buildUpdateSql(page, dbpage.getId());
            write(dataSource, updateSql);
        }
        logger.info("线程[" + Thread.currentThread().getName() + "]保存种子[" + page.getSeedName() + "]的url[" + page.getUrl() + "]完成。");

    }

    /**
     * 动态构建insert语句
     *
     * @param page Page
     * @return String
     */
    private String buildInsertSql(Page page) {
        String sql = "'" + MD5Util.uuid() + "','" + page.getSeedName() + "','" + page.getUrl() + "',";
        if (page.getHost() == null) {
            sql += "" + null + ",";
        } else {
            sql += "'" + page.getHost() + "',";
        }
        if (page.getTitle() == null) {// 之所以不判断空字符串''，是因为有可能页面的title本身就是控制字符串本身
            sql += "" + null + ",";
        } else {
            try {//title也要像content一样编码，因为title也可能存在一些字符不能直接存入数据库
                sql += "'" + URLEncoder.encode(page.getTitle(), "UTF-8") + "',";
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        if (StringUtil.isNullOrBlank(page.getAvatar())) {
            sql += "" + null + ",";
        } else {
            sql += "'" + page.getAvatar().replace("\\", "\\\\") + "',";// windows下的磁盘路径斜杠会被mysql数据库会自动去除
        }
        if (page.isHtmlContent()) {
            try {
                sql += "'" + URLEncoder.encode(page.getHtmlContent(), "UTF-8") + "',";
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        } else if (page.isJsonContent()) {
            sql += "'" + page.getJsonContent() + "',";
        } else if (page.isXmlContent()) {
            sql += "'" + page.getXmlContent() + "',";
        } else {
            sql += "" + null + ",";
        }
        if (page.getCookies() == null) {
            sql += "" + null + ",";
        } else {
            sql += "'" + page.getCookies() + "',";
        }
        if (page.getResources().isEmpty()) {
            sql += "" + null + ",";
        } else {
            sql += "'" + page.getResources().toString() + "',";
        }
        if (page.getFetchTime() == null) {
            sql += "" + null + ",";
        } else {
            sql += "'" + page.getFetchTime() + "',";
        }
        sql += "'" + DateUtil.getCurrentDate() + "' )";
        return sql;
    }

    /**
     * 动态构建update语句
     *
     * @param page Page
     * @param dbId String
     * @return String
     */
    private String buildUpdateSql(Page page, String dbId) {
        String sql = "";
        // 注意：有时候修改了seedName，此时也会随之更新
        if (StringUtil.isNullOrBlank(page.getSeedName())) {
            sql += "SEED_NAME=" + null + ",";
        } else {
            sql += "SEED_NAME='" + page.getSeedName() + "',";
        }
        if (StringUtil.isNullOrBlank(page.getAvatar())) {
            sql += "AVATAR=" + null + ",";
        } else {
            sql += "AVATAR='" + page.getAvatar().replace("\\", "\\\\") + "',";
        }
        if (page.getTitle() == null) {
            sql += "TITLE=" + null + ",";
        } else {
            try {
                sql += "TITLE='" + URLEncoder.encode(page.getTitle(), "UTF-8") + "',";
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        if (page.getCookies() == null) {
            sql += "COOKIES=" + null + ",";
        } else {
            sql += "COOKIES='" + page.getCookies() + "',";
        }
        String content = null;
        if (page.isHtmlContent()) {
            try {
                content = URLEncoder.encode(page.getHtmlContent(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        } else if (page.isJsonContent()) {
            content = page.getJsonContent();
        } else if (page.isXmlContent()) {
            content = page.getXmlContent();
        }
        if (content == null) {
            sql += "FETCH_CONTENT=" + null + ",";
        } else {
            sql += "FETCH_CONTENT='" + content + "',";
        }
        if (page.getResources().isEmpty()) {
            sql += "RESOURCES_URL=" + null + ",";
        } else {
            sql += "RESOURCES_URL='" + page.getResources().toString() + "',";
        }
        if (page.getFetchTime() == null) {
            sql += "FETCH_TIME=" + null + ",";
        } else {
            sql += "FETCH_TIME='" + page.getFetchTime() + "',";
        }
        sql += "UPDATE_TIME='" + DateUtil.getCurrentDate() + "' where id='" + dbId + "' ";
        return sql;
    }

    /**
     * 从数据库中读单个Page对象
     *
     * @param dataSource DataSource
     * @param page       Page
     * @return Page
     */
    public synchronized Page readOne(DataSource dataSource, Page page) {
        Connection con = null;
        Page rowData = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select * from page where fetch_url=' " + page.getUrl() + "'";
        try {
            con = dataSource.getConnection();
            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            while (rs.next()) {
                rowData = new Page();
                for (int i = 1; i <= columnCount; i++) {
                    String column = md.getColumnLabel(i).toUpperCase();
                    Object obj = rs.getObject(i);
                    if ("ID".equals(column)) {
                        rowData.setId(obj.toString());
                    } else if ("AVATAR".equals(column)) {
                        rowData.setAvatar(obj == null ? null : obj.toString());
                    } else if ("COOKIES".equals(column)) {
                        rowData.setCookies(obj == null ? null : obj.toString());
                    } else if ("SEED_NAME".equals(column)) {
                        rowData.setSeedName(obj == null ? null : obj.toString());
                    } else if ("TITLE".equals(column)) {
                        rowData.setTitle(obj == null ? null : obj.toString());
                    } else if ("SITE_HOST".equals(column)) {
                        rowData.setHost(obj == null ? null : obj.toString());
                    } else if ("FETCH_CONTENT".equals(column)) {
                        if (page.isHtmlContent()) {
                            rowData.setHtmlContent(obj == null ? null : obj.toString());
                        } else if (page.isJsonContent()) {
                            rowData.setJsonContent(obj == null ? null : obj.toString());
                        } else if (page.isXmlContent()) {
                            rowData.setXmlContent(obj == null ? null : obj.toString());
                        }
                    } else if ("FETCH_TIME".equals(column)) {
                        rowData.setFetchTime(obj == null ? null : obj.toString());
                    } else if ("FETCH_URL".equals(column)) {
                        rowData.setUrl(obj == null ? null : obj.toString());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("找不到数据 : " + sql, e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return rowData;
    }

    /**
     * 向数据库中批量写多个数据
     *
     * @param dataSource DataSource
     * @param sql        String
     */
    private synchronized void write(DataSource dataSource, String sql) {
        Connection con = null;
        Statement stmt = null;
        try {
            con = dataSource.getConnection();
            con.setAutoCommit(false);
            stmt = con.createStatement();
            stmt.addBatch(sql);
            stmt.executeBatch();
            con.commit();
        } catch (SQLException e) {
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            logger.error("不能执行更新sql: " + sql, e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                logger.error("Job " + Thread.currentThread().getName() + " cannot to close DB resultset.",
                        e.getCause());
            }

        }
    }

}
