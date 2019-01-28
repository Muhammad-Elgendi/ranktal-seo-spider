package com.seospider.seospider.db.impl;

import com.github.s3curitybug.similarityuniformfuzzyhash.UniformFuzzyHash;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.uci.ics.crawler4j.crawler.Page;
import com.seospider.seospider.db.DBService;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import org.slf4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DBServiceImpl implements DBService {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(com.seospider.seospider.db.impl.DBServiceImpl.class);

    private ComboPooledDataSource comboPooledDataSource;

    private PreparedStatement insertPageStatement,insertUrlStatement,insertTitleStatement,insertRedirectStatement,
            insertRobotStatement,insertRefreshStatement,insertDescriptionStatement,insertContentStatement,
            insertSimilarityStatement,getHashesStatement,removeSiteStatement,getSiteStatement,updateJobStatement;

    public DBServiceImpl(ComboPooledDataSource comboPooledDataSource) throws SQLException {
        this.comboPooledDataSource = comboPooledDataSource;
        insertPageStatement = comboPooledDataSource.getConnection().prepareStatement("insert into webpage values " +
                "(nextval('id_master_seq'),?,?,?,?)");

        insertUrlStatement =  comboPooledDataSource.getConnection().prepareStatement("insert into urls values " +
                "(?,?,?)");

        insertTitleStatement =  comboPooledDataSource.getConnection().prepareStatement("insert into titles values " +
                "(?,?)");

        insertRedirectStatement =  comboPooledDataSource.getConnection().prepareStatement("insert into redirects values " +
                "(?,?)");

        insertRobotStatement =  comboPooledDataSource.getConnection().prepareStatement("insert into robots values " +
                "(?,?,?)");

        insertRefreshStatement =  comboPooledDataSource.getConnection().prepareStatement("insert into refreshes values " +
                "(?,?,?)");

        insertDescriptionStatement =  comboPooledDataSource.getConnection().prepareStatement("insert into descriptions values " +
                "(?,?)");

        insertContentStatement =  comboPooledDataSource.getConnection().prepareStatement("insert into contents values " +
                "(?,?,?,?,?,?)");

        insertSimilarityStatement =  comboPooledDataSource.getConnection().prepareStatement("insert into similarities values " +
                "(?,?,?)");

        getHashesStatement =  comboPooledDataSource.getConnection().prepareStatement("select url,content_hash from contents where url like %?%");

        removeSiteStatement =  comboPooledDataSource.getConnection().prepareStatement("delete from urls where url like %?%");

        getSiteStatement =  comboPooledDataSource.getConnection().prepareStatement("select id from sites where host = ? and user_id = ?");

        updateJobStatement = comboPooledDataSource.getConnection().prepareStatement("update crawling_jobs set status = ? , finished_at = ? where site_id = ?");

    }

    @Override
    public void storePage(Page page) {

        if (page.getParseData() instanceof HtmlParseData) {
            try {
                HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();

                insertPageStatement.setString(1, htmlParseData.getHtml());
                insertPageStatement.setString(2, htmlParseData.getText());
                insertPageStatement.setString(3, page.getWebURL().getURL());
                insertPageStatement.setTimestamp(4, new Timestamp(new java.util.Date().getTime()));
                insertPageStatement.executeUpdate();
            } catch (SQLException e) {
                logger.error("SQL Exception while storing webpage for url'{}'", page.getWebURL().getURL(), e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void storeUrl(String url,Integer status, Integer siteId) {
        try {
            insertUrlStatement.setString(1,url);
            insertUrlStatement.setInt(2,status);
            insertUrlStatement.setInt(3,siteId);
            insertUrlStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception while storing url", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeTitle(String url,String title) {
        try {
            insertTitleStatement.setString(1,url);
            insertTitleStatement.setString(2,title);
            insertTitleStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception while storing title", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeRedirect(String url,String redirectTo) {
        try {
            insertRedirectStatement.setString(1,url);
            insertRedirectStatement.setString(2,redirectTo);
            insertRedirectStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception while storing redirect", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeRobot(String url,String type,String content) {
        try {
            insertRobotStatement.setString(1,url);
            insertRobotStatement.setString(2,type);
            insertRobotStatement.setString(3,content);
            insertRobotStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception while storing robot", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeRefresh(String url,String type,String content) {
        try {
            insertRefreshStatement.setString(1,url);
            insertRefreshStatement.setString(2,type);
            insertRefreshStatement.setString(3,content);
            insertRefreshStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception while storing refresh", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeDescription(String url,String description) {
        try {
            insertDescriptionStatement.setString(1,url);
            insertDescriptionStatement.setString(2,description);
            insertDescriptionStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception while storing description", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeContent(String url,Boolean isH1Exist,Boolean isCanonicalExist,String urlQuery,Integer contentLength ,String contentHash) {
        try {
            insertContentStatement.setString(1,url);
            insertContentStatement.setBoolean(2,isH1Exist);
            insertContentStatement.setBoolean(3,isCanonicalExist);
            insertContentStatement.setString(4,urlQuery);
            insertContentStatement.setInt(5,contentLength);
            insertContentStatement.setString(6,contentHash);
            insertContentStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception while storing content", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeSimilarity(String srcUrl,String destUrl,Float percent) {
        try {
            insertSimilarityStatement.setString(1,srcUrl);
            insertSimilarityStatement.setString(2,destUrl);
            insertSimilarityStatement.setFloat(3,percent);
            insertSimilarityStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception while storing similarity", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String,String> getHashes(String host) {
        Map<String,String> hashes = new HashMap<>();

         try {
            getHashesStatement.setString(1,host);
            ResultSet rs = getHashesStatement.executeQuery();
            while (rs.next()) {
                hashes.put(rs.getString("url"),rs.getString("content_hash"));
            }
        } catch (SQLException e) {
            logger.error("SQL Exception while getting hashes", e);
            throw new RuntimeException(e);
        }finally {
            if (getHashesStatement != null) {
                try {
                    getHashesStatement.close();
                } catch (SQLException e) {
                    logger.error("SQL Exception while closing getting hashes", e);
                    throw new RuntimeException(e);
                }
            }
        }
        return hashes;
    }

    @Override
    public void removeSite(String url) {
        try {
            removeSiteStatement.setString(1,url);
            removeSiteStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception while removing old urls of the site", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer getSite(String host,Integer user_id) {
        Integer id = null;
        try {
            getSiteStatement.setString(1,host);
            getSiteStatement.setInt(2,user_id);
            ResultSet rs = getSiteStatement.executeQuery();
            id = rs.getInt("id");
        } catch (SQLException e) {
            logger.error("SQL Exception while getting site", e);
            throw new RuntimeException(e);
        }finally {
            if (getSiteStatement != null) {
                try {
                    getSiteStatement.close();
                } catch (SQLException e) {
                    logger.error("SQL Exception while closing getting site", e);
                    throw new RuntimeException(e);
                }
            }
        }
        return id;
    }

    @Override
    public void updateJob(String status,Timestamp finishTime,Integer siteId) {
        try {
            updateJobStatement.setString(1,status);
            updateJobStatement.setTimestamp(2,finishTime);
            updateJobStatement.setInt(3,siteId);
            updateJobStatement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL Exception while update job", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (comboPooledDataSource != null) {
            comboPooledDataSource.close();
        }
    }
}