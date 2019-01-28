package com.seospider.seospider.db;

import com.github.s3curitybug.similarityuniformfuzzyhash.UniformFuzzyHash;
import edu.uci.ics.crawler4j.crawler.Page;

import java.sql.Timestamp;
import java.util.Map;

public interface DBService {

    void storePage(Page webPage);
    void storeUrl(String url,Integer status, Integer siteId);
    void storeTitle(String url,String title);
    void storeRedirect(String url,String redirectTo);
    void storeRobot(String url,String type,String content);
    void storeRefresh(String url,String type,String content);
    void storeDescription(String url,String description);
    void storeContent(String url,Boolean isH1Exist,Boolean isCanonicalExist,String urlQuery,Integer contentLength,String contentHash);
    void storeSimilarity(String srcUrl,String destUrl,Float percent);
    Map<String,String> getHashes(String host);
    void updateJob(String status, Timestamp finishTime, Integer siteId);
    void removeSite(String url);
    Integer getSite(String host,Integer user_id);
    void close();
}