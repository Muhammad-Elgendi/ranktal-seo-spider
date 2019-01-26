package com.seospider.seospider.db;

import edu.uci.ics.crawler4j.crawler.Page;

import java.util.ArrayList;

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
    ArrayList<String> getHashes(String host);
    void removeSite(Integer id);
    Integer getSite(String host,Integer user_id);
    void close();
}