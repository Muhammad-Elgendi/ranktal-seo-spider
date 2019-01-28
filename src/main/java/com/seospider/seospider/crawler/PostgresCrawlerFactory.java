package com.seospider.seospider.crawler;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import com.seospider.seospider.db.impl.DBServiceImpl;


/**
 * Created by rz on 03.06.2016.
 */
public class PostgresCrawlerFactory implements CrawlController.WebCrawlerFactory<PostgresWebCrawler> {

    private ComboPooledDataSource comboPooledDataSource;

    public PostgresCrawlerFactory(ComboPooledDataSource comboPooledDataSource) {
        this.comboPooledDataSource = comboPooledDataSource;
    }

    public PostgresWebCrawler newInstance() throws Exception {
        return new PostgresWebCrawler(new DBServiceImpl(comboPooledDataSource));
    }
}
