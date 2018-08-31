package com.seospider.seospider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import com.seospider.seospider.crawler.PostgresCrawlerFactory;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import org.flywaydb.core.Flyway;


import java.net.URL;

public class SampleLauncher {

    public static String mainUrl;
    private static final Logger logger = LoggerFactory.getLogger(SampleLauncher.class);

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            logger.info("Needed parameters: ");
            logger.info("\t Seed URL (start crawling with this URL)");
            logger.info("\t maxPagesToFetch (number of pages to be fetched)");
            return;
        }

        URL url = new URL(args[0]);
        mainUrl=args[0];
        int numberOfCrawlers = 31;


        CrawlConfig config = new CrawlConfig();

        config.setPolitenessDelay(100);

        config.setCrawlStorageFolder("/media/muhammad/disk/crawlerData/"+url.getHost());

        config.setMaxPagesToFetch(Integer.valueOf(args[1]));

        config.setRespectNoFollow(false);

        config.setRespectNoIndex(false);

        config.setUserAgentString("SEO-spider (https://github.com/Muhammad-Elgendi/SEO-spider/)");

        /*
         * Instantiate the controller for this crawl.
         */
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();

        robotstxtConfig.setUserAgentName("SEO-spider");

        PageFetcher pageFetcher = new PageFetcher(config);
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

        /*
         * For each crawl, you need to add some seed urls. These are the first
         * URLs that are fetched and then the crawler starts following links
         * which are found in these pages
         */
        controller.addSeed(args[0]);

        Flyway flyway = new Flyway();
        flyway.setDataSource(args[1], "crawler4j", "crawler4j");
        flyway.migrate();


        ComboPooledDataSource pool = new ComboPooledDataSource();
        pool.setDriverClass("org.postgresql.Driver");
        pool.setJdbcUrl(args[1]);
        pool.setUser("crawler4j");
        pool.setPassword("crawler4j");
        pool.setMaxPoolSize(numberOfCrawlers);
        pool.setMinPoolSize(numberOfCrawlers);
        pool.setInitialPoolSize(numberOfCrawlers);

        /*
         * Start the crawl. This is a blocking operation, meaning that your code
         * will reach the line after this only when crawling is finished.
         */
        controller.start(new PostgresCrawlerFactory(pool), numberOfCrawlers);

        pool.close();
    }

}