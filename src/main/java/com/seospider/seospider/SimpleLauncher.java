package com.seospider.seospider;


import com.github.s3curitybug.similarityuniformfuzzyhash.UniformFuzzyHash;
import com.github.s3curitybug.similarityuniformfuzzyhash.UniformFuzzyHashes;
import com.seospider.seospider.crawler.Similarities;
import com.seospider.seospider.db.DBService;
import com.seospider.seospider.db.impl.DBServiceImpl;
import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import com.seospider.seospider.crawler.PostgresCrawlerFactory;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

import java.net.URL;
import java.sql.Timestamp;
import java.util.Map;

public class SimpleLauncher {

    public static  String mainUrl;
    public static Integer userId;
    public static Integer siteId;

    private static final Logger logger = LoggerFactory.getLogger(SimpleLauncher.class);

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            logger.info("Needed parameters: ");
            logger.info("\t Seed URL (start crawling with this URL)");
            logger.info("\t maxPagesToFetch (number of pages to be fetched)");
            logger.info("\t nuberOfCrawlers (number of crawlers)");
            logger.info("\t user id (id of user that request the crawling)");
            logger.info("\t site id (id of site that being crawled)");
            return;
        }

        URL url = new URL(args[0]);
        mainUrl= args[0];
        userId = Integer.valueOf(args[3]);
        siteId = Integer.valueOf(args[4]);
        int numberOfCrawlers = Integer.valueOf(args[2]);

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

        Dotenv dotenv = Dotenv.load();

//        Flyway flyway = new Flyway();
//        flyway.setDataSource(dotenv.get("JDBC_URL"), dotenv.get("DB_USER_NAME"), dotenv.get("DB_PASSWORD"));
//        flyway.migrate();


        ComboPooledDataSource pool = new ComboPooledDataSource();
        pool.setDriverClass("org.postgresql.Driver");
        pool.setJdbcUrl(dotenv.get("JDBC_URL"));
        pool.setUser(dotenv.get("DB_USER_NAME"));
        pool.setPassword(dotenv.get("DB_PASSWORD"));
        pool.setMaxPoolSize(numberOfCrawlers);
        pool.setMinPoolSize(numberOfCrawlers);
        pool.setInitialPoolSize(numberOfCrawlers);


        /**
         * Delete all old urls
         */
        deleteAllUrls(pool);

        /*
         * Start the crawl. This is a blocking operation, meaning that your code
         * will reach the line after this only when crawling is finished.
         */
        controller.start(new PostgresCrawlerFactory(pool), numberOfCrawlers);

        /**
         * Check Duplicate Content and Store Similarities
         */
        checkForDuplicateContent(pool);

        notifyBackend(pool,"Finished",getCurrentTimeStamp());

        pool.close();

        /**
         * Inform backend
         */
    }

    private static void checkForDuplicateContent(ComboPooledDataSource pool) throws Exception{
        DBService postgresDBService = new DBServiceImpl(pool);
        Map<String,String> hashesStrings = postgresDBService.getHashes(mainUrl);
        Map<String, UniformFuzzyHash>  map = UniformFuzzyHashes.computeHashesFromStrings(hashesStrings,61);
        Map similarities= UniformFuzzyHashes.computeAllHashesSimilarities(map);
        Similarities.saveAllHashesSimilarities(similarities,postgresDBService);
    }

    private static void notifyBackend(ComboPooledDataSource pool, String status, Timestamp finishTime) throws Exception{
        DBService postgresDBService = new DBServiceImpl(pool);
        postgresDBService.updateJob(status,finishTime,siteId);
    }


    private static void deleteAllUrls(ComboPooledDataSource pool) throws Exception{
        DBService postgresDBService = new DBServiceImpl(pool);
        postgresDBService.removeSite(mainUrl);
    }

    private static java.sql.Timestamp getCurrentTimeStamp() {

        return new java.sql.Timestamp(new java.util.Date().getTime());

    }

}