package com.seospider.seospider.crawler;

import com.seospider.seospider.SimpleLauncher;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import com.seospider.seospider.db.DBService;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import org.slf4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;

import java.util.regex.Pattern;
import org.apache.http.Header;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class PostgresWebCrawler extends WebCrawler {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(PostgresWebCrawler.class);

    private final DBService postgresDBService;
//    Map<String, Object> problems;

    public PostgresWebCrawler(DBService postgresDBService) {
        this.postgresDBService = postgresDBService;
    }

    private final static Pattern FILTERS = Pattern.compile(".*(\\.("
            + "css|bmp|gif|jpe?g|JPE?G|png|tiff?|ico|nef|raw"
            + "|mid|mp2|mp3|mp4|wav|wma|flv|mpe?g"
            + "|avi|mov|mpeg|ram|m4v|wmv|rm|smil"
            + "|pdf|doc|docx|pub|xls|xlsx|vsd|ppt|pptx"
            + "|zip|swf|rar|gz|bz2|7z|bin"
            + "|xml|txt|java|c|cpp|exe"
            + "))$");

    /**
     * This function is called just before starting the crawl by this crawler
     * instance. It can be used for setting up the data structures or
     * initializations needed by this crawler instance.
     */
    @Override
    public void onStart() {
        // Do nothing by default
        // Sub-classed can override this to add their custom functionality
//        problems = new HashMap<>();
    }

    /**
     * This method receives two parameters. The first parameter is the page in
     * which we have discovered this new url and the second parameter is the new
     * url. You should implement this function to specify whether the given url
     * should be crawled or not (based on your crawling logic). In this example,
     * we are instructing the crawler to ignore urls that have css, js, git, ...
     * extensions and to only accept urls that start with
     * "http://www.ics.uci.edu/". In this case, we didn't need the referringPage
     * parameter to make the decision.
     *
     * @param referringPage
     * @param url
     * @return
     */
    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String href = url.getURL().toLowerCase();
        return !FILTERS.matcher(href).matches()
                && href.contains(SimpleLauncher.mainUrl);
    }

    /**
     * This function is called if the crawler encounters a page with a 3xx
     * status code
     *
     * @param page Partial page object
     */
    @Override
    protected void onRedirectedStatusCode(Page page) {
//        int statusCode = page.getStatusCode();

//        ArrayList<String> status3xx = new ArrayList<>();
//        status3xx.add(0, page.getWebURL().getURL());
//        status3xx.add(1, page.getRedirectedToUrl());
//        status3xx.add(2, String.valueOf(statusCode));
//        problems.put("status3xx", status3xx);

        // store url
        try {
            postgresDBService.storeUrl(page.getWebURL().getURL(),page.getStatusCode(), SimpleLauncher.siteId);
        } catch (RuntimeException e) {
            logger.error("Storing url in onRedirectedStatusCode() failed", e);
        }

        // store redirect
        try {
            postgresDBService.storeRedirect(page.getWebURL().getURL(),page.getRedirectedToUrl());
        } catch (RuntimeException e) {
            logger.error("Storing redirect in onRedirectedStatusCode() failed", e);
        }



//        if (page.getStatusCode() == 302 || page.getStatusCode() == 303 || page.getStatusCode() == 307) {
//            /*
//            Temporary Redirect Problems
//            Using HTTP header refreshes, 302, 303 or 307 redirects will cause search engine crawlers
//            to treat the redirect as temporary and not pass any link juice (ranking power).
//            We highly recommend that you replace temporary redirects with 301 redirects.
//            handlePageStatusCode() and Search for HTTP header refreshes.
//             */
//            ArrayList<String> temporaryRedirect = new ArrayList<>();
//            temporaryRedirect.add(0, page.getWebURL().getURL());
//            temporaryRedirect.add(1, page.getRedirectedToUrl());
//            temporaryRedirect.add(2, String.valueOf(statusCode));
//            problems.put("headerTemporaryRedirect", temporaryRedirect);
//        }

//        // print problems map
//        JSONObject json = new JSONObject(problems);
//        System.out.printf("JSON: %s", json.toString());

    }

    /**
     * This function is called if the crawler encountered an unexpected http
     * status code ( a status code other than 3xx)
     *
     * @param urlStr URL in which an unexpected error was encountered while
     * crawling
     * @param statusCode Html StatusCode
     * @param contentType Type of Content
     * @param description Error Description
     */
    @Override
    protected void onUnexpectedStatusCode(String urlStr, int statusCode, String contentType,
                                          String description) {
//        if (statusCode > 399 && statusCode < 500) {
//            // 4xx problems
//            ArrayList<String> status4xx = new ArrayList<>();
//            status4xx.add(0, urlStr);
//            status4xx.add(1, String.valueOf(statusCode));
//            problems.put("status4xx", status4xx);
//        }
//        if (statusCode > 499 && statusCode < 600) {
//            // 5xx problems
//            ArrayList<String> status5xx = new ArrayList<>();
//            status5xx.add(0, urlStr);
//            status5xx.add(1, String.valueOf(statusCode));
//            problems.put("status5xx", status5xx);
//        }
//        // print problems map
//        JSONObject json = new JSONObject(problems);
//        System.out.printf("JSON: %s", json.toString());

        // store url
        try {
            postgresDBService.storeUrl(urlStr,statusCode, SimpleLauncher.siteId);
        } catch (RuntimeException e) {
            logger.error("Storing url in onUnexpectedStatusCode() failed", e);
        }

    }

    /**
     * This function is called when a page is fetched and ready to be processed
     * by your program.
     *
     * @param page
     */
    @Override
    public void visit(Page page) {
            if (page.getParseData() instanceof HtmlParseData) {
                HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
                Document doc = Jsoup.parse(htmlParseData.getHtml());
//                problems.put("pageUrl", page.getWebURL().getURL());

                // store url
                try {
                    postgresDBService.storeUrl(page.getWebURL().getURL(),page.getStatusCode(), SimpleLauncher.siteId);
                } catch (RuntimeException e) {
                    logger.error("Storing url in vist() failed", e);
                }



                Header[] headers = page.getFetchResponseHeaders();
                for (Header header : headers) {
                    if (header.getName().equals("X-Robots-Tag")) {
                        if (header.getValue().contains("noindex")) {
                            // X-Robots-Tag: noindex problem
//                            ArrayList<String> xRobotsTagNoindex = new ArrayList<>();
//                            xRobotsTagNoindex.add(0, page.getWebURL().getURL());
//                            xRobotsTagNoindex.add(1, "noindex");
//                            problems.put("xRobotsTagNoindex", xRobotsTagNoindex);

                            // store xrobots
                            try {
                                postgresDBService.storeRobot(page.getWebURL().getURL(),"xRobots","noindex");
                            } catch (RuntimeException e) {
                                logger.error("Storing xRobots noindex failed", e);
                            }

                        }
                        if (header.getValue().contains("nofollow")) {
                            // X-Robots-Tag: nofollow problem
//                            ArrayList<String> xRobotsTagNofollow = new ArrayList<>();
//                            xRobotsTagNofollow.add(0, page.getWebURL().getURL());
//                            xRobotsTagNofollow.add(1, "nofollow");
//                            problems.put("xRobotsTagNofollow", xRobotsTagNofollow);

                            // store xrobots
                            try {
                                postgresDBService.storeRobot(page.getWebURL().getURL(),"xRobots","nofollow");
                            } catch (RuntimeException e) {
                                logger.error("Storing xRobots nofollow failed", e);
                            }


                        }
                        if (header.getValue().contains("none")) {
                            // X-Robots-Tag: noindex, nofollow problems
//                            ArrayList<String> xRobotsTagNone = new ArrayList<>();
//                            xRobotsTagNone.add(0, page.getWebURL().getURL());
//                            xRobotsTagNone.add(1, "none");
//                            problems.put("xRobotsTagNone", xRobotsTagNone);

                            // store xrobots
                            try {
                                postgresDBService.storeRobot(page.getWebURL().getURL(),"xRobots","none");
                            } catch (RuntimeException e) {
                                logger.error("Storing xRobots none failed", e);
                            }


                        }
                    }
                    if (header.getName().equals("Refresh")) {
                        // HTTP header refreshes Temporary Redirect Problems
                        /*
                           will cause search engine crawlers
                        to treat the redirect as temporary and not pass any link juice (ranking power).
                        We highly recommend that you replace temporary redirects with 301 redirects.
                        handlePageStatusCode() and Search for HTTP header refreshes.
                         */
//                        ArrayList<String> headerRefreshes = new ArrayList<>();
//                        headerRefreshes.add(0, page.getWebURL().getURL());
//                        headerRefreshes.add(1, "Header Refresh");
//                        headerRefreshes.add(2, header.getValue());
//                        problems.put("headerRefreshes", headerRefreshes);

                        // store header refresh
                        try {
                            postgresDBService.storeRefresh(page.getWebURL().getURL(),"headerRefresh",header.getValue());
                        } catch (RuntimeException e) {
                            logger.error("Storing header refresh failed", e);
                        }



                    }
                }


                Elements titleTags = doc.selectFirst("head").select("title");
                if (titleTags.isEmpty()) {
                    // Missing Title Problem
//                    ArrayList<String> titles = new ArrayList<>();
//                    titles.add(0, page.getWebURL().getURL());
//                    titles.add(1, "Missing Title");
//                    problems.put("MissingTitle", titles);

                    // store title
                    try {
                        postgresDBService.storeTitle(page.getWebURL().getURL(),"");
                    } catch (RuntimeException e) {
                        logger.error("Storing empty title failed", e);
                    }


                } else {

                    if (titleTags.size() > 1) {
                        // Multiple Titles Problem
//                        ArrayList<String> titles = new ArrayList<>();
//                        titles.add(0, page.getWebURL().getURL());
//                        titles.add(1, "Multiple Titles");
//                        problems.put("MultipleTitles", titles);
                        for (Element titleTag : titleTags) {

                            // store title
                            try {
                                postgresDBService.storeTitle(page.getWebURL().getURL(),titleTag.text());
                            } catch (RuntimeException e) {
                                logger.error("Storing multiple title failed", e);
                            }


                        }
                    }else {

                        String title = doc.selectFirst("head").select("title").first().text();


                        // store title
                        try {
                            postgresDBService.storeTitle(page.getWebURL().getURL(),title);
                        } catch (RuntimeException e) {
                            logger.error("Storing title failed", e);
                        }



//                    int titleLength = title.length();
//                    problems.put("title", title);
//                    if (titleLength > 60) {
//                        // Title too long flag is set
//                        /*
//                        Make a method to add url and problem details e.g. title itself and so on
//                        to the lists that will be sent to database
//                         */
//                        ArrayList<String> titles = new ArrayList<>();
//                        titles.add(0, page.getWebURL().getURL());
//                        titles.add(1, "Title too long");
//                        titles.add(2, String.valueOf(titleLength));
//                        titles.add(3, title);
//                        problems.put("TitleTooLong", titles);
//                    } else if (titleLength < 25) {
//                        // Title too short flag is set
//                        ArrayList<String> titles = new ArrayList<>();
//                        titles.add(0, page.getWebURL().getURL());
//                        titles.add(1, "Title too short");
//                        titles.add(2, String.valueOf(titleLength));
//                        titles.add(3, title);
//                        problems.put("TitleTooShort", titles);
//                    }
                    }
                }

                Elements descriptionTags = doc.selectFirst("head").select("meta[name=description]");
                if (descriptionTags.isEmpty()) {
                    //Missing Description
//                    ArrayList<String> description = new ArrayList<>();
//                    description.add(0, page.getWebURL().getURL());
//                    description.add(1, "Missing Description");
//                    problems.put("MissingDescription", description);

                    // store description
                    try {
                        postgresDBService.storeDescription(page.getWebURL().getURL(),"");
                    } catch (RuntimeException e) {
                        logger.error("Storing empty description failed", e);
                    }

                } else {

                    String description = doc.selectFirst("head").select("description").first().text();

                    // store description
                    try {
                        postgresDBService.storeDescription(page.getWebURL().getURL(),description);
                    } catch (RuntimeException e) {
                        logger.error("Storing description failed", e);
                    }


//                    int descriptionLength = description.length();
//                    if (descriptionLength > 300) {
//                        // description too long flag is set
//                        ArrayList<String> descriptionLengthList = new ArrayList<>();
//                        descriptionLengthList.add(0, page.getWebURL().getURL());
//                        descriptionLengthList.add(1, "Description too long");
//                        descriptionLengthList.add(2, String.valueOf(descriptionLength));
//                        descriptionLengthList.add(3, description);
//                        problems.put("DescriptionTooLong", descriptionLengthList);
//                    } else if (descriptionLength < 50) {
//                        // description too short flag is set
//                        ArrayList<String> descriptionLengthList = new ArrayList<>();
//                        descriptionLengthList.add(0, page.getWebURL().getURL());
//                        descriptionLengthList.add(1, "Description too short");
//                        descriptionLengthList.add(2, String.valueOf(descriptionLength));
//                        descriptionLengthList.add(3, description);
//                        problems.put("DescriptionTooShort", descriptionLengthList);
//                    }

                }

//                int urlLength = page.getWebURL().getURL().length();
//                if (urlLength > 75) {
//                    // Url is too long
//                    ArrayList<String> urlList = new ArrayList<>();
//                    urlList.add(0, page.getWebURL().getURL());
//                    urlList.add(1, "URL too long");
//                    urlList.add(2, String.valueOf(urlLength));
//                    problems.put("URLTooLong", urlList);
//                }

                Elements robotsTags = doc.selectFirst("head").select("meta[name=robots]");
                if (!robotsTags.isEmpty()) {
                    for (Element tag : robotsTags) {
                        if (tag.attr("content").contains("noindex")) {
                            //Meta noindex problem
//                            ArrayList<String> robotsTagNoindex = new ArrayList<>();
//                            robotsTagNoindex.add(0, page.getWebURL().getURL());
//                            robotsTagNoindex.add(1, "noindex");
//                            problems.put("metaTagNoindex", robotsTagNoindex);

                            // store robot
                            try {
                                postgresDBService.storeRobot(page.getWebURL().getURL(),"metaTag","noindex");
                            } catch (RuntimeException e) {
                                logger.error("Storing meta tag noindex failed", e);
                            }


                        }
                        if (tag.attr("content").contains("nofollow")) {
                            //Meta Nofollow problem
//                            ArrayList<String> robotsTagNofollow = new ArrayList<>();
//                            robotsTagNofollow.add(0, page.getWebURL().getURL());
//                            robotsTagNofollow.add(1, "nofollow");
//                            problems.put("metaTagNofollow", robotsTagNofollow);

                            // store robot
                            try {
                                postgresDBService.storeRobot(page.getWebURL().getURL(),"metaTag","nofollow");
                            } catch (RuntimeException e) {
                                logger.error("Storing meta tag nofollow failed", e);
                            }

                        }
                    }
                }

                Elements metaRefresh = doc.selectFirst("head").select("meta[http-equiv=refresh]");
                if (!metaRefresh.isEmpty()) {
                    // Meta Refresh Problem
//                    ArrayList<String> metaElement = new ArrayList<>();
//                    metaElement.add(0, page.getWebURL().getURL());
//                    metaElement.add(1, "Meta Refresh");
//                    metaElement.add(2, metaRefresh.first().attr("content"));
//                    problems.put("MetaRefresh", metaElement);

                    // store refresh
                    try {
                        postgresDBService.storeRefresh(page.getWebURL().getURL(),"MetaRefresh",metaRefresh.first().attr("content"));
                    } catch (RuntimeException e) {
                        logger.error("Storing meta refresh failed", e);
                    }

                }

                boolean isH1Exit =true;

                Elements H1Tags = doc.selectFirst("body").select("h1");
                if (H1Tags.isEmpty()) {
                    // Missing H1 Problem
//                    ArrayList<String> h1 = new ArrayList<>();
//                    h1.add(0, page.getWebURL().getURL());
//                    h1.add(1, "Missing H1");
//                    problems.put("MissingH1", h1);
                    isH1Exit =false;
                }

                String bodyText = doc.selectFirst("body").text();
                String contentWithoutSpaces = bodyText.replaceAll("\\s+", "");

                Integer contentLength = contentWithoutSpaces.length();

//                if (contentWithoutSpaces.length() < 300) {
//                    // Thin Content Problem
//                    ArrayList<String> content = new ArrayList<>();
//                    content.add(0, page.getWebURL().getURL());
//                    content.add(1, "Thin Content");
//                    content.add(2, String.valueOf(contentWithoutSpaces.length()));
//                    problems.put("Thin Content", content);
//                }

                String urlQuery = "";

                try {
                    URL url = new URL(page.getWebURL().getURL());
                    urlQuery = url.getQuery();
                } catch (MalformedURLException ex) {
                    logger.error("Parsing URL failed", ex);
                }




//                if (url.getQuery() != null) {
//                    //Dynamic URL problem
//                    ArrayList<String> dynamicUrl = new ArrayList<>();
//                    dynamicUrl.add(0, page.getWebURL().getURL());
//                    dynamicUrl.add(1, "Dynamic URL");
//                    dynamicUrl.add(2, url.getQuery());
//                    problems.put("DynamicURL", dynamicUrl);
//                }

                Elements canonicalTags = doc.selectFirst("head").select("link[rel=canonical]");

                boolean isCanonicalExist = true;

                if (canonicalTags.isEmpty()) {
                    // Missing canonical Url Problem
//                    ArrayList<String> canonical = new ArrayList<>();
//                    canonical.add(0, page.getWebURL().getURL());
//                    canonical.add(1, "Missing canonical Url");
//                    problems.put("MissingCanonicalUrl", canonical);
                    isCanonicalExist =false;
                }

                // calculate hash of the page
                String hash = Similarities.calculateHash(htmlParseData.getHtml());

                // store content
                try {
                    postgresDBService.storeContent(page.getWebURL().getURL(),isH1Exit,isCanonicalExist,urlQuery,contentLength,hash);
                } catch (RuntimeException e) {
                    logger.error("Storing content failed", e);
                }

                // Slow Load Time Problem
                // Search how to get load time of the page into visit()
                //duplicate content problem > 92%
                //duplicate titles problem
                // Redirect chain

//                try {
//                    postgresDBService.store(page);
//                } catch (RuntimeException e) {
//                    logger.error("Storing failed", e);
//                }


//                // print problems map
//                JSONObject json = new JSONObject(problems);
//                System.out.printf("JSON: %s", json.toString());
            }

    }

    @Override
    public void onBeforeExit() {
        if (postgresDBService != null) {
            postgresDBService.close();
        }
    }
}
