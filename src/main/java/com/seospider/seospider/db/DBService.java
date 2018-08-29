package com.seospider.seospider.db;

import edu.uci.ics.crawler4j.crawler.Page;

public interface DBService {

    void store(Page webPage);

    void close();
}