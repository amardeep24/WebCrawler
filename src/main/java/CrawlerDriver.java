import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CrawlerDriver {
    private final Set<String> linkSet = new ConcurrentSkipListSet<>();
    private final Set<String> visitedLinkSet = new ConcurrentSkipListSet<>();
    private static final Logger LOG = LogManager.getLogger("consoleLogger");
    private static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService POOL = Executors.newFixedThreadPool(MAX_THREADS * 2);

    public static void main(String[] args) {
        CrawlerDriver cd = new CrawlerDriver();
        cd.startCrawling("https://github.com/soumodeep3007/Node-Test");
        try{
            POOL.awaitTermination(1, TimeUnit.MINUTES);
            cd.crawlFromLinks();
            POOL.awaitTermination(1, TimeUnit.MINUTES);
        }catch (InterruptedException ie){
            LOG.error(ie.getMessage());
        }finally {
           POOL.shutdown();
        }


    }

    public void startCrawling(String url) {
        Document doc;
        if (!url.startsWith("https://")) {
            url = "https://github.com" + url;
        }
        if (url != null && url.contains("soumodeep3007") && url.contains("Node-Test") && !visitedLinkSet.contains(url)) {
            try {
                // need http protocol
                LOG.info("startCrawling called with url " + url);
                visitedLinkSet.add(url);
                doc = Jsoup.connect(url).get();
                populateAllLinks(doc, linkSet);
                linkSet.forEach(
                        link -> {
                            POOL.submit(() -> startCrawling(link));
                        }
                );
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        } else {
            return;
        }

    }

    private void populateAllLinks(Document doc, Set<String> linkSet) {
        // get all links
        Elements links = doc.select("a[href]");
        links.forEach(link -> {
            String url = link.attr("href");
            if (url.contains("soumodeep3007") && url.contains("Node-Test")) {
                if (!url.startsWith("https://")) {
                    url = "https://github.com" + url;
                }
                linkSet.add(url);
            }
        });
    }

    private void crawlFromLinks() {
        LOG.info("All links parsed");
        linkSet.forEach(link -> {
           POOL.submit(() -> {
            try {
                Document doc = Jsoup.connect(link).get();
                writeFileToDisk(doc.html());
            } catch (IOException e) {
               LOG.error(e.getMessage());
            }
            });
        });
    }

    private void writeFileToDisk(String html) {
        LOG.info("Started writing to disk");
        try {
            html += "\n========================================NEW PAGE========================================\n";
            Files.write(Paths.get("D:\\crawled_data.txt"), html.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
           LOG.error(e.getMessage());
        }
    }
}
