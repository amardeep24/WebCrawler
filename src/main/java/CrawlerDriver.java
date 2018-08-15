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

public class CrawlerDriver {
    private static final ExecutorService POOL;
    final Set<String> linkSet = new ConcurrentSkipListSet<>();
    final Set<String> visitedLinkSet = new ConcurrentSkipListSet<>();

    static {
        int noOFThreads = Runtime.getRuntime().availableProcessors();
        POOL = Executors.newFixedThreadPool(noOFThreads * 2);
    }

    public static void main(String[] args) {
        CrawlerDriver cd = new CrawlerDriver();
        cd.startCrawling("https://github.com/soumodeep3007/Node-Test");
        cd.crawlFromLinks();
        // POOL.shutdown();
    }

    public void startCrawling(String url) {
        Document doc;
        if (!url.startsWith("https://")) {
            url = "https://github.com" + url;
        }
        if (url != null && url.contains("soumodeep3007") && url.contains("Node-Test") && !visitedLinkSet.contains(url)) {
            try {
                // need http protocol
                System.out.println("startCrawling called with url " + url);
                visitedLinkSet.add(url);
                doc = Jsoup.connect(url).get();
                populateAllLinks(doc, linkSet);
                linkSet.forEach(link -> {
                    startCrawling(link);
                });
            } catch (IOException e) {
                // e.printStackTrace();
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
            // System.out.println("\nlink : " + url);
            if (url.contains("soumodeep3007") && url.contains("Node-Test")) {
                if (!url.startsWith("https://")) {
                    url = "https://github.com" + url;
                }
                linkSet.add(url);
            }
        });
    }

    private void crawlFromLinks() {
        System.out.println("All links parsed");
        linkSet.forEach(link -> {
           // POOL.submit(() -> {
                try {
                    Document doc = Jsoup.connect(link).get();
                    writeFileToDisk(doc.html());
                } catch (IOException e) {
                    e.printStackTrace();
                }
         //   });
        });
    }

    private void writeFileToDisk(String html) {
        System.out.println("Started writing to disk");
        try {
            html += "========================================NEW PAGE========================================";
            Files.write(Paths.get("D:\\crawled_data.txt"), html.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
