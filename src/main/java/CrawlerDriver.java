import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.*;

public class CrawlerDriver {
    private final Set<String> linkSet = new ConcurrentSkipListSet<>();
    private final Set<String> visitedLinkSet = new ConcurrentSkipListSet<>();
    private static final Logger LOG = LogManager.getLogger("consoleLogger");
    private static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
    private static final BlockingQueue workQueue = new LinkedBlockingQueue();
    private static final ThreadPoolExecutor POOL = new ThreadPoolExecutor(MAX_THREADS, 20,1L,TimeUnit.SECONDS, workQueue);
    private static final String REPOSITORY_NAME;
    private static final String USER_NAME;
    private static final String GITHUB_HOST = "https://github.com";
    static{
        Properties props = readDataFromProperties();
        USER_NAME = props.getProperty("crawler.target.user");
        REPOSITORY_NAME = props.getProperty("crawler.target.repository");
    }
    public static void main(String[] args) {
        CrawlerDriver cd = new CrawlerDriver();
        cd.startCrawling(String.format(GITHUB_HOST+"/%s/%s", USER_NAME, REPOSITORY_NAME));
        try{
            await();
            cd.crawlFromLinks();
        }catch (InterruptedException ie){
            LOG.error(ie.getMessage());
        }finally {
           POOL.shutdown();
        }


    }

    public void startCrawling(String url) {
        Document doc;
        if (!url.startsWith("https://")) {
            url = GITHUB_HOST + url;
        }
        if (url != null && url.contains(USER_NAME) && url.contains(REPOSITORY_NAME) && !visitedLinkSet.contains(url)) {
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
            if (url.contains(USER_NAME) && url.contains(REPOSITORY_NAME)) {
                if (!url.startsWith("https://")) {
                    url = GITHUB_HOST + url;
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

    private static void await() throws InterruptedException{
        while(POOL.getTaskCount() != POOL.getCompletedTaskCount()){
            Thread.sleep(5000);
        }
    }

    private static Properties readDataFromProperties(){
        String resourceName = "application.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            props.load(resourceStream);
        }catch(IOException ioe){
            LOG.error(ioe.getMessage());
        }
        return props;
    }
}
