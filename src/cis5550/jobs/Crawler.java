package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.model.CompleteUrl;
import cis5550.model.HeadResult;
import cis5550.model.StatusType;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.RobotsTxtParser;
import cis5550.tools.URLParser;
//import org.jsoup.Jsoup;
//import org.jsoup.nodes.Document;
//import org.jsoup.nodes.Element;
//import org.jsoup.select.Elements;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


//-: only retrieve robots.txt once
//-: filter out urls with assigned suffixes
//TODO: Check the gap between two requests
//-: Optimize algorithm
public class Crawler {
    private static final Logger logger = Logger.getLogger(Crawler.class);
    public static void run(FlameContext context, String[] args) throws Exception {
        if(args.length != 1 && args.length != 2) {
            logger.fatal("Invalid usage, parameters: " + Arrays.toString(args));
            context.output("""
                    Usage: Crawler <seed URL> [blacklist]
                      <seed URL>     - The starting URL for the crawl.
                      [blacklist]    - (Optional) The name of the blacklist.""");
            return;
        }

        logger.debug("Starting crawler");
        context.output("OK: Crawler started. urls: \n");
        String seedUrlString;
        try {
            seedUrlString = new CompleteUrl(URLParser.parseURL(args[0])).toString();
        } catch (IllegalArgumentException e) {
            logger.fatal("Invalid seed URL: " + args[0]);
            context.output("ERROR: Invalid seed URL: " + args[0]);
            return;
        }

        //try to download robots.txt and put it in kvs
        URL seedUrl = URI.create(seedUrlString).toURL();
        URL robotsTxtUrl = new URL(seedUrl.getProtocol() + "://" + seedUrl.getHost() + "/robots.txt");
        try(InputStream is = robotsTxtUrl.openStream()) {
            byte[] robotsTxtBytes = is.readAllBytes();
            context.getKVS().put("robots", "robots.txt", seedUrl.getHost(), robotsTxtBytes);
        } catch (IOException e) {
            logger.error("Error reading robots.txt", e);
        }

        String blacklist = args.length == 2 ? args[1] : null;

        FlameRDD urlQueue = context.parallelize(Collections.singletonList(seedUrlString));
        while (urlQueue.count() > 0) {
            urlQueue = urlQueue.flatMap(urlString -> {
                URL url = URI.create(urlString).toURL();

                //Parse robots.txt
                RobotsTxtParser robotsTxtParser;
                try {
                    byte[] robotsTxtBytes = context.getKVS().get("robots", "robots.txt", url.getHost());
                    robotsTxtParser = new RobotsTxtParser(robotsTxtBytes);
                } catch (IOException e) {
                    logger.error("Error reading robots.txt", e);
                    return Collections.emptyList();
                }

                double crawlDelaySeconds = robotsTxtParser.getCrawlDelay("cis5550-crawler");
                boolean isAllowed = robotsTxtParser.isAllowed("cis5550-crawler", url.getPath());
                if(!isAllowed) {
                    logger.warn("URL: " + urlString + " is not allowed by robots.txt");
                    return Collections.emptyList();
                }

                final KVSClient client = context.getKVS();

                //Check if the url is visited
                Row checkRow = client.getRow("hosts", url.toString());
                if(checkRow != null && checkRow.get("get") != null) {
                    logger.debug("URL: " + urlString + " already crawled");
                    return Collections.emptyList();
                }

                //perform head request and decide whether you go for a get request
                HeadResult headResult = doHead(url, client, crawlDelaySeconds);
                if(headResult == null) {
                    logger.warn("URL: " + urlString + " returned null head result");
                    return Collections.emptyList();
                }

                //switch response code and check content type
                StatusType statusType = StatusType.fromCode(headResult.getResponseCode());
                switch (statusType) {
                    case OK -> {
                        //check content type
                        if(headResult.getContentType() == null || !headResult.getContentType().startsWith("text/html")) {
                            logger.debug("URL: " + urlString + " is not html");
                            return Collections.emptyList();
                        }
                    }
                    case REDIRECT -> {
                        String location = headResult.getRedirectLocation();
                        location = CompleteUrl.getCompleteUrl(urlString, location).toString();
                        logger.info("URL: " + urlString + " redirected to: " + location);
                        return Collections.singletonList(location);
                    }
                    case OTHER -> {
                        logger.warn("URL: " + urlString + " returned response code: " + headResult.getResponseCode());
                        return Collections.emptyList();
                    }
                }

                //sleep for a while
                Instant now = Instant.now();
                Instant lastHeadInstant = getLastOperationInstant(url.toString(), "get", client);
                long crawlDelayNanos = (long) (crawlDelaySeconds * 1_000_000_000);
                Duration threshold = Duration.ofNanos(crawlDelayNanos);
                Duration durationBetween = Duration.between(lastHeadInstant, now);
                if (durationBetween.compareTo(threshold) < 0) {
                    logger.debug("URL: " + url + " last head operation was " + durationBetween + " ago, waiting");
                    long sleepTime = threshold.minus(durationBetween).toMillis();
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        logger.error("Error sleeping", e);
                    }
                }

                //perform get request
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setRequestProperty("User-Agent", "cis5550-crawler");
                connection.connect();
                client.put("hosts", url.toString(), "get", Instant.now().toString());

                List<String> levelResult = new ArrayList<>();
                int responseCode = connection.getResponseCode();
                if(responseCode != 200) {
                    logger.warn("URL: " + urlString + " returned response code: " + responseCode);
                    return Collections.emptyList();
                }

                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = connection.getInputStream().read(buffer)) != -1) {
                        baos.write(buffer, 0, bytesRead);
                    }
                    byte[] pageContentBytes = baos.toByteArray();
                    client.put("pt-crawl", Hasher.hash(urlString), "page", pageContentBytes);
                    List<String> extractedUrls = extractUrls(pageContentBytes);
                    List<CompleteUrl> completeUrls = normalizeAndFilter(urlString, extractedUrls, client);
                    levelResult.addAll(completeUrls.stream().map(CompleteUrl::toString).toList());
                } catch (IOException e) {
                    logger.error("Error reading from URL: " + urlString, e);
                }
                logger.debug("Got level result " + levelResult);
                return levelResult;
            });
        }

        Iterator<Row> iter = context.getKVS().scan("pt-crawl");
        int s = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            s++;
            context.output(row.get("url") + "\n");
        }
        context.output("OK: Crawler finished. " + s + " urls crawled.\n");
    }

    private static List<String> extractUrls(byte[] pageContentBytes) {
        Pattern LINK_PATTERN = Pattern.compile(
                "<a [^>]*href=[\"']([^\"']*)[\"']",
                Pattern.CASE_INSENSITIVE
        );
        List<String> urls = new ArrayList<>();
        Matcher matcher = LINK_PATTERN.matcher(new String(pageContentBytes));

        while (matcher.find()) {
            urls.add(matcher.group(1));
        }

        return urls;
    }

    private static List<CompleteUrl> normalizeAndFilter(String baseUrl, List<String> extractResult, KVSClient client) {
        List<CompleteUrl> completeUrls = new ArrayList<>();
        for (String url : extractResult) {
            try {
                CompleteUrl currentUrl = CompleteUrl.getCompleteUrl(baseUrl, url);
                if(client.existsRow("pt-crawl", Hasher.hash(currentUrl.toString()))) {
                    continue;
                }
                if(!isWantedUrl(currentUrl.toString())) {
                    continue;
                }
                completeUrls.add(currentUrl);
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid URL: " + url);
            } catch (FileNotFoundException e) {
                logger.warn("Record not found for url: " + url);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return completeUrls;
    }

    private static HeadResult doHead(URL url, KVSClient client, double crawlDelaySeconds) {
        try {
            Instant now = Instant.now();
            Instant lastHeadInstant = getLastOperationInstant(url.toString(), "head", client);

            long crawlDelayNanos = (long) (crawlDelaySeconds * 1_000_000_000);
            Duration threshold = Duration.ofNanos(crawlDelayNanos);
            Duration durationBetween = Duration.between(lastHeadInstant, now);
            if (durationBetween.compareTo(threshold) < 0) {
                logger.debug("URL: " + url + " last head operation was " + durationBetween + " ago, waiting");
                long sleepTime = threshold.minus(durationBetween).toMillis();
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    logger.error("Error sleeping", e);
                }
            }

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.setRequestProperty("User-Agent", "cis5550-crawler");
            connection.setInstanceFollowRedirects(false);
            connection.connect();
            int responseCode = connection.getResponseCode();
            client.put("hosts", url.toString(), "head", Instant.now().toString());

            Row newRow = new Row(Hasher.hash(url.toString()));
            newRow.put("url", url.toString());
            newRow.put("responseCode", responseCode + "");
            newRow.put("contentType", connection.getContentType());
            newRow.put("length", connection.getContentLength() + "");
            client.putRow("pt-crawl", newRow);

            String contentType = connection.getContentType();
            return new HeadResult(url.toString(), responseCode, contentType, connection.getContentLength(), connection.getHeaderFields());
        } catch (IOException e) {
            logger.error("Error reading from URL: " + url, e);
            return null;
        }
    }


    private static Instant getLastOperationInstant(String url, String op, KVSClient client) throws IOException {
        Row row = client.getRow("hosts", url);
        if (row == null) {
            return Instant.MIN;
        }
        String instantString = row.get(op);
        if (instantString == null) {
            return Instant.MIN;
        }
        return Instant.parse(instantString);
    }

    private static boolean isWantedUrl(String url) {
        CompleteUrl completeUrl;
        try {
            completeUrl = new CompleteUrl(URLParser.parseURL(url));
        } catch (IllegalArgumentException e) {
            logger.error("Invalid URL while filtering: " + url);
            return false;
        }

        String path = completeUrl.getPath();
        return path == null || (!path.endsWith(".txt") && !path.endsWith(".jpg") && !path.endsWith(".jpeg") && !path.endsWith(".gif") && !path.endsWith(".png"));
    }

    public static void main(String[] args) {
        System.out.println("Hello world");
    }

}
