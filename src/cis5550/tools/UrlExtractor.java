package cis5550.tools;

import cis5550.kvs.KVSClient;
import cis5550.model.CompleteUrl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UrlExtractor {
    private static final Logger logger = Logger.getLogger(UrlExtractor.class);
    public static List<String> extractUrls(byte[] pageContentBytes) {
        Pattern LINK_PATTERN = Pattern.compile(
                "<a [^>]*href=[\"']([^\"']*)[\"']",
                Pattern.CASE_INSENSITIVE
        );
        List<String> urls = new ArrayList<>();
        Matcher matcher = LINK_PATTERN.matcher(new String(pageContentBytes));

        while (matcher.find()) {
            urls.add(matcher.group(1));
        }

        return urls.stream().distinct().toList();
    }

    public static List<CompleteUrl> normalizeAndFilter(String baseUrl, List<String> extractResult) {
        List<CompleteUrl> completeUrls = new ArrayList<>();
        for (String url : extractResult) {
            logger.debug("Trying to normalize URL: " + url);
            try {
                CompleteUrl currentUrl = CompleteUrl.getCompleteUrl(baseUrl, url);
                if(!isWantedUrl(currentUrl.toString())) {
                    logger.debug("URL is not wanted: " + currentUrl);
                    continue;
                }
                completeUrls.add(currentUrl);
                logger.debug("Normalized URL: " + currentUrl);
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid URL: " + url);
            }
        }
        return completeUrls;
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
}
