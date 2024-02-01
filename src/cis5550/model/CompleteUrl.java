package cis5550.model;

import java.net.URI;
import java.net.URISyntaxException;

public class CompleteUrl {

    private final String protocol;
    private final String host;
    private final String port;
    private String path;
    //TODO: Optimization - checke all elements, only create a new object when all four elements ar correct
    public CompleteUrl(String[] parseResult) throws IllegalArgumentException {
        if(parseResult.length != 4) throw new IllegalArgumentException("parseResult length must be 4");
        if(parseResult[0] == null) throw new IllegalArgumentException("protocol cannot be null");
        this.protocol = parseResult[0];
        this.host = parseResult[1];
        if(parseResult[2] == null) {
            switch (protocol) {
                case "http" -> this.port = "80";
                case "https" -> this.port = "443";
                default -> throw new IllegalArgumentException("protocol must be http or https");
            }
        } else {
            this.port = parseResult[2];
        }

        //Strip document fragment
        String tPath = parseResult[3];
        int hashTag = tPath.indexOf('#');
        if(hashTag >= 0) {
            tPath = tPath.substring(0, hashTag);
        }
        this.path = tPath;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getPath() {
        return path;
    }

    public String toString() {
        return protocol + "://" + host + ":" + port + path;
    }

    public static CompleteUrl getCompleteUrl(String baseUrl, String url) {
        String[] parseResult = cis5550.tools.URLParser.parseURL(normalizeRelativePath(baseUrl, url));
        return new CompleteUrl(parseResult);
    }

    private static String normalizeRelativePath(String baseUrl, String url) {
        try {
            URI baseUri = new URI(baseUrl);
            URI relativeUri = new URI(url);
            URI resolvedUri = baseUri.resolve(relativeUri);
            URI normalizedUri = resolvedUri.normalize();
            return normalizedUri.toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

}
