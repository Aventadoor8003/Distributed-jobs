package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.tools.HtmlProcessor;
import cis5550.tools.Logger;

import java.util.List;

public class Indexer {
    private static final Logger logger = Logger.getLogger(Indexer.class);

    public static void run(FlameContext context, String[] args) throws Exception {
        context.fromTable("pt-crawl", row -> row.get("url") + "|" + row.get("page")).mapToPair(s3 -> {
            String[] urlPage = s3.split("\\|");
            return new FlamePair(urlPage[0], urlPage[1]);
        }).flatMapToPair(s -> HtmlProcessor.processHtmlString(s._2()).stream()
                    .map(word -> new FlamePair(word, s._1())).toList())
                .foldByKey("", (s1, s2) -> s1 + (s1.isEmpty() ? "" : ",") + s2)
                .saveAsTable("pt-index");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Indexer");
    }
}
