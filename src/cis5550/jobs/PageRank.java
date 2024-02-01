package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.model.CompleteUrl;
import cis5550.model.PageRankContext;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.UrlExtractor;

import java.util.ArrayList;
import java.util.List;

public class PageRank {
    private static final Logger logger = Logger.getLogger(PageRank.class);
    public static void run(FlameContext context, String[] args) throws Exception {
        if(args.length != 1) {
            logger.fatal("Usage: PageRank <threshold>");
            return;
        }

        double threshold = Double.parseDouble(args[0]);

        FlameRDD rawRdd = context.fromTable("pt-crawl", row -> row.get("url") + "|" + row.get("page"));
        PageRankContext.initialize(rawRdd.count());
        logger.debug("Network size: " + PageRankContext.getNetworkSize());

        FlamePairRDD curRDD = rawRdd.mapToPair(s -> {
            String[] urlPage = s.split("\\|");
            String url = urlPage[0];
            logger.debug("[Reading pt-crawl] Processing: " + url);
            String page = urlPage[1];
            List<String> urls = UrlExtractor.extractUrls(page.getBytes());
            List<CompleteUrl> completeUrls = UrlExtractor.normalizeAndFilter(url, urls);
            logger.debug("[Reading pt-crawl] Found urls: " + completeUrls);
            //Build the string
            StringBuilder builder = new StringBuilder();
            for (CompleteUrl completeUrl : completeUrls) {
                builder.append(Hasher.hash(completeUrl.toString()));
                builder.append(",");
            }
            if (!builder.isEmpty()) {
                builder.deleteCharAt(builder.length() - 1);
            }
            String tmp = "1.0,1.0," + builder;
            return new FlamePair(Hasher.hash(urlPage[0]), tmp);
        });
        logger.debug("Preprocessing completed");
        curRDD.saveAsTable("state");

        int round = 0;
        FlamePairRDD resultRdd = null;
        double maxDiff = Double.MAX_VALUE;
        while (maxDiff > threshold) {
            System.out.println("Round: " + round++);
            //Start pagerank computation
            FlamePairRDD transferRDD = curRDD.flatMapToPair(s -> {
                String url = s._1();
                String[] tmp = s._2().split(",");
                double prevPageRank = Double.parseDouble(tmp[0]);
                List<String> outLinks = List.of(tmp).subList(2, tmp.length);
                List<FlamePair> pairs = new ArrayList<>();
                System.out.println("Calculating pagerank for " + url);
                double v = 0.85 * prevPageRank / outLinks.size();
                for (String outLink : outLinks) {
                    pairs.add(new FlamePair(outLink, v + ""));
                    System.out.println("outlink: " + outLink + " pagerank: " + v);
                }
                pairs.add(new FlamePair(url, "0.0"));
                return pairs;
            }).foldByKey("0.0", (s, v) -> (Double.parseDouble(s) + Double.parseDouble(v)) + "");
//            transferRDD.saveAsTable("transfer" + round);

            FlamePairRDD joinRDD = curRDD.join(transferRDD);
//            joinRDD.saveAsTable("join" + round);
            FlamePairRDD newStateRDD = joinRDD.flatMapToPair(p -> {
                String url = p._1();
                String[] tmp = p._2().split(",");
                double prevPageRank = Double.parseDouble(tmp[0]);
                double currentPageRank = Double.parseDouble(tmp[tmp.length - 1]) + 0.15;
                List<String> outLinks = List.of(tmp).subList(2, tmp.length - 1);

                StringBuilder builder = new StringBuilder();
                for (String outLink : outLinks) {
                    builder.append(outLink);
                    builder.append(",");
                }
                if (!builder.isEmpty()) {
                    builder.deleteCharAt(builder.length() - 1);
                }
                String tmp2 = currentPageRank + "," + prevPageRank + (builder.isEmpty() ? "" : ",") + builder;
                return List.of(new FlamePair(url, tmp2));
            });
            curRDD.destroy();
            curRDD = newStateRDD;
            newStateRDD.saveAsTable("state");

            maxDiff = newStateRDD.flatMap(p -> {
                String[] tmp = p._2().split(",");
                double prevPageRank = Double.parseDouble(tmp[1]);
                double currentPageRank = Double.parseDouble(tmp[0]);
                return List.of(Math.abs(currentPageRank - prevPageRank) + "");
            }).collect().stream().map(Double::parseDouble).max(Double::compareTo).orElse(Double.MAX_VALUE);
            if(maxDiff < threshold) {
                newStateRDD.saveAsTable("pagerank");
                resultRdd = newStateRDD;
                logger.debug("Completed");
                break;
            }
            logger.debug("Max diff: " + maxDiff);
        }

        if(resultRdd == null) {
            resultRdd = curRDD;
            logger.warn("Result is null after all computation");
        }
        resultRdd.flatMapToPair(p -> {
            logger.debug("Saving pagerank for " + p._1());
            String[] tmp = p._2().split(",");
            double rank = Double.parseDouble(tmp[0]);
            context.getKVS().put("pt-pageranks", p._1(), "rank", rank + "");
            return new ArrayList<>();
        });
    }

    public static void main(String[] args) {
        System.out.println("PageRank");
    }

}
