package cis5550.model;

import cis5550.tools.Logger;

public class PageRankContext {
    private static final Logger logger = Logger.getLogger(PageRankContext.class);
    private static int networkSize = 0;
    private static boolean initialized = false;

    private static void check() throws RuntimeException {
        if (!initialized) {
            throw new RuntimeException("PageRankContext not initialized");
        }
    }

    public static void initialize(int networkSize) {
        PageRankContext.networkSize = networkSize;
        initialized = true;
    }

    public static int getNetworkSize() throws RuntimeException {
        check();
        return networkSize;
    }

    public boolean getInitialized() {
        return initialized;
    }
}
