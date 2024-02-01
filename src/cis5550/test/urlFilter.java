package cis5550.test;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.io.IOException;
import java.util.Iterator;

public class urlFilter {
    public static void main(String[] args) throws IOException {
        KVSClient client = new KVSClient("127.0.0.1:8000");
        Iterator<Row> iterator = null;

        try {
            iterator = client.scan("pt-crawl");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        while (iterator.hasNext()) {
            Row row = iterator.next();
            if(!row.get("responseCode").equals("200")) {
                continue;
            }
            client.putRow("pt-crawl-filtered", row);
        }

        client.delete("pt-crawl");
        client.rename("pt-crawl-filtered", "pt-crawl");
    }
}
