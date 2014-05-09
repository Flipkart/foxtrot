package com.flipkart.foxtrot.eventpush;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import org.apache.commons.cli.*;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.net.Inet4Address;
import java.util.concurrent.*;

public class App {
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("t", "threads", true, "Number of threads to use to push data");
        options.addOption("n", "num-messages", true, "Number of messages to send per thread");
        options.addOption("h", "host", true, "Foxtrot Server Host");
        options.addOption("p", "port", true, "Foxtrot Server Port");
        options.addOption("bcount", "batch-count", true, "Number of batches per thread");
        options.addOption("bsize", "bulk-size", true, "Number of events per batch");

        CommandLineParser commandLineParser = new GnuParser();
        CommandLine commandLine = null;
        try {
             commandLine = commandLineParser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error parsing command line: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "event-push-tester", options );
            return;
        }
        if(null == commandLine) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "event-push-tester", options );
            return;
        }
        int threads = Integer.parseInt(commandLine.getOptionValue("t", "1"));
        int eventCount = Integer.parseInt(commandLine.getOptionValue("n", "100"));
        String host = commandLine.getOptionValue("h", "stage-hyperion-api.digital.ch.flipkart.com");
        int port = Integer.parseInt(commandLine.getOptionValue("p", "17000"));
        int batchSize = Integer.parseInt(commandLine.getOptionValue("bsize", "1"));
        int batchCount = Integer.parseInt(commandLine.getOptionValue("bcount", "1"));

        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CompletionService<Void> completionService = new ExecutorCompletionService<Void>(executorService);
        ObjectMapper objectMapper = new ObjectMapper();
        String hostname = Inet4Address.getLocalHost().getCanonicalHostName();
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        for(int i = 1; i <= threads; i++) {
            if (commandLine.hasOption("bsize")) {
                completionService.submit(new BulkMessageSender(batchCount, batchSize, objectMapper, i, hostname, httpClient, host, port));
            } else {
                completionService.submit(new MessageSender(eventCount, objectMapper, i, hostname, httpClient, host, port));
            }
        }
        for(int i = 1; i <= threads; i++) {
            completionService.take().get();
        }
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
}
