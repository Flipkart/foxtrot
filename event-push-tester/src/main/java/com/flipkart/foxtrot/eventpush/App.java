package com.flipkart.foxtrot.eventpush;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.net.Inet4Address;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("t", "threads", true, "Number of threads to use to push data");
        options.addOption("n", "num-messages", true, "Number of messages to send per thread");
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
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CompletionService<Void> completionService = new ExecutorCompletionService<Void>(executorService);
        ObjectMapper objectMapper = new ObjectMapper();
        String hostname = Inet4Address.getLocalHost().getCanonicalHostName();
        for(int i = 1; i <= threads; i++) {
            completionService.submit(new MessageSender(eventCount, objectMapper, i, hostname, httpClient));
        }
        for(int i = 1; i <= threads; i++) {
            completionService.take().get();
        }
    }
}
