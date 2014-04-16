package com.flipkart.foxtrot.eventpush;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import javax.ws.rs.core.MediaType;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.Callable;

public class MessageSender implements Callable<Void> {
    private int numEvents;
    private ObjectMapper objectMapper;
    private int threadID;
    private String hostName;
    private CloseableHttpClient httpClient;
    private HttpClientContext httpContext;


    public MessageSender(int numEvents,
                         ObjectMapper objectMapper,
                         int threadID,
                         String hostName,
                         CloseableHttpClient httpClient) {
        this.numEvents = numEvents;
        this.objectMapper = objectMapper;
        this.threadID = threadID;
        this.hostName = hostName;
        this.httpClient = httpClient;
        this.httpContext = HttpClientContext.create();
    }

    @Override
    public Void call() throws Exception {
        for (int i = 1 ; i <= numEvents; i++) {
            Document document = new Document();
            ObjectNode rootNode = objectMapper.createObjectNode();
            rootNode.put("value", "Santanu Sinha");
            rootNode.put("threadID", threadID);
            rootNode.put("hostName", hostName);
            rootNode.put("count", i);
            rootNode.put("timestamp", System.currentTimeMillis());
            document.setId(UUID.randomUUID().toString());
            document.setTimestamp(System.currentTimeMillis());
            document.setData(rootNode);
            System.out.println("Entity: " + objectMapper.writeValueAsString(document));
            HttpPost httpPost = new HttpPost("http://stage-ingester.digital.ch.flipkart.com:17000/foxtrot/v1/document/test");
            httpPost.setHeader(new BasicHeader("Content-Type", MediaType.APPLICATION_JSON));
            httpPost.setEntity(new StringEntity(objectMapper.writeValueAsString(document), Charset.defaultCharset()));
            CloseableHttpResponse entity = httpClient.execute(httpPost, httpContext);
            try {
                if(entity.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    System.out.println(EntityUtils.toString(entity.getEntity()));
                }
                else {
                    System.err.println(entity.getStatusLine().getStatusCode() + ":" + entity.getStatusLine().getReasonPhrase());
                }

            } finally {
                entity.close();
            }
        }
        return null;
    }
}
