package com.flipkart.foxtrot.server;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:25 PM
 */
public class App {
    public static void main(String[] args) throws Exception {
        FoxtrotServer foxtrotServer = new FoxtrotServer();
        foxtrotServer.run(args);
    }
}
