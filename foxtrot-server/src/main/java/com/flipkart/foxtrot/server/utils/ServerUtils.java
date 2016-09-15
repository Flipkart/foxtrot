package com.flipkart.foxtrot.server.utils;

import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;

/**
 * Created by santanu on 31/5/16.
 */
public class ServerUtils {
    public static int port(ServerFactory serverFactory) {
        if( serverFactory instanceof SimpleServerFactory) {
            SimpleServerFactory simpleServerFactory = (SimpleServerFactory)serverFactory;
            return getPortFromConnector(simpleServerFactory.getConnector());

        }
        if( serverFactory instanceof DefaultServerFactory) {
            DefaultServerFactory defaultServerFactory = (DefaultServerFactory)serverFactory;
            for(ConnectorFactory connectorFactory : defaultServerFactory.getApplicationConnectors()) {
                if(connectorFactory instanceof HttpConnectorFactory) {
                    return getPortFromConnector(connectorFactory);
                }
            }

        }
        throw new RuntimeException("Cannot extract port from connector");
    }

    private static int getPortFromConnector(ConnectorFactory connectorFactory) {
        if(connectorFactory instanceof HttpConnectorFactory) {
            return ((HttpConnectorFactory)connectorFactory).getPort();
        }
        throw new RuntimeException("Cannot extract port from connector");
    }
}
