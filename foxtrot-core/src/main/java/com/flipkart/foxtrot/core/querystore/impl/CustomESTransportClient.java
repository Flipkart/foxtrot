package com.flipkart.foxtrot.core.querystore.impl;

import com.google.common.collect.ImmutableList;
import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.Netty4Plugin;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CustomESTransportClient extends TransportClient
{


    static {
        // initialize Netty system properties before triggering any Netty class loads
        initializeNetty();
    }

    /**
     * Netty wants to do some unwelcome things like use unsafe and replace a private field, or use a poorly considered buffer recycler. This
     * method disables these things by default, but can be overridden by setting the corresponding system properties.
     */
    private static void initializeNetty() {
        /*
         * We disable three pieces of Netty functionality here:
         *  - we disable Netty from being unsafe
         *  - we disable Netty from replacing the selector key set
         *  - we disable Netty from using the recycler
         *
         * While permissions are needed to read and set these, the permissions needed here are innocuous and thus should simply be granted
         * rather than us handling a security exception here.
         */
        setSystemPropertyIfUnset("io.netty.noUnsafe", Boolean.toString(true));
        setSystemPropertyIfUnset("io.netty.noKeySetOptimization", Boolean.toString(true));
        setSystemPropertyIfUnset("io.netty.recycler.maxCapacityPerThread", Integer.toString(0));
    }

    @SuppressForbidden(reason = "set system properties to configure Netty")
    private static void setSystemPropertyIfUnset(final String key, final String value) {
        final String currentValue = System.getProperty(key);
        if (currentValue == null) {
            System.setProperty(key, value);
        }
    }

    public CustomESTransportClient(Settings settings) {
        super(settings, ImmutableList.of(Netty4Plugin.class));
    }

    @Override
    public void close() {
        super.close();
        if (!NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings)
                || NetworkModule.TRANSPORT_TYPE_SETTING.get(settings).equals(Netty4Plugin.NETTY_TRANSPORT_NAME)) {
            try {
                GlobalEventExecutor.INSTANCE.awaitInactivity(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                ThreadDeathWatcher.awaitInactivity(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
