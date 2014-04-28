package com.flipkart.foxtrot.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.DataStoreException;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.actions.spi.ActionMetadata;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.mockito.Mockito;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class TestUtils {

    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class.getSimpleName());

    public static DataStore getDataStore() throws DataStoreException {
        HTableInterface tableInterface = MockHTable.create();
        HbaseTableConnection tableConnection = Mockito.mock(HbaseTableConnection.class);
        when(tableConnection.getTable()).thenReturn(tableInterface);
        return new HbaseDataStore(tableConnection, new ObjectMapper());
    }

    public static Document getDocument(String id, long timestamp, Object[] args, ObjectMapper mapper){
        Map<String, Object> data = new HashMap<String, Object>();
        for ( int i = 0; i < args.length; i+= 2){
            data.put((String) args[i], args[i+1]);
        }
        return new Document(id, timestamp, mapper.valueToTree(data));
    }

    public static void registerActions(AnalyticsLoader analyticsLoader, ObjectMapper mapper) throws Exception {
        Reflections reflections = new Reflections("com.flipkart.foxtrot", new SubTypesScanner());
        Set<Class<? extends Action>> actions = reflections.getSubTypesOf(Action.class);
        if(actions.isEmpty()) {
            throw new Exception("No analytics actions found!!");
        }
        List<NamedType> types = new Vector<NamedType>();
        for(Class<? extends Action> action : actions) {
            AnalyticsProvider analyticsProvider = action.getAnnotation(AnalyticsProvider.class);
            if(null == analyticsProvider.request()
                    || null == analyticsProvider.opcode()
                    || analyticsProvider.opcode().isEmpty()
                    || null == analyticsProvider.response()) {
                throw new Exception("Invalid annotation on " + action.getCanonicalName());
            }
            if(analyticsProvider.opcode().equalsIgnoreCase("default")) {
                logger.warn("Action " + action.getCanonicalName() + " does not specify cache token. " +
                        "Using default cache.");
            }
            analyticsLoader.register(new ActionMetadata(
                    analyticsProvider.request(), action,
                    analyticsProvider.cacheable(), analyticsProvider.opcode()));
            types.add(new NamedType(analyticsProvider.request(), analyticsProvider.opcode()));
            types.add(new NamedType(analyticsProvider.response(), analyticsProvider.opcode()));
            logger.info("Registered action: " + action.getCanonicalName());
        }
        mapper.getSubtypeResolver().registerSubtypes(types.toArray(new NamedType[types.size()]));
    }
}
