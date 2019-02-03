package com.flipkart.foxtrot.server.providers;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;

@Provider
@Produces({MediaType.TEXT_PLAIN, FoxtrotExtraMediaType.TEXT_CSV})
public class FlatResponseErrorTextProvider implements MessageBodyWriter<Map> {
    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Map.class.isAssignableFrom(type)
                && (mediaType.toString().equals(MediaType.TEXT_PLAIN)
                        || mediaType.toString().equals(FoxtrotExtraMediaType.TEXT_CSV));
    }

    @Override
    public long getSize(Map map, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(Map map, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        StringBuilder data = new StringBuilder();
        for(Object key : map.keySet()) {
            data.append(key.toString());
            data.append(":");
            if(null == map.get(key.toString())) {
                data.append("Check logs for more details");
            }
            else {
                data.append(map.get(key.toString()).toString());
            }
            data.append("\n");
        }
        entityStream.write(data.toString().getBytes());
    }

    public void hrLine(int length, StringBuilder stringBuilder) {
        char[] chars = new char[length - 3];
        Arrays.fill(chars, '-');
        stringBuilder.append("+").append(new String(chars)).append("+\n");
    }
}
