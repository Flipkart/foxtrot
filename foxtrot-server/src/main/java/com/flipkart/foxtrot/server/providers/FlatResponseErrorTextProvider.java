package com.flipkart.foxtrot.server.providers;

import com.flipkart.foxtrot.sql.responseprocessors.model.FieldHeader;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Provider
@Produces(MediaType.TEXT_PLAIN)
public class FlatResponseErrorTextProvider implements MessageBodyWriter<Map> {
    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type == Map.class && mediaType.toString().equals(MediaType.TEXT_PLAIN);
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
            data.append(map.get(key).toString());
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
