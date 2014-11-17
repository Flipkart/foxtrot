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
import java.util.List;
import java.util.Map;

@Provider
@Produces(MediaType.TEXT_PLAIN)
public class FlatResponseTextProvider implements MessageBodyWriter<FlatRepresentation> {
    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type == FlatRepresentation.class && mediaType.toString().equals(MediaType.TEXT_PLAIN);
    }

    @Override
    public long getSize(FlatRepresentation response, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(FlatRepresentation response, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        if(null == response) {
            entityStream.write("No records found matching the specified criterion".getBytes());
            return;
        }
        List<FieldHeader> headers = response.getHeaders();
        StringBuilder data = new StringBuilder();
        StringBuilder headerLineBuilder = new StringBuilder();
        headerLineBuilder.append("|");
        for(FieldHeader fieldHeader : headers) {
            final String name = fieldHeader.getName();
            if(name.length() > fieldHeader.getMaxLength()) {
                fieldHeader.setMaxLength(name.length());
            }
            headerLineBuilder.append(" ");
            headerLineBuilder.append(String.format("%" + fieldHeader.getMaxLength() + "s", fieldHeader.getName()));
            headerLineBuilder.append(" |");
        }
        headerLineBuilder.append("\n");
        final String headerLine = headerLineBuilder.toString();
        hrLine(headerLine.length(), data);
        data.append(headerLine);
        hrLine(headerLine.length(), data);
        List<Map<String, Object>> rows = response.getRows();
        for(Map<String, Object> row : rows) {
            StringBuilder rowBuilder = new StringBuilder();
            rowBuilder.append("|");
            for(FieldHeader fieldHeader : headers) {
                rowBuilder.append(" ");
                rowBuilder.append(String.format("%" + fieldHeader.getMaxLength() + "s", row.get(fieldHeader.getName())));
                rowBuilder.append(" |");
            }
            rowBuilder.append("\n");
            data.append(rowBuilder.toString().replaceAll("\"", " ").replaceAll("null", "    "));
        }
        hrLine(headerLine.length(), data);
        entityStream.write(data.toString().getBytes());
    }

    public void hrLine(int length, StringBuilder stringBuilder) {
        char[] chars = new char[length - 3];
        Arrays.fill(chars, '-');
        stringBuilder.append("+").append(new String(chars)).append("+\n");
    }
}
