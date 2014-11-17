package com.flipkart.foxtrot.server.providers;

import au.com.bytecode.opencsv.CSVWriter;
import com.flipkart.foxtrot.sql.responseprocessors.model.FieldHeader;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Provider
@Produces(FoxtrotExtraMediaType.TEXT_CSV)
public class FlatResponseCsvProvider implements MessageBodyWriter<FlatRepresentation> {

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type == FlatRepresentation.class && mediaType.toString().equals(FoxtrotExtraMediaType.TEXT_CSV);
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
        StringWriter dataBuffer = new StringWriter();
        CSVWriter data = new CSVWriter(new BufferedWriter(dataBuffer));
        String headerNames[] = new String[headers.size()];
        int i = 0;
        for(FieldHeader fieldHeader : headers) {
            headerNames[i++] = fieldHeader.getName();
        }
        data.writeNext(headerNames);

        List<Map<String, Object>> rows = response.getRows();
        for(Map<String, Object> row : rows) {
            String rowData[] = new String[row.size()];
            i = 0;
            for(FieldHeader fieldHeader : headers) {
                rowData[i++] = row.get(fieldHeader.getName()).toString().replaceAll("\"", "").replaceAll("null", "");
            }

            data.writeNext(rowData);
        }
        data.close();
        entityStream.write(dataBuffer.toString().getBytes());
    }


}
