package com.flipkart.foxtrot.server.providers;

import au.com.bytecode.opencsv.CSVWriter;
import com.flipkart.foxtrot.sql.responseprocessors.model.FieldHeader;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;

public class FlatToCsvConverter {
    public static void convert(final FlatRepresentation representation, Writer writer) throws IOException {
        CSVWriter data = new CSVWriter(writer);
        List<FieldHeader> headers = representation.getHeaders();
        String headerNames[] = new String[headers.size()];
        int i = 0;
        for(FieldHeader fieldHeader : headers) {
            headerNames[i++] = fieldHeader.getName();
        }
        data.writeNext(headerNames);

        List<Map<String, Object>> rows = representation.getRows();
        for(Map<String, Object> row : rows) {
            String rowData[] = new String[row.size()];
            i = 0;
            for(FieldHeader fieldHeader : headers) {
                rowData[i++] = row.get(fieldHeader.getName()).toString().replaceAll("\"", "").replaceAll("null", "");
            }

            data.writeNext(rowData);
        }
        data.close();
    }
}
