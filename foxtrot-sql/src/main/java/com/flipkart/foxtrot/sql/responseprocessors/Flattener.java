package com.flipkart.foxtrot.sql.responseprocessors;

import static com.flipkart.foxtrot.common.Opcodes.COUNT;
import static com.flipkart.foxtrot.sql.responseprocessors.FlatteningUtils.generateFieldMappings;
import static com.flipkart.foxtrot.sql.responseprocessors.FlatteningUtils.genericParse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.QueryResponse;
import com.flipkart.foxtrot.common.ResponseVisitor;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.distinct.DistinctResponse;
import com.flipkart.foxtrot.common.exception.FqlParsingException;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.MultiQueryResponse;
import com.flipkart.foxtrot.common.query.MultiTimeQueryResponse;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.sql.responseprocessors.model.FieldHeader;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.flipkart.foxtrot.sql.responseprocessors.model.MetaData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Flattener implements ResponseVisitor<FlatRepresentation> {

    private final List<String> fieldsToReturn;
    private ObjectMapper objectMapper;
    private ActionRequest request;


    public Flattener(ObjectMapper objectMapper, ActionRequest request, List<String> fieldsToReturn) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.fieldsToReturn = fieldsToReturn;
    }

    @Override
    public FlatRepresentation visit(GroupResponse groupResponse) {
        final String separator = "__SEPARATOR__";
        Map<String, Integer> fieldNames = Maps.newTreeMap();
        Map<String, MetaData> dataFields = generateFieldMappings(null,
                objectMapper.valueToTree(groupResponse.getResult()), separator);
        GroupRequest groupRequest = (GroupRequest) request;
        List<Map<String, Object>> rows = Lists.newArrayList();
        for (Map.Entry<String, MetaData> groupData : dataFields.entrySet()) {
            String[] values = groupData.getKey()
                    .split(separator);
            Map<String, Object> row = Maps.newHashMap();
            for (int i = 0; i < groupRequest.getNesting()
                    .size(); i++) {
                final String fieldName = groupRequest.getNesting()
                        .get(i);
                row.put(fieldName, values[i]);
                if (!fieldNames.containsKey(fieldName)) {
                    fieldNames.put(fieldName, 0);
                }
                fieldNames.put(fieldName, lengthMax(fieldNames.get(fieldName), values[i]));
            }
            row.put(COUNT, groupData.getValue()
                    .getData());
            rows.add(row);
        }
        fieldNames.put(COUNT, 10);
        List<FieldHeader> headers = Lists.newArrayList();
        for (String fieldName : groupRequest.getNesting()) {
            headers.add(new FieldHeader(fieldName, fieldNames.get(fieldName)));
        }
        headers.add(new FieldHeader(COUNT, 10));
        return new FlatRepresentation("group", headers, rows);
    }

    @Override
    public FlatRepresentation visit(HistogramResponse histogramResponse) {
        List<Map<String, Object>> rows = Lists.newArrayList();
        rows.addAll(histogramResponse.getCounts()
                .stream()
                .map(count -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("timestamp", count.getPeriod());
                    map.put(COUNT, count.getCount());
                    return map;
                })
                .collect(Collectors.toList()));

        List<FieldHeader> headers = Lists.newArrayList();
        headers.add(new FieldHeader("timestamp", 15));
        headers.add(new FieldHeader(COUNT, 15));
        return new FlatRepresentation("histogram", headers, rows);
    }

    @Override
    public FlatRepresentation visit(QueryResponse queryResponse) {
        Map<String, Integer> fieldNames = Maps.newTreeMap();
        List<Map<String, Object>> rows = Lists.newArrayList();
        Set<String> fieldToLookup = (null == fieldsToReturn) ? Collections.emptySet() : new HashSet<>(fieldsToReturn);
        boolean isAllFields = fieldToLookup.isEmpty();
        for (Document document : queryResponse.getDocuments()) {
            Map<String, MetaData> docFields = generateFieldMappings(null, objectMapper.valueToTree(document));
            Map<String, Object> row = Maps.newTreeMap();
            for (Map.Entry<String, MetaData> docField : docFields.entrySet()) {
                String fieldName = docField.getKey();
                String prettyFieldName = fieldName.replaceFirst("data.", "");
                if (!isAllFields && !fieldToLookup.contains(prettyFieldName)) {
                    continue;
                }
                row.put(prettyFieldName, docField.getValue()
                        .getData());
                if (!fieldNames.containsKey(prettyFieldName)) {
                    fieldNames.put(prettyFieldName, 0);
                }
                fieldNames.put(prettyFieldName, Math.max(fieldNames.get(prettyFieldName), docField.getValue()
                        .getLength()));
            }
            rows.add(row);
        }
        if (!rows.isEmpty()) {
            return new FlatRepresentation("query", getFieldsFromList(fieldNames), rows);
        }
        return null;
    }

    @Override
    public FlatRepresentation visit(StatsResponse statsResponse) {
        FlatRepresentation flatRepresentation = genericParse(objectMapper.valueToTree(statsResponse.getResult()));
        List<FieldHeader> headers = Lists.newArrayList();
        headers.add(new FieldHeader("percentiles.1.0", 20));
        headers.add(new FieldHeader("percentiles.5.0", 20));
        headers.add(new FieldHeader("percentiles.25.0", 20));
        headers.add(new FieldHeader("percentiles.50.0", 20));
        headers.add(new FieldHeader("percentiles.75.0", 20));
        headers.add(new FieldHeader("percentiles.95.0", 20));
        headers.add(new FieldHeader("percentiles.99.0", 20));
        headers.add(new FieldHeader("stats.count", 20));
        headers.add(new FieldHeader("stats.avg", 20));
        headers.add(new FieldHeader("stats.max", 20));
        headers.add(new FieldHeader("stats.min", 20));
        headers.add(new FieldHeader("stats.sum", 20));
        headers.add(new FieldHeader("stats.sum_of_squares", 20));
        headers.add(new FieldHeader("stats.variance", 20));
        headers.add(new FieldHeader("stats.std_deviation", 20));
        flatRepresentation.setHeaders(headers);
        flatRepresentation.setOpcode("stats");
        return flatRepresentation;
    }


    @Override
    public FlatRepresentation visit(StatsTrendResponse statsTrendResponse) {
        List<Map<String, Object>> rows = Lists.newArrayList();
        List<FieldHeader> fieldHeaders = Lists.newArrayList();
        fieldHeaders.add(new FieldHeader("period", 20));
        fieldHeaders.add(new FieldHeader("percentiles.1.0", 20));
        fieldHeaders.add(new FieldHeader("percentiles.5.0", 20));
        fieldHeaders.add(new FieldHeader("percentiles.25.0", 20));
        fieldHeaders.add(new FieldHeader("percentiles.50.0", 20));
        fieldHeaders.add(new FieldHeader("percentiles.75.0", 20));
        fieldHeaders.add(new FieldHeader("percentiles.95.0", 20));
        fieldHeaders.add(new FieldHeader("percentiles.99.0", 20));
        fieldHeaders.add(new FieldHeader("stats.count", 20));
        fieldHeaders.add(new FieldHeader("stats.avg", 20));
        fieldHeaders.add(new FieldHeader("stats.max", 20));
        fieldHeaders.add(new FieldHeader("stats.min", 20));
        fieldHeaders.add(new FieldHeader("stats.sum", 20));
        fieldHeaders.add(new FieldHeader("stats.sum_of_squares", 20));
        fieldHeaders.add(new FieldHeader("stats.variance", 20));
        fieldHeaders.add(new FieldHeader("stats.std_deviation", 20));
        return new FlatRepresentation("statstrend", fieldHeaders, rows);
    }

    @Override
    public FlatRepresentation visit(TrendResponse trendResponse) {
        List<FieldHeader> headers = Lists.newArrayListWithCapacity(3);
        JsonNode root = objectMapper.valueToTree(trendResponse.getTrends());
        if (null == root || !root.isObject()) {
            return null;
        }
        List<String> types = Lists.newArrayList();
        List<Map<String, Object>> rows = Lists.newArrayList();
        Iterator<String> typeNameIt = root.fieldNames();
        Map<String, Map<String, Object>> representation = Maps.newTreeMap();
        int typeNameMaxLength = 0;
        while (typeNameIt.hasNext()) {
            String typeName = typeNameIt.next();
            types.add(typeName);
            typeNameMaxLength = Math.max(typeNameMaxLength, typeName.length());
            for (JsonNode dataNode : root.get(typeName)) {
                final String time = Long.toString(dataNode.get("period")
                        .asLong());
                if (!representation.containsKey(time)) {
                    representation.put(time, Maps.newHashMap());
                }
                representation.get(time)
                        .put(typeName, dataNode.get(COUNT)
                                .asLong());
            }
        }

        headers.add(new FieldHeader("time", 20));
        for (String type : types) {
            headers.add(new FieldHeader(type, 20));
        }
        for (Map.Entry<String, Map<String, Object>> element : representation.entrySet()) {
            Map<String, Object> row = Maps.newTreeMap();
            row.put("time", element.getKey());
            for (Map.Entry<String, Object> data : element.getValue()
                    .entrySet()) {
                row.put(data.getKey(), data.getValue());
            }
            rows.add(row);
        }
        return new FlatRepresentation("trend", new ArrayList<>(headers), rows);
    }

    @Override
    public FlatRepresentation visit(CountResponse countResponse) {
        FieldHeader fieldHeader = new FieldHeader(COUNT, 20);
        List<Map<String, Object>> rows = Lists.newArrayList();
        rows.add(Collections.singletonMap(COUNT, countResponse.getCount()));
        return new FlatRepresentation(COUNT, Collections.singletonList(fieldHeader), rows);
    }

    @Override
    public FlatRepresentation visit(DistinctResponse distinctResponse) {
        List<FieldHeader> fieldHeaders = Lists.newArrayList();
        for (String header : distinctResponse.getHeaders()) {
            fieldHeaders.add(new FieldHeader(header, 10));
        }
        List<List<String>> distinctResponseRows = distinctResponse.getResult();
        List<Map<String, Object>> rows = Lists.newArrayList();
        for (List<String> responseRow : distinctResponseRows) {
            Map<String, Object> row = Maps.newHashMap();
            for (int i = 0; i < fieldHeaders.size(); i++) {
                row.put(fieldHeaders.get(i)
                        .getName(), responseRow.get(i));
            }
            rows.add(row);
        }
        return new FlatRepresentation("distinct", fieldHeaders, rows);
    }

    @Override
    public FlatRepresentation visit(MultiQueryResponse multiQueryResponse) {
        throw new FqlParsingException("Fql query not supported for this operation");
    }

    @Override
    public FlatRepresentation visit(MultiTimeQueryResponse multiTimeQueryResponse) {
        throw new FqlParsingException("Fql query not supported for this operation");
    }

    private int lengthMax(int currMax, final String rhs) {
        return currMax > rhs.length() ? currMax : rhs.length();
    }

    private List<FieldHeader> getFieldsFromList(Map<String, Integer> fieldNames) {
        List<FieldHeader> headers = Lists.newArrayList();
        if (null == fieldsToReturn || fieldsToReturn.isEmpty()) {
            for (Map.Entry<String, Integer> entry : fieldNames.entrySet()) {
                headers.add(new FieldHeader(entry.getKey(), entry.getValue()));
            }
        } else {
            for (String fieldName : fieldsToReturn) {
                headers.add(new FieldHeader(fieldName, fieldNames.get(fieldName)));
            }
        }
        return headers;
    }

}
