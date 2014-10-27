package com.flipkart.foxtrot.sql.responseprocessors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.ResponseVisitor;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.QueryResponse;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendValue;
import com.flipkart.foxtrot.common.stats.StatsValue;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.common.trend.TrendResponse;
import com.flipkart.foxtrot.sql.responseprocessors.model.FieldHeader;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import com.flipkart.foxtrot.sql.responseprocessors.model.MetaData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.PrintStream;
import java.util.*;

import static com.flipkart.foxtrot.sql.responseprocessors.FlatteningUtils.generateFieldMappings;
import static com.flipkart.foxtrot.sql.responseprocessors.FlatteningUtils.genericParse;

public class Flattener implements ResponseVisitor {
    private FlatRepresentation flatRepresentation;
    private ObjectMapper objectMapper;
    private ActionRequest request;
    private final List<String> fieldsToReturn;


    public Flattener(ObjectMapper objectMapper, ActionRequest request, List<String> fieldsToReturn) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.fieldsToReturn = fieldsToReturn;
    }

    @Override
    public void visit(GroupResponse groupResponse) {
        Map<String, Integer> fieldNames = Maps.newTreeMap();
        Map<String, MetaData> dataFields = generateFieldMappings(null, objectMapper.valueToTree(groupResponse.getResult()));
        GroupRequest groupRequest = (GroupRequest) request;
        List<Map<String, Object>> rows = Lists.newArrayList();
        for(Map.Entry<String, MetaData> groupData : dataFields.entrySet()) {
            String values[] = groupData.getKey().split("\\.");
            Map<String, Object> row = Maps.newHashMap();
            for(int i = 0; i < groupRequest.getNesting().size(); i++ ) {
                final String fieldName = groupRequest.getNesting().get(i);
                row.put(fieldName, values[i]);
                if(!fieldNames.containsKey(fieldName)) {
                    fieldNames.put(fieldName, 0);
                }
                fieldNames.put(fieldName, lengthMax(fieldNames.get(fieldName), values[i]));
            }
            row.put("count", groupData.getValue().getData());
            rows.add(row);
        }
        fieldNames.put("count", 10);
        List<FieldHeader> headers = Lists.newArrayList();
        for(String fieldName : groupRequest.getNesting()) {
            headers.add(new FieldHeader(fieldName, fieldNames.get(fieldName)));
        }
        headers.add(new FieldHeader("count", 10));
        flatRepresentation = new FlatRepresentation(headers, rows);
    }

    @Override
    public void visit(HistogramResponse histogramResponse) {
        //TODO
    }

    @Override
    public void visit(QueryResponse queryResponse) {
        Map<String, Integer> fieldNames = Maps.newTreeMap();
        List<Map<String, Object>> rows = Lists.newArrayList();
        Set<String> fieldToLookup = (null == fieldsToReturn) ? Collections.<String>emptySet() : new HashSet<String>(fieldsToReturn);
        boolean isAllFields = fieldToLookup.isEmpty();
        for(Document document : queryResponse.getDocuments()) {
            Map<String, MetaData> docFields = generateFieldMappings(null, objectMapper.valueToTree(document));
            Map<String, Object> row = Maps.newTreeMap();
            for(Map.Entry<String, MetaData> docField : docFields.entrySet()) {
                String fieldName = docField.getKey();
                String prettyFieldName = fieldName.replaceFirst("data.", "");
                if(!isAllFields && !fieldToLookup.contains(prettyFieldName)) {
                    continue;
                }
                row.put(prettyFieldName, docField.getValue().getData());
                if(!fieldNames.containsKey(prettyFieldName)) {
                    fieldNames.put(prettyFieldName, 0);
                }
                fieldNames.put(prettyFieldName,
                        Math.max(fieldNames.get(prettyFieldName), docField.getValue().getLength()));
            }
            rows.add(row);
        }
        if(null != rows && !rows.isEmpty()) {
            flatRepresentation = new FlatRepresentation(getFieldsFromList(fieldNames), rows);
        }
    }

    @Override
    public void visit(StatsResponse statsResponse) {
        flatRepresentation = genericParse(objectMapper.valueToTree(statsResponse.getResult()));
    }


    @Override
    public void visit(StatsTrendResponse statsTrendResponse) {
        Set<FieldHeader> headers = Sets.newHashSet();
        List<Map<String, Object>> rows = Lists.newArrayList();
        for(StatsTrendValue statsTrendValue : statsTrendResponse.getResult()) {
            FlatRepresentation tmpFlatR = genericParse(objectMapper.valueToTree(statsTrendValue));
            headers.addAll(tmpFlatR.getHeaders());
            rows.add(tmpFlatR.getRows().get(0));
        }
        flatRepresentation = new FlatRepresentation(new ArrayList<FieldHeader>(headers), rows);
    }

    @Override
    public void visit(TrendResponse trendResponse) {
        List<FieldHeader> headers = Lists.newArrayListWithCapacity(3);
        JsonNode root = objectMapper.valueToTree(trendResponse.getTrends());
        if(null == root || !root.isObject()) {
            return;
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
                final String time = Long.toString(dataNode.get("period").asLong());
                if (!representation.containsKey(time)) {
                    representation.put(time, Maps.<String, Object>newHashMap());
                }
                representation.get(time).put(typeName, dataNode.get("count").asLong());
            }
        }

        headers.add(new FieldHeader("time", 20));
        for(String type : types) {
            headers.add(new FieldHeader(type, 20));
        }
        for (Map.Entry<String, Map<String, Object>> element : representation.entrySet()) {
            Map<String, Object> row = Maps.newTreeMap();
            row.put("time", element.getKey());
            for(Map.Entry<String, Object> data : element.getValue().entrySet()) {
                row.put(data.getKey(), data.getValue());
            }
            rows.add(row);
        }
        flatRepresentation = new FlatRepresentation(new ArrayList<FieldHeader>(headers), rows);
    }

    public FlatRepresentation getFlatRepresentation() {
        return flatRepresentation;
    }

    private int lengthMax(int currMax, final String rhs) {
        return currMax > rhs.length() ? currMax : rhs.length();
    }

    private int lengthMax(final String lhs, final String rhs) {
        return lhs.length() > rhs.length() ? lhs.length() : rhs.length();
    }

    private List<FieldHeader> getFieldsFromList(Map<String, Integer> fieldNames) {
        List<FieldHeader> headers = Lists.newArrayList();
        if( null == fieldsToReturn || fieldsToReturn.isEmpty()) {
            for (String fieldName : fieldNames.keySet()) {
                headers.add(new FieldHeader(fieldName, fieldNames.get(fieldName)));
            }
        }
        else {
            for(String fieldName : fieldsToReturn) {
                headers.add(new FieldHeader(fieldName, fieldNames.get(fieldName)));
            }
        }
        return headers;
    }


    public static String hrLine(int length) {
        char[] chars = new char[length - 4];
        Arrays.fill(chars, '-');
        return "+" + new String(chars) + "+\r\n";
    }

    public static void main(String[] args) throws Exception {
        String jsonRequest = "{\n" +
                "    \"opcode\": \"TrendRequest\",\n" +
                "    \"table\": \"europa\",\n" +
                "    \"filters\": [\n" +
                "        {\n" +
                "            \"field\": \"_timestamp\",\n" +
                "            \"operator\": \"between\",\n" +
                "            \"temporal\": true,\n" +
                "            \"from\": 1412562600000,\n" +
                "            \"to\": 1412584200000\n" +
                "        },\n" +
                "      {\n" +
                "          \"field\": \"header.configName\",\n" +
                "          \"operator\": \"in\",\n" +
                "          \"values\": [\"CHECKOUT_INIT\", \"CHECKOUT_COMPLETE\"]\n" +
                "      }      \n" +
                "    ],\n" +
                "    \"field\": \"header.configName\",\n" +
                "    \"period\": \"hours\"\n" +
                "}";

        String json = "{\"opcode\":\"TrendResponse\",\"trends\":{\"CHECKOUT_COMPLETE\":[{\"period\":1412560800000,\"count\":136271},{\"period\":1412564400000,\"count\":258107},{\"period\":1412568000000,\"count\":165422},{\"period\":1412571600000,\"count\":203263},{\"period\":1412575200000,\"count\":181267},{\"period\":1412578800000,\"count\":165814},{\"period\":1412582400000,\"count\":68814}],\"CHECKOUT_INIT\":[{\"period\":1412560800000,\"count\":687813},{\"period\":1412564400000,\"count\":1275330},{\"period\":1412568000000,\"count\":1686986},{\"period\":1412571600000,\"count\":1127714},{\"period\":1412575200000,\"count\":1057421},{\"period\":1412578800000,\"count\":949818},{\"period\":1412582400000,\"count\":420226}]}}";

        ObjectMapper mapper = new ObjectMapper();
        TrendRequest trendRequest = mapper.readValue(jsonRequest, TrendRequest.class);
        TrendResponse trendResponse = mapper.readValue(json, TrendResponse.class);

        Flattener flattener = new Flattener(mapper, trendRequest, null);
        trendResponse.accept(flattener);
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(flattener.getFlatRepresentation()));
    }
}
