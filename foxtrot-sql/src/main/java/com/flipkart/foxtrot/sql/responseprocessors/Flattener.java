package com.flipkart.foxtrot.sql.responseprocessors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.ResponseVisitor;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.histogram.HistogramResponse;
import com.flipkart.foxtrot.common.query.QueryResponse;
import com.flipkart.foxtrot.common.stats.StatsResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendResponse;
import com.flipkart.foxtrot.common.stats.StatsTrendValue;
import com.flipkart.foxtrot.common.stats.StatsValue;
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
        for(Document document : queryResponse.getDocuments()) {
            Map<String, MetaData> docFields = generateFieldMappings(null, objectMapper.valueToTree(document));
            Map<String, Object> row = Maps.newTreeMap();
            for(Map.Entry<String, MetaData> docField : docFields.entrySet()) {
                String fieldName = docField.getKey();
                String prettyFieldName = fieldName.replaceFirst("data.", "");
                //if(!isAllFields && !fieldToLookup.contains(fieldName)) {
                //    continue;
                //}
                row.put(prettyFieldName, docField.getValue().getData());
                if(!fieldNames.containsKey(prettyFieldName)) {
                    fieldNames.put(prettyFieldName, 0);
                }
                fieldNames.put(prettyFieldName,
                        Math.max(fieldNames.get(prettyFieldName), docField.getValue().getLength()));
            }
            rows.add(row);
        }
        flatRepresentation = new FlatRepresentation(getFieldsFromList(fieldNames), rows);

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
        Set<FieldHeader> headers = Sets.newHashSet();
        Map<String, MetaData> docFields = generateFieldMappings(null, objectMapper.valueToTree(trendResponse.getTrends()));

        Map<String, Map<String, Object>> representation = Maps.newTreeMap();
        for (Map.Entry<String, MetaData> docField : docFields.entrySet()) {
            final String key = docField.getKey();
            final String values[] = key.split(".");
            final String type = values[0];
            final String time = values[1];
            if (!representation.containsKey(time)) {
                representation.put(key, Maps.<String, Object>newHashMap());
            }
            representation.get(time).put(type, docField.getValue().getData());
        }
        List<Map<String, Object>> rows = Lists.newArrayList();
        for (Map.Entry<String, Map<String, Object>> element : representation.entrySet()) {
            headers.add(new FieldHeader("time", 20));
            Map<String, Object> row = Maps.newTreeMap();
            row.put("time", element.getKey());
            for(Map.Entry<String, Object> data : element.getValue().entrySet()) {
                row.put(data.getKey(), data.getValue());
                headers.add(new FieldHeader(data.getKey(), 10));
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

    public static void main(String[] args) throws Exception {
        String json = "{\"opcode\":\"QueryResponse\",\"documents\":[{\"id\":\"a815b245de1e68c0b150036d4c53c52f768e744d5d10a0f951fb6457144f5d6283430e1a8dd03ace89ebd35d19e36c57235bf661f91bca0eb1b22900652562af\",\"timestamp\":1413866482627,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"d0189dfe-2b9d-4d7a-bc28-3c5680d11062\",\"timestamp\":1413866482627},\"data\":{\"request\":{\"checkoutId\":\"OD3010721779461376\",\"requestId\":\"R-32001103-9c52-44d8-a343-d970a3920510\",\"clientTraceId\":\"T-R-fd753796-e9ff-4170-95cd-8c245a6e30a7\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"106.77.208.107\",\"clientHostName\":\"w3-web1396:co-callisto-svc13.nm.flipkart.com\"},\"actionType\":{\"type\":\"GET_CART\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866482615,\"endTime\":1413866482627,\"duration\":12}}},{\"id\":\"57df1dd9646d23775fbb8cf72a1e1757a9158ac1e04456b3e393e11dd611993f7d805cb362483dfd70a86fedf0439ba99095083919a8038f2ecbae61adc4def1\",\"timestamp\":1413866482435,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"CHECKOUT_EXECUTED\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"0be35d5f-14fc-4ea0-9db8-f45481f26643\",\"timestamp\":1413866482435},\"data\":{\"request\":{\"checkoutId\":\"OD3010721779461376\",\"requestId\":\"R-5074dfc6-6dfa-40e5-b7d1-563e4355d3d6\",\"clientTraceId\":\"T-R-75961d4a-d87c-4581-bb38-f491499b4347\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"106.77.208.107\",\"clientHostName\":\"w3-web1396:co-callisto-svc13.nm.flipkart.com\"},\"dataFlow\":\"digitalCompletedData\",\"accountId\":\"ACC14107023254836822\",\"email\":\"ramsingh.rajput1979@gmail.com\",\"channel\":{\"salesChannel\":\"MobileSite\",\"createdBy\":\"mobile_site\",\"terminalId\":\"TI141370081579076819236975657197918673077222640811745332907627329287\",\"userAgent\":\"Mozilla/5.0 (Linux; Android 4.4.2; iBall Slide 3G 1035Q-90 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Safari/537.36\",\"userIp\":\"106.77.208.107\",\"sessionId\":\"SI27C0BA47DCE94EAF9EE06438DBD01EC7\"},\"checkoutType\":\"PHYSICAL\",\"salesChannel\":\"MobileSite\"}}},{\"id\":\"a2e90c14dade5cbc4cd117294ea61f7ba6301cd063544d2ba665e2283d6186738defd166a7ad76f8d2535e6c86de34be713113dd04be314b53a9acbe80eeeb06\",\"timestamp\":1413866482315,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"a554db21-5311-4cc0-89cb-6e414535641a\",\"timestamp\":1413866482315},\"data\":{\"request\":{\"checkoutId\":\"OD2010721797294867\",\"requestId\":\"R-253d08c2-0e8c-4e15-97d1-ec8b850d390b\",\"clientTraceId\":\"T-R-9c156045-fd86-4813-874e-ff9980ee47ad\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"27.97.215.42\",\"clientHostName\":\"w3-web1379:co-callisto-svc10.nm.flipkart.com\"},\"actionType\":{\"type\":\"GET_CART\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866482304,\"endTime\":1413866482315,\"duration\":11}}},{\"id\":\"df69976d98ab17cfce8f2cd81c94f756dbde9728aca8c3526698dfbf99a85f1492dce0bdb0ea04646b0f9fb6048fd6777f9f661a221cbae0f61b921f3d54b6d7\",\"timestamp\":1413866482243,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"bb549cae-aba3-42d4-9829-feddec0df3dd\",\"timestamp\":1413866482243},\"data\":{\"request\":{\"checkoutId\":\"OD1010721756265324\",\"requestId\":\"R-d47203a5-299b-4226-b3c5-286019b162ac\",\"clientTraceId\":\"T-R-104fa382-e9e8-4413-9e22-1e03660e8364\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"117.207.101.37\",\"clientHostName\":\"w3-web1443:co-callisto-svc13.nm.flipkart.com\"},\"actionType\":{\"type\":\"MERGE_CART\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866482217,\"endTime\":1413866482243,\"duration\":26}}},{\"id\":\"9000fc5c8381b3734926f349cd126f5c94c176c9033ef4c03a772f8aca4af4750115a1d7c95ce4f4d4e1a8402dd069b5d20ed647f4a7fad83ea18f26572e913b\",\"timestamp\":1413866482103,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"CHECKOUT_EXECUTED\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc10.nm.flipkart.com\",\"eventId\":\"9db483dc-21e5-4a84-aa6a-f3f4ac893156\",\"timestamp\":1413866482103},\"data\":{\"request\":{\"checkoutId\":\"OD1010721762694587\",\"requestId\":\"R-2d84e508-be3b-497f-b982-918ea284f933\",\"clientTraceId\":\"T-R-225dfecc-9c0c-4a16-972b-8d8d0fb26c13\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"113.193.184.26\",\"clientHostName\":\"w3-web1494:co-callisto-svc5.nm.flipkart.com\"},\"dataFlow\":\"digitalCompletedData\",\"accountId\":\"ACC13615444923448791\",\"email\":\"magendiranmagi@yahoo.in\",\"channel\":{\"salesChannel\":\"WEB\",\"createdBy\":\"website\",\"terminalId\":\"TI141386560291578626369627558417376664564481551573210273929761598164\",\"userAgent\":\"Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.49 Safari/537.36\",\"userIp\":\"113.193.184.26\",\"sessionId\":\"SIE85D0BD79CB84BA8BB1A8C3F4C170330\"},\"checkoutType\":\"PHYSICAL\",\"salesChannel\":\"WEB\"}}},{\"id\":\"2133fe7770460ad2a8acb9ea0598a8ed07f7dcec2a6f8c11d5e97ff629265b5822db529d74851f1b3b59ddfd454d89164aad779dee6f54c0c2c0968a08026f14\",\"timestamp\":1413866482087,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"2149817c-0755-44d8-a943-f8739f37f350\",\"timestamp\":1413866482087},\"data\":{\"request\":{\"checkoutId\":\"OD1010721756265324\",\"requestId\":\"R-d47203a5-299b-4226-b3c5-286019b162ac\",\"clientTraceId\":\"T-R-104fa382-e9e8-4413-9e22-1e03660e8364\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"117.207.101.37\",\"clientHostName\":\"w3-web1443:co-callisto-svc13.nm.flipkart.com\"},\"actionType\":{\"type\":\"APPLY_PAYMENT_BASED_OFFERS\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866482086,\"endTime\":1413866482087,\"duration\":1}}},{\"id\":\"1a7c2eeaa98a9cc9de27a29d4923446eb8fec843a916a8a96f7edeeb803e44d80a6a7be95cfca52cf4eec406883621025061f0b444170609cf2e4e581426c1c1\",\"timestamp\":1413866482017,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"CHECKOUT_EXECUTED\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc4.nm.flipkart.com\",\"eventId\":\"e5567add-f025-419c-8c80-7ce9296d7a9f\",\"timestamp\":1413866482017},\"data\":{\"request\":{\"checkoutId\":\"OD3010721752496432\",\"requestId\":\"R-2f6e383f-0373-4f10-8165-aca4ff240afc\",\"clientTraceId\":\"T-R-8cb9a175-dd49-413a-8e4f-de297f56a144\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"43.252.26.177\",\"clientHostName\":\"w3-web1467:co-callisto-svc19.nm.flipkart.com\"},\"dataFlow\":\"digitalCompletedData\",\"accountId\":\"ACC14138054831687946\",\"email\":\"malikaakil007@gmail.com\",\"channel\":{\"salesChannel\":\"WEB\",\"createdBy\":\"website\",\"terminalId\":\"TI140618853968262498017663962585848918824560539392164405030040431035\",\"userAgent\":\"Mozilla/5.0 (Windows NT 5.1; rv:32.0) Gecko/20100101 Firefox/32.0\",\"userIp\":\"43.252.26.177\",\"sessionId\":\"SIFDCB8B3B430948EAA703BEFAA7B13997\"},\"checkoutType\":\"PHYSICAL\",\"salesChannel\":\"WEB\"}}},{\"id\":\"bf8e8ebec227ab1b642e099c3bc2e415dc3b2eb5b5893a74aabdcc7e5da811037749a5d3ff70a96124bd9aa47fe36cd9826ae04f4ce6034cc5bd361302a1026d\",\"timestamp\":1413866481990,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"d107ba93-1ac2-46a0-a395-ca92a24fdaf0\",\"timestamp\":1413866481990},\"data\":{\"request\":{\"checkoutId\":\"OD1010721756265324\",\"requestId\":\"R-e9a8eac9-3706-4b6c-9c2f-c577d1de4c89\",\"clientTraceId\":\"T-R-104fa382-e9e8-4413-9e22-1e03660e8364\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"117.207.101.37\",\"clientHostName\":\"w3-web1443:co-callisto-svc13.nm.flipkart.com\"},\"actionType\":{\"type\":\"INTEGRATION_LOGIC\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866481990,\"endTime\":1413866481990,\"duration\":0}}},{\"id\":\"040d5fefed4098dd386403935781f8707324ada55e905716015bd74de8282fc93d00cec620cc20bbd4f433f5ca88daa1a96066858194b9055c7ccc3ef1ee8915\",\"timestamp\":1413866481961,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"CHECKOUT_INIT\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"947f3d85-01dd-43af-844e-67f9ceae5c08\",\"timestamp\":1413866481961},\"data\":{\"request\":{\"checkoutId\":\"OD3010721796956060\",\"requestId\":\"R-eb8b140a-9c96-4a38-8226-d762d47ce10e\",\"clientTraceId\":\"T-R-8b12ce40-91ed-4ca1-bf3b-8a0c4bc479d3\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"1.39.63.17\",\"clientHostName\":\"w3-web1489:co-callisto-svc10.nm.flipkart.com\"},\"dataFlow\":\"digitalCompletedData\",\"checkoutType\":\"PHYSICAL\",\"salesChannel\":\"AndroidApp\"}}},{\"id\":\"9fd7531d8488056f36d6ce9e161a973612aab76a97aac2f6efbb59cbfccd2740543721fa127575c2f93f1a5c52a3c37eb79eb1cad389822e31da1dd0c7e0c251\",\"timestamp\":1413866481935,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc30.nm.flipkart.com\",\"eventId\":\"43649c65-bd56-4d7e-a121-6a362d1ed47d\",\"timestamp\":1413866481935},\"data\":{\"request\":{\"checkoutId\":\"OD0010721796878488\",\"requestId\":\"R-56abfc50-e237-42c5-9bf1-0dbd0084c92e\",\"clientTraceId\":\"T-R-266e5dce-efbf-4757-918b-d16fa57daca7\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"203.32.5.67\",\"clientHostName\":\"w3-web1498:co-callisto-svc21.nm.flipkart.com\"},\"actionType\":{\"type\":\"CREATE_ORDER\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866481910,\"endTime\":1413866481935,\"duration\":25}}}]}";
        /*String json = "{\n" +
                "    \"opcode\": \"GroupResponse\",\n" +
                "    \"result\": {\n" +
                "        \"CHECKOUT_COMPLETE\": {\n" +
                "            \"PHYSICAL\": 6446798,\n" +
                "            \"FLIPKART_FIRST\": 27348,\n" +
                "            \"NONE\": 359,\n" +
                "            \"EGV\": 9332,\n" +
                "            \"DIGITAL\": 15542\n" +
                "        },\n" +
                "        \"CHECKOUT_INIT\": {\n" +
                "            \"PHYSICAL\": 66517780,\n" +
                "            \"FLIPKART_FIRST\": 127788,\n" +
                "            \"EGV\": 44081,\n" +
                "            \"DIGITAL\": 86646\n" +
                "        }\n" +
                "    }\n" +
                "}";*/
        /*String json = "{\n" +
                "    \"opcode\": \"StatsResponse\",\n" +
                "    \"result\": {\n" +
                "        \"stats\": {\n" +
                "            \"min\": -419,\n" +
                "            \"max\": 23860,\n" +
                "            \"count\": 389725998,\n" +
                "            \"std_deviation\": 105.38667690479743,\n" +
                "            \"sum\": 12679624049,\n" +
                "            \"variance\": 11106.351669036163,\n" +
                "            \"sum_of_squares\": 4740961935807,\n" +
                "            \"avg\": 32.53471442518443\n" +
                "        }" +
                "    }\n" +
                "}";*/
        StatsValue statsValue = new StatsValue();
        Map<Number, Number> percentiles = Maps.newHashMap();
        percentiles.put(99.0, 332.04164711863444);
        percentiles.put(25.0, 5);
        statsValue.setPercentiles(percentiles);
        Map<String, Number> stats = Maps.newHashMap();
        stats.put("min", -419);
        stats.put("max", 23860);
        statsValue.setStats(stats);
        StatsResponse statsResponse = new StatsResponse();
        statsResponse.setResult(statsValue);
        ObjectMapper objectMapper = new ObjectMapper();
        //objectMapper.enable(DeserializationFeature.)
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setNesting(Lists.newArrayList("header.configName", "data.checkoutType"));
        Flattener flattener = new Flattener(objectMapper, groupRequest, Lists.newArrayList("data.accountId", "data.actionType.type"));
        new ObjectMapper().readValue(json, QueryResponse.class).accept(flattener);
        //statsResponse.accept(flattener);
        //System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(flattener.getFlatRepresentation()));
        List<FieldHeader> headers = flattener.getFlatRepresentation().getHeaders();
        PrintStream entityStream = System.out;
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
        headerLineBuilder.append("\r\n");
        final String headerLine = headerLineBuilder.toString();
        entityStream.write(hrLine(headerLine.length()).getBytes());
        entityStream.write(headerLine.getBytes());
        entityStream.write(hrLine(headerLine.length()).getBytes());
        List<Map<String, Object>> rows = flattener.getFlatRepresentation().getRows();
        for(Map<String, Object> row : rows) {
            StringBuilder rowBuilder = new StringBuilder();
            rowBuilder.append("|");
            for(FieldHeader fieldHeader : headers) {
                rowBuilder.append(" ");
                rowBuilder.append(String.format("%" + fieldHeader.getMaxLength() + "s", row.get(fieldHeader.getName())));
                rowBuilder.append(" |");
            }
            rowBuilder.append("\r\n");
            entityStream.write(rowBuilder.toString().replaceAll("\"", " ").replaceAll("null", "    ").getBytes());
        }
        entityStream.write(hrLine(headerLine.length()).getBytes());

    }

    public static String hrLine(int length) {
        char[] chars = new char[length - 4];
        Arrays.fill(chars, '-');
        return "+" + new String(chars) + "+\r\n";
    }
/*
    public static void main(String[] args) throws Exception {
        //String json = "{\"opcode\":\"QueryResponse\",\"documents\":[{\"id\":\"a815b245de1e68c0b150036d4c53c52f768e744d5d10a0f951fb6457144f5d6283430e1a8dd03ace89ebd35d19e36c57235bf661f91bca0eb1b22900652562af\",\"timestamp\":1413866482627,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"d0189dfe-2b9d-4d7a-bc28-3c5680d11062\",\"timestamp\":1413866482627},\"data\":{\"request\":{\"checkoutId\":\"OD3010721779461376\",\"requestId\":\"R-32001103-9c52-44d8-a343-d970a3920510\",\"clientTraceId\":\"T-R-fd753796-e9ff-4170-95cd-8c245a6e30a7\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"106.77.208.107\",\"clientHostName\":\"w3-web1396:co-callisto-svc13.nm.flipkart.com\"},\"actionType\":{\"type\":\"GET_CART\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866482615,\"endTime\":1413866482627,\"duration\":12}}},{\"id\":\"57df1dd9646d23775fbb8cf72a1e1757a9158ac1e04456b3e393e11dd611993f7d805cb362483dfd70a86fedf0439ba99095083919a8038f2ecbae61adc4def1\",\"timestamp\":1413866482435,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"CHECKOUT_EXECUTED\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"0be35d5f-14fc-4ea0-9db8-f45481f26643\",\"timestamp\":1413866482435},\"data\":{\"request\":{\"checkoutId\":\"OD3010721779461376\",\"requestId\":\"R-5074dfc6-6dfa-40e5-b7d1-563e4355d3d6\",\"clientTraceId\":\"T-R-75961d4a-d87c-4581-bb38-f491499b4347\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"106.77.208.107\",\"clientHostName\":\"w3-web1396:co-callisto-svc13.nm.flipkart.com\"},\"dataFlow\":\"digitalCompletedData\",\"accountId\":\"ACC14107023254836822\",\"email\":\"ramsingh.rajput1979@gmail.com\",\"channel\":{\"salesChannel\":\"MobileSite\",\"createdBy\":\"mobile_site\",\"terminalId\":\"TI141370081579076819236975657197918673077222640811745332907627329287\",\"userAgent\":\"Mozilla/5.0 (Linux; Android 4.4.2; iBall Slide 3G 1035Q-90 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Safari/537.36\",\"userIp\":\"106.77.208.107\",\"sessionId\":\"SI27C0BA47DCE94EAF9EE06438DBD01EC7\"},\"checkoutType\":\"PHYSICAL\",\"salesChannel\":\"MobileSite\"}}},{\"id\":\"a2e90c14dade5cbc4cd117294ea61f7ba6301cd063544d2ba665e2283d6186738defd166a7ad76f8d2535e6c86de34be713113dd04be314b53a9acbe80eeeb06\",\"timestamp\":1413866482315,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"a554db21-5311-4cc0-89cb-6e414535641a\",\"timestamp\":1413866482315},\"data\":{\"request\":{\"checkoutId\":\"OD2010721797294867\",\"requestId\":\"R-253d08c2-0e8c-4e15-97d1-ec8b850d390b\",\"clientTraceId\":\"T-R-9c156045-fd86-4813-874e-ff9980ee47ad\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"27.97.215.42\",\"clientHostName\":\"w3-web1379:co-callisto-svc10.nm.flipkart.com\"},\"actionType\":{\"type\":\"GET_CART\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866482304,\"endTime\":1413866482315,\"duration\":11}}},{\"id\":\"df69976d98ab17cfce8f2cd81c94f756dbde9728aca8c3526698dfbf99a85f1492dce0bdb0ea04646b0f9fb6048fd6777f9f661a221cbae0f61b921f3d54b6d7\",\"timestamp\":1413866482243,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"bb549cae-aba3-42d4-9829-feddec0df3dd\",\"timestamp\":1413866482243},\"data\":{\"request\":{\"checkoutId\":\"OD1010721756265324\",\"requestId\":\"R-d47203a5-299b-4226-b3c5-286019b162ac\",\"clientTraceId\":\"T-R-104fa382-e9e8-4413-9e22-1e03660e8364\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"117.207.101.37\",\"clientHostName\":\"w3-web1443:co-callisto-svc13.nm.flipkart.com\"},\"actionType\":{\"type\":\"MERGE_CART\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866482217,\"endTime\":1413866482243,\"duration\":26}}},{\"id\":\"9000fc5c8381b3734926f349cd126f5c94c176c9033ef4c03a772f8aca4af4750115a1d7c95ce4f4d4e1a8402dd069b5d20ed647f4a7fad83ea18f26572e913b\",\"timestamp\":1413866482103,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"CHECKOUT_EXECUTED\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc10.nm.flipkart.com\",\"eventId\":\"9db483dc-21e5-4a84-aa6a-f3f4ac893156\",\"timestamp\":1413866482103},\"data\":{\"request\":{\"checkoutId\":\"OD1010721762694587\",\"requestId\":\"R-2d84e508-be3b-497f-b982-918ea284f933\",\"clientTraceId\":\"T-R-225dfecc-9c0c-4a16-972b-8d8d0fb26c13\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"113.193.184.26\",\"clientHostName\":\"w3-web1494:co-callisto-svc5.nm.flipkart.com\"},\"dataFlow\":\"digitalCompletedData\",\"accountId\":\"ACC13615444923448791\",\"email\":\"magendiranmagi@yahoo.in\",\"channel\":{\"salesChannel\":\"WEB\",\"createdBy\":\"website\",\"terminalId\":\"TI141386560291578626369627558417376664564481551573210273929761598164\",\"userAgent\":\"Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.49 Safari/537.36\",\"userIp\":\"113.193.184.26\",\"sessionId\":\"SIE85D0BD79CB84BA8BB1A8C3F4C170330\"},\"checkoutType\":\"PHYSICAL\",\"salesChannel\":\"WEB\"}}},{\"id\":\"2133fe7770460ad2a8acb9ea0598a8ed07f7dcec2a6f8c11d5e97ff629265b5822db529d74851f1b3b59ddfd454d89164aad779dee6f54c0c2c0968a08026f14\",\"timestamp\":1413866482087,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"2149817c-0755-44d8-a943-f8739f37f350\",\"timestamp\":1413866482087},\"data\":{\"request\":{\"checkoutId\":\"OD1010721756265324\",\"requestId\":\"R-d47203a5-299b-4226-b3c5-286019b162ac\",\"clientTraceId\":\"T-R-104fa382-e9e8-4413-9e22-1e03660e8364\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"117.207.101.37\",\"clientHostName\":\"w3-web1443:co-callisto-svc13.nm.flipkart.com\"},\"actionType\":{\"type\":\"APPLY_PAYMENT_BASED_OFFERS\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866482086,\"endTime\":1413866482087,\"duration\":1}}},{\"id\":\"1a7c2eeaa98a9cc9de27a29d4923446eb8fec843a916a8a96f7edeeb803e44d80a6a7be95cfca52cf4eec406883621025061f0b444170609cf2e4e581426c1c1\",\"timestamp\":1413866482017,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"CHECKOUT_EXECUTED\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc4.nm.flipkart.com\",\"eventId\":\"e5567add-f025-419c-8c80-7ce9296d7a9f\",\"timestamp\":1413866482017},\"data\":{\"request\":{\"checkoutId\":\"OD3010721752496432\",\"requestId\":\"R-2f6e383f-0373-4f10-8165-aca4ff240afc\",\"clientTraceId\":\"T-R-8cb9a175-dd49-413a-8e4f-de297f56a144\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"43.252.26.177\",\"clientHostName\":\"w3-web1467:co-callisto-svc19.nm.flipkart.com\"},\"dataFlow\":\"digitalCompletedData\",\"accountId\":\"ACC14138054831687946\",\"email\":\"malikaakil007@gmail.com\",\"channel\":{\"salesChannel\":\"WEB\",\"createdBy\":\"website\",\"terminalId\":\"TI140618853968262498017663962585848918824560539392164405030040431035\",\"userAgent\":\"Mozilla/5.0 (Windows NT 5.1; rv:32.0) Gecko/20100101 Firefox/32.0\",\"userIp\":\"43.252.26.177\",\"sessionId\":\"SIFDCB8B3B430948EAA703BEFAA7B13997\"},\"checkoutType\":\"PHYSICAL\",\"salesChannel\":\"WEB\"}}},{\"id\":\"bf8e8ebec227ab1b642e099c3bc2e415dc3b2eb5b5893a74aabdcc7e5da811037749a5d3ff70a96124bd9aa47fe36cd9826ae04f4ce6034cc5bd361302a1026d\",\"timestamp\":1413866481990,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"d107ba93-1ac2-46a0-a395-ca92a24fdaf0\",\"timestamp\":1413866481990},\"data\":{\"request\":{\"checkoutId\":\"OD1010721756265324\",\"requestId\":\"R-e9a8eac9-3706-4b6c-9c2f-c577d1de4c89\",\"clientTraceId\":\"T-R-104fa382-e9e8-4413-9e22-1e03660e8364\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"117.207.101.37\",\"clientHostName\":\"w3-web1443:co-callisto-svc13.nm.flipkart.com\"},\"actionType\":{\"type\":\"INTEGRATION_LOGIC\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866481990,\"endTime\":1413866481990,\"duration\":0}}},{\"id\":\"040d5fefed4098dd386403935781f8707324ada55e905716015bd74de8282fc93d00cec620cc20bbd4f433f5ca88daa1a96066858194b9055c7ccc3ef1ee8915\",\"timestamp\":1413866481961,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"CHECKOUT_INIT\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc11.nm.flipkart.com\",\"eventId\":\"947f3d85-01dd-43af-844e-67f9ceae5c08\",\"timestamp\":1413866481961},\"data\":{\"request\":{\"checkoutId\":\"OD3010721796956060\",\"requestId\":\"R-eb8b140a-9c96-4a38-8226-d762d47ce10e\",\"clientTraceId\":\"T-R-8b12ce40-91ed-4ca1-bf3b-8a0c4bc479d3\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"1.39.63.17\",\"clientHostName\":\"w3-web1489:co-callisto-svc10.nm.flipkart.com\"},\"dataFlow\":\"digitalCompletedData\",\"checkoutType\":\"PHYSICAL\",\"salesChannel\":\"AndroidApp\"}}},{\"id\":\"9fd7531d8488056f36d6ce9e161a973612aab76a97aac2f6efbb59cbfccd2740543721fa127575c2f93f1a5c52a3c37eb79eb1cad389822e31da1dd0c7e0c251\",\"timestamp\":1413866481935,\"data\":{\"header\":{\"appName\":\"europa\",\"configName\":\"SERVICE_ACTION_CALL_SUCCESS\",\"profile\":\"platform\",\"instanceId\":\"co-europa-svc30.nm.flipkart.com\",\"eventId\":\"43649c65-bd56-4d7e-a121-6a362d1ed47d\",\"timestamp\":1413866481935},\"data\":{\"request\":{\"checkoutId\":\"OD0010721796878488\",\"requestId\":\"R-56abfc50-e237-42c5-9bf1-0dbd0084c92e\",\"clientTraceId\":\"T-R-266e5dce-efbf-4757-918b-d16fa57daca7\",\"client\":\"CHECKOUT\",\"clientAppIPAddress\":\"203.32.5.67\",\"clientHostName\":\"w3-web1498:co-callisto-svc21.nm.flipkart.com\"},\"actionType\":{\"type\":\"CREATE_ORDER\"},\"version\":\"1.0\",\"tenant\":\"flipkart\",\"startTime\":1413866481910,\"endTime\":1413866481935,\"duration\":25}}}]}";
        */
/*String json = "{\n" +
                "    \"opcode\": \"GroupResponse\",\n" +
                "    \"result\": {\n" +
                "        \"CHECKOUT_COMPLETE\": {\n" +
                "            \"PHYSICAL\": 6446798,\n" +
                "            \"FLIPKART_FIRST\": 27348,\n" +
                "            \"NONE\": 359,\n" +
                "            \"EGV\": 9332,\n" +
                "            \"DIGITAL\": 15542\n" +
                "        },\n" +
                "        \"CHECKOUT_INIT\": {\n" +
                "            \"PHYSICAL\": 66517780,\n" +
                "            \"FLIPKART_FIRST\": 127788,\n" +
                "            \"EGV\": 44081,\n" +
                "            \"DIGITAL\": 86646\n" +
                "        }\n" +
                "    }\n" +
                "}";*//*

        String json = "{\n" +
                "    \"opcode\": \"StatsResponse\",\n" +
                "    \"result\": {\n" +
                "        \"stats\": {\n" +
                "            \"min\": -419,\n" +
                "            \"max\": 23860,\n" +
                "            \"count\": 389725998,\n" +
                "            \"std_deviation\": 105.38667690479743,\n" +
                "            \"sum\": 12679624049,\n" +
                "            \"variance\": 11106.351669036163,\n" +
                "            \"sum_of_squares\": 4740961935807,\n" +
                "            \"avg\": 32.53471442518443\n" +
                "        }" +
                "    }\n" +
                "}";
        StatsValue statsValue = new StatsValue();
        Map<Number, Number> percentiles = Maps.newHashMap();
        percentiles.put(99.0, 332.04164711863444);
        percentiles.put(25.0, 5);
        statsValue.setPercentiles(percentiles);
        Map<String, Number> stats = Maps.newHashMap();
        stats.put("min", -419);
        stats.put("max", 23860);
        statsValue.setStats(stats);
        StatsResponse statsResponse = new StatsResponse();
        statsResponse.setResult(statsValue);
        ObjectMapper objectMapper = new ObjectMapper();
        //objectMapper.enable(DeserializationFeature.)
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setNesting(Lists.newArrayList("header.configName", "data.checkoutType"));
        Flattener flattener = new Flattener(objectMapper, groupRequest);
        //new ObjectMapper().readValue(json, StatsResponse.class).accept(flattener);
        statsResponse.accept(flattener);
        System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(flattener.getFlatRepresentation()));
    }
*/


}
