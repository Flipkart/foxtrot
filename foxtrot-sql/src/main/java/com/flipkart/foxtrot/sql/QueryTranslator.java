package com.flipkart.foxtrot.sql;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.distinct.DistinctRequest;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.*;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.flipkart.foxtrot.sql.extendedsql.ExtendedSqlStatement;
import com.flipkart.foxtrot.sql.extendedsql.desc.Describe;
import com.flipkart.foxtrot.sql.extendedsql.showtables.ShowTables;
import com.flipkart.foxtrot.sql.query.FqlActionQuery;
import com.flipkart.foxtrot.sql.query.FqlDescribeTable;
import com.flipkart.foxtrot.sql.query.FqlShowTablesQuery;
import com.flipkart.foxtrot.sql.util.QueryUtils;
import com.google.common.collect.Lists;
import io.dropwizard.util.Duration;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.List;

public class QueryTranslator extends SqlElementVisitor {
    private static final Logger logger = LoggerFactory.getLogger(QueryTranslator.class.getSimpleName());
    private static final MetaStatementMatcher metastatementMatcher = new MetaStatementMatcher();

    private FqlQueryType queryType = FqlQueryType.select;
    private String tableName;
    private List<String> groupBycolumnsList = Lists.newArrayList();
    private ResultSort resultSort;
    private boolean hasLimit = false;
    private long limitFrom;
    private long limitCount;
    private ActionRequest calledAction;
    private List<Filter> filters;
    private List<String> selectedColumns = Lists.newArrayList();
    private List<ResultSort> columnsWithSort = Lists.newArrayList();

    @Override
    public void visit(PlainSelect plainSelect) {
        List selectItems = plainSelect.getSelectItems();
        //selectItems.accept(this);
        for(Object selectItem : selectItems) {
            SelectItem selectExpressionItem = (SelectItem)selectItem;
            //System.out.println(selectExpressionItem.getExpression());
            FunctionReader functionReader = new FunctionReader();
            selectExpressionItem.accept(functionReader);
            final String columnName = functionReader.columnName;
            if(null != columnName && !columnName.isEmpty()) {
                selectedColumns.add(columnName);
                continue;
            }
            calledAction = functionReader.actionRequest;
            queryType = functionReader.queryType;
        }

        plainSelect.getFromItem().accept(this); //Populate table name
        List groupByItems = plainSelect.getGroupByColumnReferences();
        if(null != groupByItems) {
            queryType = FqlQueryType.group;
            for(Object groupByItem : groupByItems) {
                if(groupByItem instanceof Column) {
                    Column column = (Column) groupByItem;
                    groupBycolumnsList.add(column.getFullyQualifiedName());
                }
            }
        }
        if(FqlQueryType.select == queryType) {
            List orderByElements = plainSelect.getOrderByElements();
            resultSort = generateResultSort(orderByElements);
            if (null != plainSelect.getLimit()) {
                hasLimit = true;
                limitFrom = plainSelect.getLimit().getOffset();
                limitCount = plainSelect.getLimit().getRowCount();
            }
        }

        if(null != plainSelect.getWhere()) {
            FilterParser filterParser = new FilterParser();
            plainSelect.getWhere().accept(filterParser);
            filters = (filterParser.filters.isEmpty()) ? null : filterParser.filters;
        }

        // Handle distinct
        List<ResultSort> tempColumnsWithSort = generateColumnSort(plainSelect.getOrderByElements());
        if (null != plainSelect.getDistinct()){
            for (String selectedColumn : selectedColumns){
                boolean alreadyAdded = false;
                for (ResultSort columnWithSort : tempColumnsWithSort){
                    if (selectedColumn.equalsIgnoreCase(columnWithSort.getField())){
                        columnsWithSort.add(columnWithSort);
                        alreadyAdded = true;
                        break;
                    }
                }
                if (!alreadyAdded){
                    ResultSort resultSort = new ResultSort();
                    resultSort.setField(selectedColumn);
                    resultSort.setOrder(ResultSort.Order.desc);
                    columnsWithSort.add(resultSort);
                }
            }
            this.queryType = FqlQueryType.distinct;
        }
    }

    @Override
    public void visit(Select select) {
        select.getSelectBody().accept(this);
    }

    @Override
    public void visit(Table tableName) {
        this.tableName = tableName.getName().replaceAll(Constants.SQL_TABLE_REGEX, "");
    }

    @Override
    public void visit(Function function) {
        List params = function.getParameters().getExpressions();

        ((Expression)params.toArray()[0]).accept(this); //TODO
    }

    @Override
    public void visit(ExpressionList expressionList) {
        ExpressionList expressions = (ExpressionList)expressionList.getExpressions();
        for(Object expression : expressions.getExpressions()) {
            System.out.println(expression.getClass());
        }
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        selectExpressionItem.getExpression().accept(this);
    }

    public FqlQuery translate(String sql) throws Exception {
        ExtendedSqlStatement extendedSqlStatement = metastatementMatcher.parse(sql);
        if(null != extendedSqlStatement) {
            ExtendedSqlParser parser = new ExtendedSqlParser();
            extendedSqlStatement.receive(parser);
            return parser.getQuery();
        }

        CCJSqlParserManager ccjSqlParserManager = new CCJSqlParserManager();
        Statement statement = ccjSqlParserManager.parse(new StringReader(sql));
        Select select = (Select) statement;
        select.accept(this);
        ActionRequest request = null;
        switch (queryType) {
            case select: {
                Query query = new Query();
                query.setTable(tableName);
                query.setSort(resultSort);
                if(hasLimit) {
                    query.setFrom((int)limitFrom);
                    query.setLimit((int) limitCount);
                }
                query.setFilters(filters);
                request = query;
                break;
            }
            case group: {
                GroupRequest group = new GroupRequest();
                group.setTable(tableName);
                group.setNesting(groupBycolumnsList);
                group.setFilters(filters);
                request = group;
                break;
            }
            case trend: {
                TrendRequest trend =  (TrendRequest)calledAction;
                trend.setTable(tableName);
                trend.setFilters(filters);
                request = trend;
                break;
            }
            case statstrend: {
                StatsTrendRequest statsTrend =  (StatsTrendRequest)calledAction;
                statsTrend.setTable(tableName);
                statsTrend.setFilters(filters);
                request = statsTrend;
                break;
            }
            case stats: {
                StatsRequest stats =  (StatsRequest)calledAction;
                stats.setTable(tableName);
                stats.setFilters(filters);
                request = stats;
                break;
            }
            case histogram: {
                HistogramRequest histogram = (HistogramRequest)calledAction;
                histogram.setTable(tableName);
                histogram.setFilters(filters);
                request = histogram;
                break;
            }
            case count: {
                CountRequest countRequest = (CountRequest)calledAction;
                countRequest.setTable(tableName);
                countRequest.setFilters(filters);
                request = countRequest;
                break;
            }

            case distinct: {
                DistinctRequest distinctRequest = new DistinctRequest();
                distinctRequest.setTable(tableName);
                distinctRequest.setFilters(filters);
                distinctRequest.setNesting(columnsWithSort);
                request = distinctRequest;
            }

        }
        if(null == request) {
            throw new Exception("Could not parse provided FQL.");
        }
        return new FqlActionQuery(queryType, request, selectedColumns);
    }

    private ResultSort generateResultSort(List orderByElements) {
        if(null == orderByElements) {
            return null;
        }
        for(Object orderByElementObject : orderByElements) {
            OrderByElement orderByElement = (OrderByElement)orderByElementObject;
            Column sortColumn = (Column)orderByElement.getExpression();
            ResultSort resultSort = new ResultSort();
            resultSort.setField(sortColumn.getFullyQualifiedName());
            resultSort.setOrder(orderByElement.isAsc()? ResultSort.Order.asc : ResultSort.Order.desc);
            logger.info("ResultSort: " + resultSort);
            return resultSort;
        }
        return null;
    }

    private List<ResultSort> generateColumnSort(List<OrderByElement> orderItems){
        List<ResultSort> resultSortList = Lists.newArrayList();
        if (orderItems == null || orderItems.isEmpty()){
            return resultSortList;
        }
        for (OrderByElement orderByElement : orderItems){
            Column sortColumn = (Column)orderByElement.getExpression();
            ResultSort resultSort = new ResultSort();
            resultSort.setField(sortColumn.getFullyQualifiedName());
            resultSort.setOrder(orderByElement.isAsc()? ResultSort.Order.asc : ResultSort.Order.desc);
            resultSortList.add(resultSort);
        }
        return resultSortList;
    }

    private static final class FunctionReader extends SqlElementVisitor {
        private boolean allColumn = false;
        private ActionRequest actionRequest;
        public FqlQueryType queryType = FqlQueryType.select;
        private String columnName = null;

        @Override
        public void visit(SelectExpressionItem selectExpressionItem) {
            Expression expression = selectExpressionItem.getExpression();
            if(expression instanceof Function) {
                Function function = (Function)expression;
                queryType = getType(function.getName());
                switch (queryType) {
                    case trend:
                        actionRequest = parseTrendFunction(function.getParameters().getExpressions());
                        break;
                    case statstrend:
                        actionRequest = parseStatsTrendFunction(function.getParameters().getExpressions());
                        break;
                    case stats:
                        actionRequest = parseStatsFunction(function.getParameters().getExpressions());
                        break;
                    case histogram:
                        actionRequest = parseHistogramRequest(function.getParameters());
                        break;
                    case count:
                        actionRequest = parseCountRequest(function.getParameters(), function.isAllColumns(), function.isDistinct());
                        break;
                    case desc:
                    case select:
                    case group:
                        break;
                }
            }
            else {

                if (expression instanceof Parenthesis){
                    columnName = ((Column)((Parenthesis)expression).getExpression()).getFullyQualifiedName();
                } else if(expression instanceof Column) {
                    columnName = ((Column)expression).getFullyQualifiedName();
                }
            }
        }

        private FqlQueryType getType(String function) {
            if(function.equalsIgnoreCase("trend")) {
                return FqlQueryType.trend;
            }
            if(function.equalsIgnoreCase("statstrend")) {
                return FqlQueryType.statstrend;
            }
            if(function.equalsIgnoreCase("stats")) {
                return FqlQueryType.stats;
            }
            if(function.equalsIgnoreCase("histogram")) {
                return FqlQueryType.histogram;
            }
            if(function.equalsIgnoreCase("count")) {
                return FqlQueryType.count;
            }
            return FqlQueryType.select;
        }

        private TrendRequest parseTrendFunction(List expressions) {
            if(expressions == null || expressions.isEmpty() || expressions.size() > 3) {
                throw new RuntimeException("trend function has following format: trend(fieldname, [period, [timestamp field]])");
            }
            TrendRequest trendRequest = new TrendRequest();
            trendRequest.setField(QueryUtils.expressionToString((Expression) expressions.get(0)));
            if(expressions.size() > 1) {
                trendRequest.setPeriod(Period.valueOf(QueryUtils.expressionToString((Expression) expressions.get(1)).toLowerCase()));
            }
            if(expressions.size() > 2) {
                trendRequest.setTimestamp(QueryUtils.expressionToString((Expression) expressions.get(2)));
            }
            return trendRequest;
        }

        private StatsTrendRequest parseStatsTrendFunction(List expressions) {
            if(expressions == null || expressions.isEmpty() || expressions.size() > 2) {
                throw new RuntimeException("statstrend function has following format: statstrend(fieldname, [period])");
            }
            StatsTrendRequest statsTrendRequest = new StatsTrendRequest();
            statsTrendRequest.setField(QueryUtils.expressionToString((Expression) expressions.get(0)));
            if(expressions.size() > 1) {
                statsTrendRequest.setPeriod(Period.valueOf(QueryUtils.expressionToString((Expression) expressions.get(1)).toLowerCase()));
            }
            return statsTrendRequest;
        }

        private StatsRequest parseStatsFunction(List expressions) {
            if(expressions == null || expressions.isEmpty() || expressions.size() > 1) {
                throw new RuntimeException("stats function has following format: stats(fieldname)");
            }
            StatsRequest statsRequest = new StatsRequest();
            statsRequest.setField(QueryUtils.expressionToString((Expression) expressions.get(0)));
            return statsRequest;
        }

        private HistogramRequest parseHistogramRequest(ExpressionList expressionList) {
            if(expressionList != null && (expressionList.getExpressions() != null && expressionList.getExpressions().size() > 2)) {
                throw new RuntimeException("histogram function has the following format: histogram([period, [timestamp field]])");
            }
            HistogramRequest histogramRequest = new HistogramRequest();
            if(null != expressionList) {
                List expressions = expressionList.getExpressions();
                histogramRequest.setPeriod(Period.valueOf(QueryUtils.expressionToString((Expression) expressions.get(0)).toLowerCase()));
                if(expressions.size() > 1) {
                    histogramRequest.setField(QueryUtils.expressionToString((Expression) expressions.get(1)));
                }
            }
            return histogramRequest;
        }

        private ActionRequest parseCountRequest(ExpressionList expressionList, boolean allColumns, boolean isDistinct) {

            CountRequest countRequest = new CountRequest();
            if (allColumns){
                countRequest.setField(null);
                return countRequest;
            }

            if (expressionList != null && (expressionList.getExpressions() != null && expressionList.getExpressions().size() == 1)) {
                List<Expression> expressions = expressionList.getExpressions();
                if (allColumns){
                    countRequest.setField(null);
                } else {
                    countRequest.setField(expressionToString(expressions.get(0)));
                    countRequest.setDistinct(isDistinct);
                }
                return countRequest;
            }
            throw new RuntimeException("count function has the following format: count([distinct] */column_name)");
        }

        private String expressionToString(Expression expression) {
            if(expression instanceof Column) {
                return ((Column)expression).getFullyQualifiedName();
            }
            if(expression instanceof StringValue) {
                return ((StringValue)expression).getValue();
            }
            return null;
        }

        @Override
        public void visit(AllColumns allColumns) {
            allColumn = true;
        }

        public boolean isAllColumn() {
            return allColumn;
        }

    }

    private static final class FilterParser extends SqlElementVisitor {

        private List<Filter> filters = Lists.newArrayList();

        @Override
        public void visit(EqualsTo equalsTo) {
            EqualsFilter equalsFilter = new EqualsFilter();
            String field = ((Column)equalsTo.getLeftExpression()).getFullyQualifiedName();
            equalsFilter.setField(field.replaceAll(Constants.SQL_FIELD_REGEX, ""));
            equalsFilter.setValue(getValueFromExpression(equalsTo.getRightExpression()));
            filters.add(equalsFilter);
        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            NotEqualsFilter notEqualsFilter = new NotEqualsFilter();
            String field = ((Column)notEqualsTo.getLeftExpression()).getFullyQualifiedName();
            notEqualsFilter.setField(field.replaceAll(Constants.SQL_FIELD_REGEX, ""));
            notEqualsFilter.setValue(getValueFromExpression(notEqualsTo.getRightExpression()));
            filters.add(notEqualsFilter);
        }

        @Override
        public void visit(AndExpression andExpression) {
            andExpression.getLeftExpression().accept(this);
            andExpression.getRightExpression().accept(this);
        }

        @Override
        public void visit(Between between) {
            BetweenFilter betweenFilter = new BetweenFilter();
            ColumnData columnData = setupColumn(between.getLeftExpression());
            betweenFilter.setField(columnData.getColumnName().replaceAll(Constants.SQL_FIELD_REGEX, ""));
            betweenFilter.setTemporal(columnData.isTemporal());
            Number from = getNumbericValue(between.getBetweenExpressionStart());
            Number to = getNumbericValue(between.getBetweenExpressionEnd());
            betweenFilter.setFrom(from);
            betweenFilter.setTo(to);
            filters.add(betweenFilter);
        }

        @Override
        public void visit(GreaterThan greaterThan) {
            GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
            ColumnData columnData = setupColumn(greaterThan.getLeftExpression());
            greaterThanFilter.setField(columnData.getColumnName().replaceAll(Constants.SQL_FIELD_REGEX, ""));
            greaterThanFilter.setTemporal(columnData.isTemporal());
            greaterThanFilter.setValue(getNumbericValue(greaterThan.getRightExpression()));
            filters.add(greaterThanFilter);
        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
            ColumnData columnData = setupColumn(greaterThanEquals.getLeftExpression());
            greaterEqualFilter.setField(columnData.getColumnName().replaceAll(Constants.SQL_FIELD_REGEX, ""));
            greaterEqualFilter.setTemporal(columnData.isTemporal());
            greaterEqualFilter.setValue(getNumbericValue(greaterThanEquals.getRightExpression()));
            filters.add(greaterEqualFilter);
        }

        @Override
        public void visit(InExpression inExpression) {
            InFilter inFilter = new InFilter();
            inFilter.setField(((Column)inExpression.getLeftExpression()).getFullyQualifiedName().replaceAll(Constants.SQL_FIELD_REGEX, ""));
            ItemsList itemsList = inExpression.getRightItemsList();
            if(!(itemsList instanceof ExpressionList)) {
                throw new RuntimeException("Sub selects not supported");
            }

            ExpressionList expressionList = (ExpressionList)itemsList;
            List<Object> filterValues = Lists.newArrayList();
            for(Object expressionObject : expressionList.getExpressions()) {
                Expression expression = (Expression) expressionObject;
                filterValues.add(getValueFromExpression(expression));
            }
            inFilter.setValues(filterValues);
            filters.add(inFilter);
        }

        @Override
        public void visit(IsNullExpression isNullExpression) {
            super.visit(isNullExpression);
            ColumnData columnData = setupColumn(isNullExpression.getLeftExpression());
            if (isNullExpression.isNot()) {
                ExistsFilter existsFilter = new ExistsFilter();

                existsFilter.setField(columnData.getColumnName().replaceAll(Constants.SQL_FIELD_REGEX, ""));
                filters.add(existsFilter);
            } else {
                MissingFilter missingFilter = new MissingFilter();
                missingFilter.setField(columnData.getColumnName().replaceAll(Constants.SQL_FIELD_REGEX, ""));
                filters.add(missingFilter);
            }
        }

        @Override
        public void visit(LikeExpression likeExpression) {
            super.visit(likeExpression);
            //ContainsFilter containsFilter = new ContainsFilter(); TODO
        }

        @Override
        public void visit(MinorThan minorThan) {
            LessThanFilter lessThanFilter = new LessThanFilter();
            ColumnData columnData = setupColumn(minorThan.getLeftExpression());
            lessThanFilter.setField(columnData.getColumnName().replaceAll(Constants.SQL_FIELD_REGEX, ""));
            lessThanFilter.setTemporal(columnData.isTemporal());
            lessThanFilter.setValue(getNumbericValue(minorThan.getRightExpression()));
            filters.add(lessThanFilter);

        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            LessEqualFilter lessEqualFilter = new LessEqualFilter();
            ColumnData columnData = setupColumn(minorThanEquals.getLeftExpression());
            lessEqualFilter.setField(columnData.getColumnName().replaceAll(Constants.SQL_FIELD_REGEX, ""));
            lessEqualFilter.setTemporal(columnData.isTemporal());
            lessEqualFilter.setValue(getNumbericValue(minorThanEquals.getRightExpression()));
            filters.add(lessEqualFilter);
        }

        @Override
        public void visit(Function function) {
            if(function.getName().equalsIgnoreCase("last")) {
                LastFilter lastFilter = parseWindowFunction(function.getParameters().getExpressions());
                filters.add(lastFilter);
                return;
            }
            throw new RuntimeException("Only last() function is supported");
        }


        private LastFilter parseWindowFunction(List expressions) {
            if(expressions == null || expressions.isEmpty() || expressions.size() > 3) {
                throw new RuntimeException("last function has following format: last(duration, [start-time, [timestamp field]])");
            }
            LastFilter lastFilter = new LastFilter();
            lastFilter.setDuration(Duration.parse(QueryUtils.expressionToString((Expression) expressions.get(0))));
            if(expressions.size() > 1) {
                lastFilter.setCurrentTime(QueryUtils.expressionToNumber((Expression) expressions.get(1)).longValue());
            }
            if(expressions.size() > 2) {
                lastFilter.setField(QueryUtils.expressionToString((Expression) expressions.get(2)).replaceAll(Constants.SQL_FIELD_REGEX, ""));
            }
            return lastFilter;
        }

        private Object getValueFromExpression(Expression expression) {
            if(expression instanceof StringValue) {
                return ((StringValue) expression).getValue();
            }
            return getNumbericValue(expression);
        }

        private Number getNumbericValue(Expression expression) {
            if(expression instanceof DoubleValue) {
                return ((DoubleValue)expression).getValue();
            }
            if(expression instanceof LongValue) {
                return ((LongValue)expression).getValue();
            }
            if(expression instanceof DateValue) {
                return ((DateValue)expression).getValue().getTime();
            }
            if(expression instanceof TimeValue) {
                return ((TimeValue)expression).getValue().getTime();
            }
            throw new RuntimeException("Unsupported value type.");

        }

        private static final class ColumnData {
            private final String columnName;
            private boolean temporal = false;
            private boolean window = false;

            private ColumnData(String columnName) {
                this.columnName = columnName;
            }

            static ColumnData temporal(String columnName) {
                ColumnData columnData = new ColumnData(columnName);
                columnData.temporal = true;
                return columnData;
            }

            static ColumnData window(String columnName) {
                ColumnData columnData = new ColumnData(columnName);
                columnData.window = true;
                return columnData;
            }

            public String getColumnName() {
                return columnName;
            }

            public boolean isTemporal() {
                return temporal;
            }

            public boolean isWindow() {
                return window;
            }
        }
        private ColumnData setupColumn(Expression expression) {
            if(expression instanceof Function) {
                Function function = (Function) expression;
                if(function.getName().equalsIgnoreCase("temporal")) {
                    List parameters = function.getParameters().getExpressions();
                    if(parameters.size() != 1 || ! (parameters.get(0) instanceof Column)) {
                        throw new RuntimeException("temporal function must have a fieldname as parameter");
                    }
                    return ColumnData.temporal(((Column)parameters.get(0)).getFullyQualifiedName());
                }
                throw new RuntimeException("Only the function 'temporal' is supported in where clause");
            }
            if(expression instanceof Column) {
                return new ColumnData(((Column)expression).getFullyQualifiedName());
            }
            throw new RuntimeException("Only the function 'temporal([fieldname)' and fieldname is supported in where clause");
        }
    }

    private static final class ExtendedSqlParser extends SqlElementVisitor {
        private FqlQuery query;

        @Override
        public void visit(Describe describe) {
            query = new FqlDescribeTable(describe.getTable().getName());
        }

        @Override
        public void visit(ShowTables showTables) {
            query = new FqlShowTablesQuery();
        }

        public FqlQuery getQuery() {
            return query;
        }
    }


}