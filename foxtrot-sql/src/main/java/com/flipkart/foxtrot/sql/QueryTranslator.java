package com.flipkart.foxtrot.sql;


import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.histogram.HistogramRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.Query;
import com.flipkart.foxtrot.common.query.ResultSort;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.InFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.stats.StatsRequest;
import com.flipkart.foxtrot.common.stats.StatsTrendRequest;
import com.flipkart.foxtrot.common.trend.TrendRequest;
import com.google.common.collect.Lists;
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

    private QueryType queryType = QueryType.select;
    private String tableName;
    private boolean allColumn = false;
    private List<String> groupBycolumnsList = Lists.newArrayList();
    private ResultSort resultSort;
    private boolean hasLimit = false;
    private long limitFrom;
    private long limitCount;
    private ActionRequest calledAction;
    private List<Filter> filters;

    @Override
    public void visit(PlainSelect plainSelect) {
        List selectItems = plainSelect.getSelectItems();
        //selectItems.accept(this);
        for(Object selectItem : selectItems) {
            SelectItem selectExpressionItem = (SelectItem)selectItem;
            //System.out.println(selectExpressionItem.getExpression());
            FunctionReader functionReader = new FunctionReader();
            selectExpressionItem.accept(functionReader);
            allColumn = functionReader.isAllColumn();
            calledAction = functionReader.actionRequest;
            queryType = functionReader.queryType;
        }
        plainSelect.getFromItem().accept(this); //Populate table name
        List groupByItems = plainSelect.getGroupByColumnReferences();
        if(null != groupByItems) {
            queryType = QueryType.group;
            for(Object groupByItem : groupByItems) {
                if(groupByItem instanceof Column) {
                    Column column = (Column) groupByItem;
                    groupBycolumnsList.add(column.getWholeColumnName());
                }
            }
        }
        if(QueryType.select == queryType) {
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
    }

    @Override
    public void visit(Select select) {
        select.getSelectBody().accept(this);
    }

    @Override
    public void visit(Table tableName) {
        this.tableName = tableName.getName();
    }

    @Override
    public void visit(Function function) {
        System.out.println("   FUNCTION: " + function.getName());
        List params = function.getParameters().getExpressions();

        ((Expression)params.toArray()[0]).accept(this); //TODO
    }

    @Override
    public void visit(Column tableColumn) {
        System.out.println("        COLUMN: " + tableColumn.getWholeColumnName());
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

    public ActionRequest translate(String sql) throws Exception {
        CCJSqlParserManager ccjSqlParserManager = new CCJSqlParserManager();
        Statement statement = ccjSqlParserManager.parse(new StringReader(sql));
        Select select = (Select) statement;
        select.accept(this);
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
                return query;
            }
            case group: {
                GroupRequest group = new GroupRequest();
                group.setTable(tableName);
                group.setNesting(groupBycolumnsList);
                group.setFilters(filters);
                return group;
            }
            case trend: {
                TrendRequest trend =  (TrendRequest)calledAction;
                trend.setTable(tableName);
                trend.setFilters(filters);
                return trend;
            }
            case statstrend: {
                StatsTrendRequest statsTrend =  (StatsTrendRequest)calledAction;
                statsTrend.setTable(tableName);
                statsTrend.setFilters(filters);
                return statsTrend;
            }
            case stats: {
                StatsRequest stats =  (StatsRequest)calledAction;
                stats.setTable(tableName);
                stats.setFilters(filters);
                return stats;
            }
            case histogram: {
                HistogramRequest histogram = (HistogramRequest)calledAction;
                histogram.setTable(tableName);
                histogram.setFilters(filters);
                return histogram;
            }
            case desc:
                break;
        }
        return null;
    }

    private ResultSort generateResultSort(List orderByElements) {
        if(null == orderByElements) {
            return null;
        }
        for(Object orderByElementObject : orderByElements) {
            OrderByElement orderByElement = (OrderByElement)orderByElementObject;
            Column sortColumn = (Column)orderByElement.getExpression();
            ResultSort resultSort = new ResultSort();
            resultSort.setField(sortColumn.getWholeColumnName());
            resultSort.setOrder(orderByElement.isAsc()? ResultSort.Order.asc : ResultSort.Order.desc);
            logger.info("ResultSort: " + resultSort);
            return resultSort;
        }
        return null;
    }

    private enum QueryType {
        select,
        group,
        trend,
        statstrend,
        stats,
        histogram,
        desc,
    }

    private static final class FunctionReader extends SqlElementVisitor {
        private boolean allColumn = false;
        private ActionRequest actionRequest;
        public QueryType queryType = QueryType.select;

        @Override
        public void visit(SelectExpressionItem selectExpressionItem) {
            Function function = (Function)selectExpressionItem.getExpression(); //TODO::HANDLE FIELDS
            //functionName = function.getName();
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
                case desc:
                case select:
                case group:
                    break;
            }
        }

        private QueryType getType(String function) {
            if(function.equalsIgnoreCase("trend")) {
                return QueryType.trend;
            }
            if(function.equalsIgnoreCase("statstrend")) {
                return QueryType.statstrend;
            }
            if(function.equalsIgnoreCase("stats")) {
                return QueryType.stats;
            }
            if(function.equalsIgnoreCase("histogram")) {
                return QueryType.histogram;
            }
            return QueryType.select;
        }

        private TrendRequest parseTrendFunction(List expressions) {
            if(expressions == null || expressions.isEmpty() || expressions.size() > 3) {
                throw new RuntimeException("trend function has following format: trend(fieldname, [period, [timestamp field]])");
            }
            TrendRequest trendRequest = new TrendRequest();
            trendRequest.setField(expressionToString((Expression) expressions.get(0)));
            if(expressions.size() > 1) {
                trendRequest.setPeriod(Period.valueOf(expressionToString((Expression) expressions.get(1)).toLowerCase()));
            }
            if(expressions.size() > 2) {
                trendRequest.setTimestamp(expressionToString((Expression) expressions.get(2)));
            }
            return trendRequest;
        }

        private StatsTrendRequest parseStatsTrendFunction(List expressions) {
            if(expressions == null || expressions.isEmpty() || expressions.size() > 2) {
                throw new RuntimeException("statstrend function has following format: statstrend(fieldname, [period])");
            }
            StatsTrendRequest statsTrendRequest = new StatsTrendRequest();
            statsTrendRequest.setField(expressionToString((Expression) expressions.get(0)));
            if(expressions.size() > 1) {
                statsTrendRequest.setPeriod(Period.valueOf(expressionToString((Expression) expressions.get(1)).toLowerCase()));
            }
            return statsTrendRequest;
        }

        private StatsRequest parseStatsFunction(List expressions) {
            if(expressions == null || expressions.isEmpty() || expressions.size() > 1) {
                throw new RuntimeException("stats function has following format: stats(fieldname)");
            }
            StatsRequest statsRequest = new StatsRequest();
            statsRequest.setField(expressionToString((Expression) expressions.get(0)));
            return statsRequest;
        }

        private HistogramRequest parseHistogramRequest(ExpressionList expressionList) {
            if(expressionList != null && (expressionList.getExpressions() != null && expressionList.getExpressions().size() > 2)) {
                throw new RuntimeException("histogram function has the following format: histogram([period, [timestamp field]])");
            }
            HistogramRequest histogramRequest = new HistogramRequest();
            if(null != expressionList) {
                List expressions = expressionList.getExpressions();
                histogramRequest.setPeriod(Period.valueOf(expressionToString((Expression) expressions.get(0)).toLowerCase()));
                if(expressions.size() > 1) {
                    histogramRequest.setField(expressionToString((Expression) expressions.get(1)));
                }
            }
            return histogramRequest;
        }

        private String expressionToString(Expression expression) {
            if(expression instanceof Column) {
                return ((Column)expression).getWholeColumnName();
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
            equalsFilter.setField(((Column)equalsTo.getLeftExpression()).getWholeColumnName());
            equalsFilter.setValue(getValueFromExpression(equalsTo.getRightExpression()));
            filters.add(equalsFilter);
        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            NotEqualsFilter notEqualsFilter = new NotEqualsFilter();
            notEqualsFilter.setField(((Column)notEqualsTo.getLeftExpression()).getWholeColumnName());
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
            betweenFilter.setField(columnData.getColumnName());
            betweenFilter.setTemporal(columnData.isTemporal());
            betweenFilter.setFrom(getNumbericValue(between.getBetweenExpressionStart()));
            betweenFilter.setTo(getNumbericValue(between.getBetweenExpressionEnd()));
            filters.add(betweenFilter);
        }

        @Override
        public void visit(GreaterThan greaterThan) {
            GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
            ColumnData columnData = setupColumn(greaterThan.getLeftExpression());
            greaterThanFilter.setField(columnData.getColumnName());
            greaterThanFilter.setTemporal(columnData.isTemporal());
            greaterThanFilter.setValue(getNumbericValue(greaterThan.getRightExpression()));
            filters.add(greaterThanFilter);
        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            GreaterEqualFilter greaterEqualFilter = new GreaterEqualFilter();
            ColumnData columnData = setupColumn(greaterThanEquals.getLeftExpression());
            greaterEqualFilter.setField(columnData.getColumnName());
            greaterEqualFilter.setTemporal(columnData.isTemporal());
            greaterEqualFilter.setValue(getNumbericValue(greaterThanEquals.getRightExpression()));
            filters.add(greaterEqualFilter);
        }

        @Override
        public void visit(InExpression inExpression) {
            InFilter inFilter = new InFilter();
            inFilter.setField(((Column)inExpression.getLeftExpression()).getWholeColumnName());
            ItemsList itemsList = inExpression.getItemsList();
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
            lessThanFilter.setField(columnData.getColumnName());
            lessThanFilter.setTemporal(columnData.isTemporal());
            lessThanFilter.setValue(getNumbericValue(minorThan.getRightExpression()));
            filters.add(lessThanFilter);

        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            LessEqualFilter lessEqualFilter = new LessEqualFilter();
            ColumnData columnData = setupColumn(minorThanEquals.getLeftExpression());
            lessEqualFilter.setField(columnData.getColumnName());
            lessEqualFilter.setTemporal(columnData.isTemporal());
            lessEqualFilter.setValue(getNumbericValue(minorThanEquals.getRightExpression()));
            filters.add(lessEqualFilter);
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

            private ColumnData(String columnName) {
                this.columnName = columnName;
            }

            private ColumnData(String columnName, boolean temporal) {
                this.columnName = columnName;
                this.temporal = temporal;
            }

            public String getColumnName() {
                return columnName;
            }

            public boolean isTemporal() {
                return temporal;
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
                    return new ColumnData(((Column)parameters.get(0)).getWholeColumnName(), true);
                }
                throw new RuntimeException("Only the function 'temporal' is supported in where clause");
            }
            if(expression instanceof Column) {
                return new ColumnData(((Column)expression).getWholeColumnName());
            }
            throw new RuntimeException("Only the function 'temporal([fieldname)' and fieldname is supported in where clause");
        }
    }


}