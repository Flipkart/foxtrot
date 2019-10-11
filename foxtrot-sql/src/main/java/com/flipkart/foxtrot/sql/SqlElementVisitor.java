package com.flipkart.foxtrot.sql;

import com.flipkart.foxtrot.core.exception.FqlParsingException;
import com.flipkart.foxtrot.sql.extendedsql.ExtendedSqlStatementVisitor;
import com.flipkart.foxtrot.sql.extendedsql.desc.Describe;
import com.flipkart.foxtrot.sql.extendedsql.showtables.ShowTables;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class SqlElementVisitor
        implements StatementVisitor, SelectVisitor, FromItemVisitor, ItemsListVisitor, ExpressionVisitor,
        SelectItemVisitor, ExtendedSqlStatementVisitor {

    @Override
    public void visit(NullValue nullValue) {
        invalid(nullValue);
    }

    private void invalid(Object object) {
        throw new FqlParsingException("Unsupported construct: " + object.getClass()
                .getSimpleName());
    }

    @Override
    public void visit(Function function) {
        invalid(function);
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        invalid(signedExpression);
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        invalid(jdbcParameter);
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        invalid(jdbcNamedParameter);
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        invalid(doubleValue);
    }

    @Override
    public void visit(LongValue longValue) {
        invalid(longValue);
    }

    @Override
    public void visit(DateValue dateValue) {
        invalid(dateValue);
    }

    @Override
    public void visit(TimeValue timeValue) {
        invalid(timeValue);
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        //supported construct
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        //supported construct
    }

    @Override
    public void visit(StringValue stringValue) {
        //supported construct
    }

    @Override
    public void visit(Addition addition) {
        //supported construct
    }

    @Override
    public void visit(Division division) {
        //supported construct
    }

    @Override
    public void visit(Multiplication multiplication) {
        //supported construct
    }

    @Override
    public void visit(Subtraction subtraction) {
        //supported construct
    }

    @Override
    public void visit(AndExpression andExpression) {
        //supported construct
    }

    @Override
    public void visit(OrExpression orExpression) {
        //supported construct
    }

    @Override
    public void visit(Between between) {
        //supported construct
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        //supported construct
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        //supported construct
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        //supported construct
    }

    @Override
    public void visit(InExpression inExpression) {
        //supported construct
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        //supported construct
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        //supported construct
    }

    @Override
    public void visit(MinorThan minorThan) {
        //supported construct
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        //supported construct
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        //supported construct
    }

    @Override
    public void visit(Column tableColumn) {
        //supported construct
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        //supported construct
    }

    @Override
    public void visit(WhenClause whenClause) {
        //supported construct
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        //supported construct
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        //supported construct
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        //supported construct
    }

    @Override
    public void visit(Concat concat) {
        //supported construct
    }

    @Override
    public void visit(Matches matches) {
        //supported construct
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        //supported construct
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        //supported construct
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        //supported construct
    }

    @Override
    public void visit(CastExpression cast) {
        //supported construct
    }

    @Override
    public void visit(Modulo modulo) {
        //supported construct
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        //supported construct
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        //supported construct
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        //supported construct
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        //supported construct
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        //supported construct
    }

    @Override
    public void visit(Table tableName) {
        //supported construct
    }

    @Override
    public void visit(SubSelect subSelect) {
        //supported construct
    }

    @Override
    public void visit(SubJoin subjoin) {
        //supported construct
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        //supported construct
    }

    @Override
    public void visit(ValuesList valuesList) {
        //supported construct
    }

    @Override
    public void visit(ExpressionList expressionList) {
        //supported construct
    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        //supported construct
    }

    @Override
    public void visit(AllColumns allColumns) {
        //supported construct
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        //supported construct
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        //supported construct
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        //supported construct
    }

    @Override
    public void visit(SetOperationList setOpList) {
        //supported construct
    }

    @Override
    public void visit(WithItem withItem) {
        //supported construct
    }

    @Override
    public void visit(Select select) {
        //supported construct
    }

    @Override
    public void visit(Delete delete) {
        //supported construct
    }

    @Override
    public void visit(Update update) {
        //supported construct
    }

    @Override
    public void visit(Insert insert) {
        //supported construct
    }

    @Override
    public void visit(Replace replace) {
        //supported construct
    }

    @Override
    public void visit(Drop drop) {
        //supported construct
    }

    @Override
    public void visit(Truncate truncate) {
        //supported construct
    }

    @Override
    public void visit(CreateIndex createIndex) {
        //supported construct
    }

    @Override
    public void visit(CreateTable createTable) {
        //supported construct
    }

    @Override
    public void visit(CreateView createView) {
        //supported construct
    }

    @Override
    public void visit(Alter alter) {
        //supported construct
    }

    @Override
    public void visit(Statements stmts) {
        //supported construct
    }

    @Override
    public void visit(Describe describe) {
        //supported construct
    }

    @Override
    public void visit(ShowTables showTables) {
        //supported construct
    }
}
