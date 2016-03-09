package com.flipkart.foxtrot.sql;

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

public class SqlElementVisitor implements StatementVisitor, SelectVisitor, FromItemVisitor, ItemsListVisitor,
                                            ExpressionVisitor, SelectItemVisitor, ExtendedSqlStatementVisitor {
    private void invalid(Object object) {
        throw new RuntimeException("Unsupported construct: " + object.getClass().getSimpleName());
    }

    @Override
    public void visit(NullValue nullValue) {
        invalid(nullValue);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(StringValue stringValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Addition addition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Division division) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Multiplication multiplication) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Subtraction subtraction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(AndExpression andExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OrExpression orExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Between between) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(InExpression inExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MinorThan minorThan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Column tableColumn) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(WhenClause whenClause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Concat concat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Matches matches) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(CastExpression cast) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Modulo modulo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Table tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(ExpressionList expressionList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(SubJoin subjoin) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(ValuesList valuesList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(AllColumns allColumns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(SetOperationList setOpList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(WithItem withItem) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Select select) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Delete delete) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Update update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Insert insert) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Replace replace) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Drop drop) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Truncate truncate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(CreateIndex createIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(CreateTable createTable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(CreateView createView) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Alter alter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Statements stmts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Describe describe) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(ShowTables showTables) {
        throw new UnsupportedOperationException();
    }
}
