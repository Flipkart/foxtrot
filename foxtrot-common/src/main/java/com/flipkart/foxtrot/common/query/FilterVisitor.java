package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:20 PM
 */
public abstract class FilterVisitor {

    public abstract void visit(BetweenFilter betweenFilter) throws Exception;

    public abstract void visit(EqualsFilter equalsFilter) throws Exception;

    public abstract void visit(NotEqualsFilter notEqualsFilter) throws Exception;

    public abstract void visit(ContainsFilter stringContainsFilterElement) throws Exception;

    public abstract void visit(GreaterThanFilter greaterThanFilter) throws Exception;

    public abstract void visit(GreaterEqualFilter greaterEqualFilter) throws Exception;

    public abstract void visit(LessThanFilter lessThanFilter) throws Exception;

    public abstract void visit(LessEqualFilter lessEqualFilter) throws Exception;
}
