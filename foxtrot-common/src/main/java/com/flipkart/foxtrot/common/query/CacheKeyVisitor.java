package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.*;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.query.string.WildCardFilter;

/***
 Created by nitish.goyal on 08/01/20
 ***/
public class CacheKeyVisitor<T> implements FilterVisitor {

    @Override
    public Integer visit(BetweenFilter betweenFilter) {
        return betweenFilter.hashCode();
    }

    @Override
    public Integer visit(EqualsFilter equalsFilter) {
        return equalsFilter.hashCode();
    }

    @Override
    public Integer visit(NotEqualsFilter notEqualsFilter) {
        return notEqualsFilter.hashCode();
    }

    @Override
    public Integer visit(ContainsFilter stringContainsFilterElement) {
        return stringContainsFilterElement.hashCode();
    }

    @Override
    public Integer visit(GreaterThanFilter greaterThanFilter) {
        return greaterThanFilter.hashCode();
    }

    @Override
    public Integer visit(GreaterEqualFilter greaterEqualFilter) {
        return greaterEqualFilter.hashCode();
    }

    @Override
    public Integer visit(LessThanFilter lessThanFilter) {
        return lessThanFilter.hashCode();
    }

    @Override
    public Integer visit(LessEqualFilter lessEqualFilter) {
        return lessEqualFilter.hashCode();
    }

    @Override
    public Integer visit(AnyFilter anyFilter) {
        return anyFilter.hashCode();
    }

    @Override
    public Integer visit(InFilter inFilter) {
        return inFilter.hashCode();
    }

    @Override
    public Integer visit(NotInFilter inFilter) {
        return inFilter.hashCode();
    }

    @Override
    public Integer visit(ExistsFilter existsFilter) {
        return existsFilter.hashCode();
    }

    @Override
    public Integer visit(LastFilter lastFilter) {
        if(lastFilter.isCachedResultsAccepted()) {
            int result = lastFilter.getOperator().hashCode();
            result = 31 * result + lastFilter.getField().hashCode();
            result = 31 * result + lastFilter.getRoundingMode().hashCode();
            result = 31 * result + lastFilter.getDuration().hashCode();

            long currentTime = lastFilter.getCurrentTime();
            String field = lastFilter.getField();
            if(!field.equals("_timestamp")) {
                result = result * 31 + (currentTime == 0
                                        ? 43
                                        : Long.valueOf(currentTime).hashCode());
            }
            else {
                result = result * 31 + Long.valueOf(currentTime / (long) 30000).hashCode();
            }
            return result;
        }
        else {
            return lastFilter.hashCode();
        }
    }

    @Override
    public Integer visit(MissingFilter missingFilter) {
        return missingFilter.hashCode();
    }

    @Override
    public Integer visit(WildCardFilter wildCardFilter) {
        return wildCardFilter.hashCode();
    }
}


