package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.*;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.query.string.WildCardFilter;

/***
 Created by nitish.goyal on 08/01/20
 ***/
public class CacheKeyVisitor implements FilterVisitor<Integer> {

    private static final String TIMESTAMP_FIELD = "_timestamp";

    @Override
    public Integer visit(BetweenFilter betweenFilter) {
        if (betweenFilter.isCachedResultsAccepted()) {
            int result = betweenFilter.getOperator()
                    .hashCode();
            result = 31 * result + betweenFilter.getField()
                    .hashCode();
            result = 31 * result + (betweenFilter.isTemporal()
                    ? 79
                    : 97);
            if (!betweenFilter.getField()
                    .equals(TIMESTAMP_FIELD)) {
                result = result * 31 + (betweenFilter.getFrom() == null
                        ? 43
                        : betweenFilter.getFrom()
                        .hashCode());
                result = result * 31 + (betweenFilter.getTo() == null
                        ? 29
                        : betweenFilter.getTo()
                        .hashCode());
            } else {
                result = result * 31 + Long.valueOf(betweenFilter.getFrom()
                        .longValue() / (long) 30000)
                        .hashCode();
                result = result * 31 + Long.valueOf(betweenFilter.getTo()
                        .longValue() / (long) 30000)
                        .hashCode();
            }
            return result;
        } else {
            return betweenFilter.hashCode();
        }
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
        return this.numericBinaryFilterHashCode(greaterThanFilter);
    }

    @Override
    public Integer visit(GreaterEqualFilter greaterEqualFilter) {
        return this.numericBinaryFilterHashCode(greaterEqualFilter);
    }

    @Override
    public Integer visit(LessThanFilter lessThanFilter) {
        return this.numericBinaryFilterHashCode(lessThanFilter);
    }

    @Override
    public Integer visit(LessEqualFilter lessEqualFilter) {
        return this.numericBinaryFilterHashCode(lessEqualFilter);
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
        if (lastFilter.isCachedResultsAccepted()) {
            int result = lastFilter.getOperator()
                    .hashCode();
            result = 31 * result + lastFilter.getField()
                    .hashCode();
            result = 31 * result + lastFilter.getRoundingMode()
                    .hashCode();
            result = 31 * result + lastFilter.getDuration()
                    .hashCode();

            long currentTime = lastFilter.getCurrentTime();
            String field = lastFilter.getField();
            if (!field.equals(TIMESTAMP_FIELD)) {
                result = result * 31 + (currentTime == 0
                        ? 43
                        : Long.valueOf(currentTime)
                        .hashCode());
            } else {
                result = result * 31 + Long.valueOf(currentTime / (long) 30000)
                        .hashCode();
            }
            return result;
        } else {
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


    private Integer numericBinaryFilterHashCode(NumericBinaryFilter numericBinaryFilter) {
        if (numericBinaryFilter.isCachedResultsAccepted()) {
            int result = numericBinaryFilter.getOperator()
                    .hashCode();
            result = 31 * result + numericBinaryFilter.getField()
                    .hashCode();
            result = 31 * result + (numericBinaryFilter.isTemporal()
                    ? 79
                    : 97);
            if (!numericBinaryFilter.getField()
                    .equals(TIMESTAMP_FIELD)) {
                result = result * 31 + (numericBinaryFilter.getValue() == null
                        ? 43
                        : numericBinaryFilter.getValue()
                        .hashCode());
            } else {
                result = result * 31 + Long.valueOf(numericBinaryFilter.getValue()
                        .longValue() / (long) 30000)
                        .hashCode();
            }
            return result;
        } else {
            return numericBinaryFilter.hashCode();
        }
    }
}


