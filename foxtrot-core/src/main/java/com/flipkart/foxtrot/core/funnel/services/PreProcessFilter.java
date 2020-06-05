package com.flipkart.foxtrot.core.funnel.services;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.general.AnyFilter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.ExistsFilter;
import com.flipkart.foxtrot.common.query.general.InFilter;
import com.flipkart.foxtrot.common.query.general.MissingFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotInFilter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterEqualFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.common.query.numeric.LessEqualFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.query.string.WildCardFilter;
import com.flipkart.foxtrot.core.funnel.model.request.FilterRequest;

/***
 Created by mudit.g on Feb, 2019
 ***/
public class PreProcessFilter implements FilterVisitor<Void> {

    public Void visit(BetweenFilter filter) {
        return null;
    }

    public Void visit(EqualsFilter filter) {
        if (filter.getValue() instanceof String) {
            String value = (String) filter.getValue();
            filter.setValue(value.toLowerCase());
        }
        return null;
    }

    public Void visit(NotEqualsFilter filter) {
        if (filter.getValue() instanceof String) {
            String value = (String) filter.getValue();
            filter.setValue(value.toLowerCase());
        }
        return null;
    }

    public Void visit(ContainsFilter filter) {
        filter.setValue(filter.getValue()
                .toLowerCase());
        return null;
    }

    public Void visit(GreaterThanFilter filter) {
        return null;
    }

    public Void visit(GreaterEqualFilter filter) {
        return null;
    }

    public Void visit(LessThanFilter filter) {
        return null;
    }

    public Void visit(LessEqualFilter filter) {
        return null;
    }

    public Void visit(AnyFilter filter) {
        return null;
    }

    public Void visit(InFilter filter) {
        for (Object value : CollectionUtils.nullAndEmptySafeValueList(filter.getValues())) {
            if (value instanceof String) {
                String lowerCaseValue = ((String) value).toLowerCase();
                filter.getValues()
                        .remove(value);
                filter.getValues()
                        .add(lowerCaseValue);
            }
        }
        return null;
    }

    public Void visit(NotInFilter notInFilter) {
        for (Object value : CollectionUtils.nullAndEmptySafeValueList(notInFilter.getValues())) {
            if (value instanceof String) {
                String lowerCaseValue = ((String) value).toLowerCase();
                notInFilter.getValues()
                        .remove(value);
                notInFilter.getValues()
                        .add(lowerCaseValue);
            }
        }
        return null;
    }

    public Void visit(ExistsFilter filter) {
        return null;
    }

    public Void visit(LastFilter filter) {
        return null;
    }

    public Void visit(MissingFilter filter) {
        return null;
    }

    public Void visit(WildCardFilter filter) {
        filter.setValue(filter.getValue()
                .toLowerCase());
        return null;
    }

    public void preProcess(FilterRequest filterRequest,
                           MappingService mappingService,
                           String funnelIndex) {
        String textType = "text";
        String keywordType = "keyword";
        for (Filter filter : CollectionUtils.nullAndEmptySafeValueList(filterRequest.getFilters())) {
            filter.accept(this);
            if (filter instanceof WildCardFilter) {
                WildCardFilter wildCardFilter = (WildCardFilter) filter;
                String fieldType = mappingService.getFieldType(wildCardFilter.getField(), funnelIndex);
                wildCardFilter.setField(mappingService.getAnalyzedFieldName(wildCardFilter.getField(), funnelIndex));
                if (!fieldType.equals(textType) && !fieldType.equals(keywordType)) {
                    EqualsFilter equalsFilter = new EqualsFilter(wildCardFilter.getField(), wildCardFilter.getValue());
                    filterRequest.getFilters()
                            .remove(wildCardFilter);
                    filterRequest.getFilters()
                            .add(equalsFilter);
                }
            }
        }
    }
}
