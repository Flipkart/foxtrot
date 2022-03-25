package com.flipkart.foxtrot.common.query.string;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Set;

/***
 Created by mudit.g on Dec, 2018
 ***/
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class WildCardFilter extends Filter {

    private String value;

    public WildCardFilter() {
        super(FilterOperator.wildcard);
    }

    @Builder
    public WildCardFilter(String value) {
        super(FilterOperator.wildcard);
        this.value = value;
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Set<String> validate() {
        Set<String> validationErrors = super.validate();
        if (value == null) {
            validationErrors.add("filter value cannot be null");
        }
        return validationErrors;
    }
}
