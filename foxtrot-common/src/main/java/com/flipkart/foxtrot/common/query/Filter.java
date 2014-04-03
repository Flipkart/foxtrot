package com.flipkart.foxtrot.common.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.general.NotEqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.*;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;

import javax.validation.constraints.NotNull;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 2:09 AM
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "operator")
@JsonSubTypes({
        //Numeric
        @JsonSubTypes.Type(value = GreaterEqualFilter.class, name=FilterOperator.greater_equal),
        @JsonSubTypes.Type(value = GreaterThanFilter.class, name=FilterOperator.greater_than),
        @JsonSubTypes.Type(value = LessEqualFilter.class, name=FilterOperator.less_equal),
        @JsonSubTypes.Type(value = LessThanFilter.class, name=FilterOperator.less_than),
        @JsonSubTypes.Type(value = BetweenFilter.class, name=FilterOperator.between),

        //General
        @JsonSubTypes.Type(value = EqualsFilter.class, name=FilterOperator.equals),
        @JsonSubTypes.Type(value = NotEqualsFilter.class, name=FilterOperator.not_equals),

        //String
        @JsonSubTypes.Type(value = ContainsFilter.class, name=FilterOperator.contains),

})

public abstract class Filter {
    @JsonIgnore
    private final String operator;

    @NotNull
    private String field;

    protected Filter(String operator) {
        this.operator = operator;
    }

    protected Filter(String operator, String field) {
        this.operator = operator;
        this.field = field;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOperator() {
        return operator;
    }

    public abstract void accept(FilterVisitor visitor) throws Exception;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Filter filter = (Filter) o;

        if (!field.equals(filter.field)) return false;
        if (!operator.equals(filter.operator)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = operator.hashCode();
        result = 31 * result + field.hashCode();
        return result;
    }
}
