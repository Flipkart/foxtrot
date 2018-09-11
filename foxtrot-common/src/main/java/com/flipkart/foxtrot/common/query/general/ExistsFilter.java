package com.flipkart.foxtrot.common.query.general;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Created by rishabh.goyal on 03/11/14.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ExistsFilter extends Filter {

    public ExistsFilter() {
        super(FilterOperator.exists);
    }

    @Builder
    public ExistsFilter(String field) {
        super(FilterOperator.exists, field);
    }

    @Override
    public<T> T accept(FilterVisitor<T> visitor) throws Exception {
        return visitor.visit(this);
    }
}
