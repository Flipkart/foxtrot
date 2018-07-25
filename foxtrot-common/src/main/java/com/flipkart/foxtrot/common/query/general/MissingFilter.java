package com.flipkart.foxtrot.common.query.general;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Created by avanish.pandey on 23/11/15.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MissingFilter extends Filter{
	public MissingFilter() {
		super(FilterOperator.missing);
	}

	@Builder
	public MissingFilter(String field) {
		super(FilterOperator.missing, field);
	}

	@Override
	public<T> T accept(FilterVisitor<T> visitor) throws Exception {
		return visitor.visit(this);
	}


}
