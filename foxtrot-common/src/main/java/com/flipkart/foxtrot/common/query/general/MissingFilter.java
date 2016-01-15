package com.flipkart.foxtrot.common.query.general;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;

/**
 * Created by avanish.pandey on 23/11/15.
 */
public class MissingFilter extends Filter{


	public MissingFilter() {
		super(FilterOperator.missing);
	}

	public MissingFilter(String field) {
		super(FilterOperator.missing, field);
	}

	@Override
	public void accept(FilterVisitor visitor) throws Exception {
		visitor.visit(this);
	}


}
