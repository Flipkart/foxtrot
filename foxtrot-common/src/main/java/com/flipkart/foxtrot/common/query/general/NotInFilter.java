package com.flipkart.foxtrot.common.query.general;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterOperator;
import com.flipkart.foxtrot.common.query.FilterVisitor;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: rishabh.goyal
 * Date: 02/09/14
 * Time: 11:46 AM
 * To change this template use File | Settings | File Templates.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class NotInFilter extends Filter {

    @NotNull
    @NotEmpty
    @Size(min = 1, max = 10000)
    private List<Object> values;

    public NotInFilter() {
        super(FilterOperator.not_in);
    }

    @Builder
    public NotInFilter(String field, List<Object> values) {
        super(FilterOperator.not_in, field);
        this.values = values;
    }

    @Override
    public<T> T accept(FilterVisitor<T> visitor) throws Exception {
        return visitor.visit(this);
    }

    @Override
    public Set<String> validate() {
        Set<String> validationErrors = super.validate();
        if (CollectionUtils.isNullOrEmpty(values)) {
            validationErrors.add("at least one value needs to be provided for field");
        }
        return validationErrors;
    }
}
