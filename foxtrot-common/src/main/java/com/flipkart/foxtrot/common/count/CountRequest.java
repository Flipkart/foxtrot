package com.flipkart.foxtrot.common.count;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.Filter;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by rishabh.goyal on 02/11/14.
 */
public class CountRequest extends ActionRequest {

    @NotNull
    @NotEmpty
    private String table;

    private String field;

    private boolean isDistinct = false;


    public CountRequest() {
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    @Override
    public String toString() {
        return "CountRequest{" +
                "table='" + table + '\'' +
                ", field='" + field + '\'' +
                ", isDistinct=" + isDistinct +
                ", filters=" + getFilters() +
                '}';
    }
}
