package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import io.dropwizard.util.Duration;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Getter
@Setter
@AllArgsConstructor
public class MultiTimeQueryRequest extends ActionRequest {

    private int sampleSize;

    @NotNull
    private transient Duration skipDuration;

    @NotNull
    private ActionRequest actionRequest;

    public <T> T accept(ActionRequestVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("sampleSize", sampleSize)
                .append("interval", skipDuration.toMilliseconds())
                .append("actionRequest", actionRequest.toString())
                .toString();
    }
}
