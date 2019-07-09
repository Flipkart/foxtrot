package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Opcodes;
import io.dropwizard.util.Duration;
import lombok.Data;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.validation.constraints.NotNull;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Data
public class MultiTimeQueryRequest extends ActionRequest {

    private int sampleSize;

    @NotNull
    private Duration skipDuration;

    @NotNull
    private ActionRequest actionRequest;

    public MultiTimeQueryRequest() {
        super(Opcodes.MULTI_TIME_QUERY);
    }

    public MultiTimeQueryRequest(int sampleSize, Duration skipDuration, ActionRequest actionRequest) {
        super(Opcodes.MULTI_TIME_QUERY);
        this.sampleSize = sampleSize;
        this.skipDuration = skipDuration;
        this.actionRequest = actionRequest;
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
