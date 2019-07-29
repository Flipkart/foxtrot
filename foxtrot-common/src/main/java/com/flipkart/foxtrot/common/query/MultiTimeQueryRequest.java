package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.ActionRequest;
<<<<<<< HEAD
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.Opcodes;
import io.dropwizard.util.Duration;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Getter
@Setter
=======
import com.flipkart.foxtrot.common.Opcodes;
import io.dropwizard.util.Duration;
import lombok.Data;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.validation.constraints.NotNull;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Data
>>>>>>> phonepe-develop
public class MultiTimeQueryRequest extends ActionRequest {

    private int sampleSize;

    @NotNull
<<<<<<< HEAD
    private transient Duration skipDuration;
=======
    private Duration skipDuration;

    @NotNull
    private ActionRequest actionRequest;
>>>>>>> phonepe-develop

    public MultiTimeQueryRequest() {
        super(Opcodes.MULTI_TIME_QUERY);
    }

<<<<<<< HEAD

    public MultiTimeQueryRequest(int sampleSize, Duration skipDuration,
            ActionRequest actionRequest) {
=======
    public MultiTimeQueryRequest(int sampleSize, Duration skipDuration, ActionRequest actionRequest) {
>>>>>>> phonepe-develop
        super(Opcodes.MULTI_TIME_QUERY);
        this.sampleSize = sampleSize;
        this.skipDuration = skipDuration;
        this.actionRequest = actionRequest;
    }

<<<<<<< HEAD
    @NotNull
    private ActionRequest actionRequest;

    public <T> T accept(ActionRequestVisitor<T> visitor) {
        return visitor.visit(this);
    }

=======
>>>>>>> phonepe-develop
    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("sampleSize", sampleSize)
                .append("interval", skipDuration.toMilliseconds())
                .append("actionRequest", actionRequest.toString())
                .toString();
    }
}
