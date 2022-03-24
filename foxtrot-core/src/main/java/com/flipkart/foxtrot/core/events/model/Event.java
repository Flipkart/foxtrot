package com.flipkart.foxtrot.core.events.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event<T> {

    @NotNull
    @NotEmpty
    private String app;

    @NotNull
    @NotEmpty
    private String eventType;

    @NotNull
    @NotEmpty
    private String id;

    @NotEmpty
    @NotNull
    private String groupingKey = "";

    private String farmId;

    private String partitionKey;

    private String eventSchemaVersion = "";

    private Date time = new Date();

    @NotNull
    private T eventData;
}
