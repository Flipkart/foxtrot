package com.flipkart.foxtrot.core.funnel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Data
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode
public class EventAttributes {

    private String eventId;
    private String identifierId;

}
