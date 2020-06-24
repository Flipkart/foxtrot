package com.flipkart.foxtrot.core.funnel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
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
public class EventAttributes implements Serializable {

    private static final long serialVersionUID = -4212561989849321831L;

    private String eventType;
    private String category;

}
