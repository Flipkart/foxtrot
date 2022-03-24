package com.flipkart.foxtrot.core.funnel.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FunnelUserDetails implements Serializable {

    private static final long serialVersionUID = 345822790929661673L;
    private String userId;
    private String token;

}
