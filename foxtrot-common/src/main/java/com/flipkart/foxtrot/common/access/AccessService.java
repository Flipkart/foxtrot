package com.flipkart.foxtrot.common.access;

import com.flipkart.foxtrot.common.ActionRequest;
import com.phonepe.gandalf.models.user.UserDetails;

import javax.validation.Valid;

/***
 Created by mudit.g on Apr, 2019
 ***/
public interface AccessService {

    boolean hasAccess(@Valid final ActionRequest request, UserDetails userDetails);
}
