package com.flipkart.foxtrot.server.auth.io;

import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import lombok.Value;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 *
 */
@Value
public class CreateUserRequest {
    @NotNull
    @NotEmpty
    String id;
    @NotNull
    @NotEmpty
    Set<FoxtrotRole> roles;
    Set<String> tables;
    boolean system;
}
