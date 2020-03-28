package com.flipkart.foxtrot.core.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;

/**
 *
 */
@Value
public class User {
    public static final User DEFAULT
            = new User("__DEFAULT__", EnumSet.allOf(FoxtrotRole.class), Collections.emptySet(), new Date(), new Date());

    @NotNull
    @NotEmpty
    String id;
    @NotNull
    @NotEmpty
    Set<FoxtrotRole> roles;
    Set<String> tables;
    Date created;
    Date updated;

    public User(
            @JsonProperty("id") String id,
            @JsonProperty("roles") Set<FoxtrotRole> roles,
            @JsonProperty("tables") Set<String> tables,
            @JsonProperty("created") Date created,
            @JsonProperty("updated") Date updated) {
        this.id = id;
        this.roles = roles;
        this.tables = tables;
        this.created = created;
        this.updated = updated;
    }
}
