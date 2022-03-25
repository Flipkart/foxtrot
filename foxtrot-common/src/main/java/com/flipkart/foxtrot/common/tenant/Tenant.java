package com.flipkart.foxtrot.common.tenant;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.dropwizard.validation.ValidationMethod;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tenant implements Serializable {


    @JsonIgnore
    private static final long serialVersionUID = -3644862623500592817L;

    @NotNull
    @NotEmpty
    String tenantName;

    @NotNull
    @NotEmpty
    String[] emailIds;

    @Builder
    public Tenant(String tenantName,
                  String[] emailIds) {
        this.tenantName = tenantName;
        this.emailIds = emailIds;
    }


    @ValidationMethod(message = "Invalid email id")
    boolean isValid() {
        Pattern VALID_EMAIL_ADDRESS_REGEX = Pattern.compile("^[A-Z0-9._%+-]+@(.+)$", Pattern.CASE_INSENSITIVE);
        return Arrays.stream(emailIds)
                .allMatch(emailId -> VALID_EMAIL_ADDRESS_REGEX.matcher(emailId)
                        .matches());
    }


}
