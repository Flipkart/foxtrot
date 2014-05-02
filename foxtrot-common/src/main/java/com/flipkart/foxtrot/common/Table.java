package com.flipkart.foxtrot.common;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.Serializable;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:51 PM
 */
public class Table implements Serializable {
    @NotNull
    @NotEmpty
    private String name;

    @Min(1)
    @Max(60)
    private int ttl;

    public Table() {
    }

    public Table(String name, int ttl) {
        this.name = name;
        this.ttl = ttl;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("ttl", ttl)
                .toString();
    }
}
