package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class BadRequestException extends FoxtrotException {

    private final String entity;
    private final List<String> messages;

    protected BadRequestException(String entity,
                                  List<String> messages) {
        super(ErrorCode.INVALID_REQUEST);
        this.entity = entity;
        this.messages = messages;
    }

    public BadRequestException(String entity,
                               Exception e) {
        super(ErrorCode.INVALID_REQUEST, e);
        this.entity = entity;
        this.messages = Collections.singletonList(e.getMessage());
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("entity", this.entity);
        map.put("messages", this.messages);
        return map;
    }
}
