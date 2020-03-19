package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.flipkart.foxtrot.core.exception.FoxtrotExceptions.ERROR_DELIMITER;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class BadRequestException extends FoxtrotException {

    private final String table;
    private final List<String> messages;

    protected BadRequestException(String table, List<String> messages) {
        super(ErrorCode.INVALID_REQUEST);
        this.table = table;
        this.messages = messages;
    }

    public BadRequestException(String table, Exception e) {
        super(ErrorCode.INVALID_REQUEST, e);
        this.table = table;
        this.messages = Collections.singletonList(e.getMessage());
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("table", this.table);
        map.put("message", StringUtils.join(messages, ERROR_DELIMITER));
        return map;
    }
}
