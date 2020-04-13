package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * Created by rishabh.goyal on 18/12/15.
 */
@Getter
public class DocumentMissingException extends FoxtrotException {

    private final String table;
    private final List<String> ids;

    protected DocumentMissingException(String table, List<String> ids) {
        super(ErrorCode.DOCUMENT_NOT_FOUND);
        this.table = table;
        this.ids = ids;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("table", this.table);
        map.put("ids", ids);
        return map;
    }
}
