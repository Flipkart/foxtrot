package com.flipkart.foxtrot.core.funnel.model;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.foxtrot.core.funnel.model.enums.FunnelStatus;
import lombok.Builder;
import lombok.Data;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.collections.CollectionUtils.nullAndEmptySafeValueList;
import static com.collections.CollectionUtils.nullSafeMap;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class Funnel {

    private String id;

    private String documentId;

    @NotBlank
    private String name;

    private String desc;

    private int percentage;

    private int startPercentage;

    private int endPercentage;

    // stores a mapping of field vs list of values for which this funnel is applicable
    @NotNull
    private Map<String, List<String>> fieldVsValues;

    @NotNull
    private List<EventAttributes> eventAttributes;

    private String table;

    private String creatorEmailId;

    @NotBlank
    private String approverEmailId;

    private FunnelStatus funnelStatus;

    private boolean deleted;

    private Date createdAt;

    private Date approvedAt;

    private Date deletedAt;


    /**
     * Funnels are similar if
     * 1. all fieldVsValues entries are same
     * 2. all eventAttributes are same
     *
     * @param funnel {@link Funnel}
     * @return boolean
     */
    public boolean isSimilar(Funnel funnel) {
        boolean similarFunnel = false;
        if (eventAttributes.size() == funnel.getEventAttributes().size()
                && fieldVsValues.size() == funnel.getFieldVsValues().size()) {
            similarFunnel = true;
            for (Map.Entry<String, List<String>> field : nullSafeMap(funnel.getFieldVsValues()).entrySet()) {
                if (!CollectionUtils.listEquals(field.getValue(), fieldVsValues.get(field.getKey()))) {
                    similarFunnel = false;
                    break;
                }
            }
            for (EventAttributes existingEventAttribute : nullAndEmptySafeValueList(funnel.getEventAttributes())) {
                if (!eventAttributes.contains(existingEventAttribute)) {
                    similarFunnel = false;
                    break;
                }
            }
        }
        return similarFunnel;
    }
}
