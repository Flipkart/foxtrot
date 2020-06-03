package com.flipkart.foxtrot.core.funnel.constants;

public final class FunnelAttributes {

    public static final String ID = "id";
    public static final String FIELD_VS_VALUES = "fieldVsValues";
    public static final String START_PERCENTAGE = "startPercentage";
    public static final String END_PERCENTAGE = "endPercentage";
    public static final String EVENT_ATTRIBUTES = "eventAttributes";
    public static final String DELETED = "deleted";
    public static final String FUNNEL_STATUS = "funnelStatus";
    public static final String APPROVAL_DATE = "approvedAt";

    private FunnelAttributes() {
        throw new IllegalStateException("Utility class");
    }


}
