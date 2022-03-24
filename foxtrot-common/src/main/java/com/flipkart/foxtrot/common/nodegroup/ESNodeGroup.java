package com.flipkart.foxtrot.common.nodegroup;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.common.nodegroup.visitors.ESNodeGroupVisitor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.SortedSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 Only one VACANT ES node group can exist in cluster
 Only one ALLOCATED ES node group with table allocation = COMMON can exist in cluster
 */
@Data
@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "status")
@JsonSubTypes({@JsonSubTypes.Type(name = "ALLOCATED", value = AllocatedESNodeGroup.class),
        @JsonSubTypes.Type(name = "VACANT", value = VacantESNodeGroup.class)})
public abstract class ESNodeGroup {

    private String groupName;

    private AllocationStatus status;

    private SortedSet<String> nodePatterns;

    public ESNodeGroup(String groupName,
                       AllocationStatus status,
                       SortedSet<String> nodePatterns) {
        this.groupName = groupName;
        this.status = status;
        this.nodePatterns = nodePatterns;
    }

    public ESNodeGroup(AllocationStatus status) {
        this.status = status;
    }

    @JsonIgnore
    public static Function<String, Pattern> nodeRegexPattern() {
        return nodePattern -> Pattern.compile(nodePattern.replace("*", "(.*)"));
    }

    public abstract <T> T accept(ESNodeGroupVisitor<T> visitor);

    @JsonIgnore
    public boolean isAnyNodePatternOverlappingWith(ESNodeGroup group) {
        return isAnyNodePatternOverlapping(group, this) || isAnyNodePatternOverlapping(this, group);
    }

    @JsonIgnore
    private boolean isAnyNodePatternOverlapping(ESNodeGroup group1,
                                                ESNodeGroup group2) {
        return group1.getNodePatterns()
                .stream()
                .map(nodeRegexPattern())
                .anyMatch(pattern -> group2.getNodePatterns()
                        .stream()
                        .anyMatch(nodePattern1 -> {
                            Matcher matcher = pattern.matcher(nodePattern1.replaceAll("\\*", ""));
                            return matcher.matches();
                        }));
    }

    public enum AllocationStatus {
        ALLOCATED,
        VACANT
    }

}
