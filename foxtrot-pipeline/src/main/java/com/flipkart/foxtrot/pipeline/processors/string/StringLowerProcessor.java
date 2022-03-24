package com.flipkart.foxtrot.pipeline.processors.string;

import com.fasterxml.jackson.databind.node.TextNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.pipeline.ProcessorMarker;
import com.flipkart.foxtrot.pipeline.processors.Processor;
import com.jayway.jsonpath.JsonPath;
import io.appform.functionmetrics.MonitoredFunction;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@Setter
@ProcessorMarker(name = "STR::LOWER", processorClass = StringLowerProcessorDefinition.class)
@NoArgsConstructor
@AllArgsConstructor
public class StringLowerProcessor implements Processor<StringLowerProcessorDefinition> {

    private StringLowerProcessorDefinition processorDefinition;


    @Override
    @MonitoredFunction
    public void handle(Document document) {
        val context = JsonPath.parse(document.getData());
        val lat = (TextNode) context.read(processorDefinition.getMatchField());
        context.set(processorDefinition.getMatchField(), lat.asText()
                .toLowerCase());
    }

}
