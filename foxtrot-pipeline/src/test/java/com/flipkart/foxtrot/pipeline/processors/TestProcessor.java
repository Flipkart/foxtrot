package com.flipkart.foxtrot.pipeline.processors;

import com.fasterxml.jackson.databind.node.TextNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.pipeline.ProcessorMarker;
import com.jayway.jsonpath.JsonPath;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@Setter
@ProcessorMarker(name = "TEST", processorClass = TestProcessorDefinition.class)
@NoArgsConstructor
@AllArgsConstructor
public class TestProcessor implements Processor<TestProcessorDefinition> {

    private TestProcessorDefinition processorDefinition;

    @Override
    public void handle(Document document) {
        val context = JsonPath.parse(document.getData());
        val lat = (TextNode) context.read(processorDefinition.getMatchField());
        context.set(processorDefinition.getMatchField(), "test");
    }
}
