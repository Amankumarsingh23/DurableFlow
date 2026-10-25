package io.durableflow.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.api.ApiException;
import io.durableflow.workflow.definition.RegisterWorkflowDefinitionRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import java.io.*;
import java.util.*;

/** Local resource guard, not authentication/authorization. Bytes mean serialized UTF-8 JSON. */
@Component
public class ResourceLimits {
    public enum Payload { DEFINITION, WORKFLOW_INPUT, SIGNAL, TOOL_ARGUMENTS, SCHEDULE_INPUT }
    private final int httpBytes,definitionBytes,nodes,dependencies,inputBytes,signalBytes,toolBytes,scheduleBytes,depth,elements;
    private final ObjectMapper json=new ObjectMapper();
    public ResourceLimits(@Value("${durableflow.limits.http-body-bytes:1048576}") int httpBytes,
            @Value("${durableflow.limits.definition-bytes:262144}") int definitionBytes,
            @Value("${durableflow.limits.nodes:128}") int nodes,
            @Value("${durableflow.limits.dependencies-per-node:64}") int dependencies,
            @Value("${durableflow.limits.workflow-input-bytes:262144}") int inputBytes,
            @Value("${durableflow.limits.signal-bytes:32768}") int signalBytes,
            @Value("${durableflow.limits.tool-argument-bytes:8192}") int toolBytes,
            @Value("${durableflow.limits.schedule-input-bytes:131072}") int scheduleBytes,
            @Value("${durableflow.limits.json-depth:32}") int depth,
            @Value("${durableflow.limits.json-elements:10000}") int elements) {
        for(int value:List.of(httpBytes,definitionBytes,inputBytes,signalBytes,toolBytes,scheduleBytes))
            if(value<1 || value>8388608)throw new IllegalArgumentException("Payload limits must be 1..8388608 bytes");
        if(nodes<1 || nodes>1024 || dependencies<1 || dependencies>256 || depth<1 || depth>64 || elements<1 || elements>100000)
            throw new IllegalArgumentException("Invalid structural resource limits");
        this.httpBytes=httpBytes;this.definitionBytes=definitionBytes;this.nodes=nodes;this.dependencies=dependencies;
        this.inputBytes=inputBytes;this.signalBytes=signalBytes;this.toolBytes=toolBytes;this.scheduleBytes=scheduleBytes;this.depth=depth;this.elements=elements;
    }
    public static ResourceLimits defaults() {return new ResourceLimits(1048576,262144,128,64,262144,32768,8192,131072,32,10000);}
    public int httpBodyBytes() {return httpBytes;}
    public void definition(RegisterWorkflowDefinitionRequest request) {
        if(request==null || request.steps()==null)return; // Existing validation supplies the required-field error.
        if(request.steps().size()>nodes)throw exceeded("DEFINITION_NODES_LIMIT");
        for(var step:request.steps())if(step!=null) {
            if(step.dependsOn()!=null && step.dependsOn().size()>dependencies)throw exceeded("NODE_DEPENDENCIES_LIMIT");
            if(step.childInput()!=null)checkJson(Payload.WORKFLOW_INPUT,step.childInput());
        }
        serialized(Payload.DEFINITION,request,definitionBytes);
    }
    public void checkJson(Payload payload,Object value) {
        var pending=new ArrayDeque<Node>();pending.push(new Node(value,1));int visited=0;
        while(!pending.isEmpty()) {
            var node=pending.pop();if(++visited>elements)throw exceeded("JSON_ELEMENTS_LIMIT");
            if(node.depth()>depth)throw exceeded("JSON_DEPTH_LIMIT");
            if(node.value() instanceof Map<?,?> map) {
                if((long)visited+pending.size()+map.size()>elements)throw exceeded("JSON_ELEMENTS_LIMIT");
                for(var entry:map.entrySet()) {
                    if(!(entry.getKey() instanceof String))throw invalid();
                    pending.push(new Node(entry.getValue(),node.depth()+1));
                }
            } else if(node.value() instanceof Collection<?> values) {
                if((long)visited+pending.size()+values.size()>elements)throw exceeded("JSON_ELEMENTS_LIMIT");
                for(var item:values)pending.push(new Node(item,node.depth()+1));
            } else if(node.value() instanceof Double n && !Double.isFinite(n) || node.value() instanceof Float f && !Float.isFinite(f))throw invalid();
            else if(node.value()!=null && !(node.value() instanceof String) && !(node.value() instanceof Boolean) && !(node.value() instanceof Number))throw invalid();
        }
        int maximum=switch(payload) {case DEFINITION -> definitionBytes;case WORKFLOW_INPUT -> inputBytes;case SIGNAL -> signalBytes;case TOOL_ARGUMENTS -> toolBytes;case SCHEDULE_INPUT -> scheduleBytes;};
        serialized(payload,value,maximum);
    }
    private record Node(Object value,int depth) {}
    private void serialized(Payload payload,Object value,int maximum) {
        try {json.writeValue(new OutputStream() {
            private long count;
            @Override public void write(int b) throws IOException {add(1);}
            @Override public void write(byte[] b,int offset,int length) throws IOException {add(length);}
            private void add(int length) throws IOException {count+=length;if(count>maximum)throw new IOException("LIMIT");}
        },value);}catch(IOException failure) {throw exceeded(payload.name()+"_BYTES_LIMIT");}
    }
    private ApiException exceeded(String code) {return new ApiException(HttpStatus.valueOf(413),code);}
    private ApiException invalid() {return new ApiException(HttpStatus.BAD_REQUEST,"JSON_VALUES_REQUIRED");}
}
