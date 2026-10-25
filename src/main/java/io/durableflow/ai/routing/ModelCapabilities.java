package io.durableflow.ai.routing;
public record ModelCapabilities(boolean structuredOutput,boolean toolUse,int maxContextChars) {
    public ModelCapabilities {if(maxContextChars<1 || maxContextChars>128000)throw new IllegalArgumentException("Invalid model context bound");}
}
