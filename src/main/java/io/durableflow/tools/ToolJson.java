package io.durableflow.tools;

import com.fasterxml.jackson.databind.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Bounded canonical JSON, shared by fingerprints and the simulated provider ledger. */
public final class ToolJson {
    private static final ObjectMapper JSON = new ObjectMapper();
    private ToolJson() {}
    public static String encode(Object value) {
        try { return JSON.writeValueAsString(value); }
        catch (Exception e) { throw new IllegalArgumentException("TOOL_INVALID_JSON"); }
    }
    @SuppressWarnings("unchecked") public static Map<String,Object> decode(String text) {
        try { return JSON.readValue(text, Map.class); }
        catch (Exception e) { throw new IllegalArgumentException("TOOL_INVALID_JSON"); }
    }
    public static Map<String,Object> object(Object value) {
        if (!(value instanceof Map<?,?> map) || map.keySet().stream().anyMatch(k -> !(k instanceof String)))
            throw new IllegalArgumentException("TOOL_OBJECT_REQUIRED");
        return decode(encode(map));
    }
    public static Map<String,Object> freeze(Map<String,Object> value) {
        var result = new TreeMap<String,Object>();
        value.forEach((k,v) -> result.put(k, freezeValue(v)));
        return Collections.unmodifiableMap(result);
    }
    private static Object freezeValue(Object value) {
        if (value instanceof Map<?,?>) return freeze(object(value));
        if (value instanceof List<?> list) return list.stream().map(ToolJson::freezeValue).toList();
        if (value instanceof Number n) return new BigDecimal(n.toString()).stripTrailingZeros();
        if (value instanceof String || value instanceof Boolean) return value;
        throw new IllegalArgumentException("TOOL_UNSUPPORTED_VALUE");
    }
    public static String fingerprint(String name, Map<String,Object> args) {
        String canonical = encode(freeze(Map.of("toolName",name,"arguments",freeze(args))));
        if (canonical.length()>8000) throw new IllegalArgumentException("TOOL_ARGUMENTS_TOO_LARGE");
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
