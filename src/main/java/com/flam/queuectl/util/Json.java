package com.flam.queuectl.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A tiny, dependency-free JSON parser and writer.
 *
 * WHY hand-roll JSON instead of pulling in Gson/Jackson: the only JSON this
 * tool ever parses is the flat job object passed to `enqueue`
 * ({"id": "...", "command": "...", ...}) — strings, integers, booleans, null.
 * A 150-line recursive-descent parser removes an entire dependency (and its
 * transitive tree) from the build, which keeps the project a single jar +
 * the SQLite driver. That is a deliberate trade-off I can defend: less
 * surface area, trivially auditable, and sufficient for the input contract.
 * If the input format ever grew nested structures, swapping in Jackson is a
 * one-class change because the rest of the code only sees Map<String,Object>.
 *
 * The parser is strict: trailing garbage, unquoted keys, or malformed
 * escapes throw JsonException with a position, so `enqueue` can fail with a
 * clear error instead of half-accepting bad input ("invalid commands fail
 * gracefully" applies to invalid JSON too).
 */
public final class Json {

    public static class JsonException extends RuntimeException {
        public JsonException(String msg) { super(msg); }
    }

    private final String src;
    private int pos = 0;

    private Json(String src) { this.src = src; }

    /** Parse a single flat-ish JSON object. Nested objects/arrays are rejected. */
    public static Map<String, Object> parseObject(String text) {
        Json p = new Json(text);
        p.skipWs();
        Map<String, Object> obj = p.readObject();
        p.skipWs();
        if (p.pos != p.src.length()) {
            throw new JsonException("Unexpected trailing characters at position " + p.pos);
        }
        return obj;
    }

    private Map<String, Object> readObject() {
        expect('{');
        Map<String, Object> out = new LinkedHashMap<>();
        skipWs();
        if (peek() == '}') { pos++; return out; }
        while (true) {
            skipWs();
            if (peek() != '"') throw new JsonException("Expected quoted key at position " + pos);
            String key = readString();
            skipWs();
            expect(':');
            skipWs();
            out.put(key, readValue());
            skipWs();
            char c = next();
            if (c == '}') return out;
            if (c != ',') throw new JsonException("Expected ',' or '}' at position " + (pos - 1));
        }
    }

    private Object readValue() {
        char c = peek();
        if (c == '"') return readString();
        if (c == '{' || c == '[') {
            throw new JsonException("Nested objects/arrays are not supported in a job definition (position " + pos + ")");
        }
        if (c == 't' && src.startsWith("true", pos))  { pos += 4; return Boolean.TRUE; }
        if (c == 'f' && src.startsWith("false", pos)) { pos += 5; return Boolean.FALSE; }
        if (c == 'n' && src.startsWith("null", pos))  { pos += 4; return null; }
        return readNumber();
    }

    private String readString() {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (pos >= src.length()) throw new JsonException("Unterminated string");
            char c = src.charAt(pos++);
            if (c == '"') return sb.toString();
            if (c == '\\') {
                char e = src.charAt(pos++);
                switch (e) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'n' -> sb.append('\n');
                    case 't' -> sb.append('\t');
                    case 'r' -> sb.append('\r');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'u' -> {
                        sb.append((char) Integer.parseInt(src.substring(pos, pos + 4), 16));
                        pos += 4;
                    }
                    default -> throw new JsonException("Bad escape '\\" + e + "' at position " + (pos - 1));
                }
            } else {
                sb.append(c);
            }
        }
    }

    private Object readNumber() {
        int start = pos;
        while (pos < src.length() && "-+.eE0123456789".indexOf(src.charAt(pos)) >= 0) pos++;
        String num = src.substring(start, pos);
        if (num.isEmpty()) throw new JsonException("Expected a value at position " + start);
        try {
            if (num.contains(".") || num.contains("e") || num.contains("E")) return Double.parseDouble(num);
            return Long.parseLong(num);
        } catch (NumberFormatException e) {
            throw new JsonException("Invalid number '" + num + "' at position " + start);
        }
    }

    private void skipWs() { while (pos < src.length() && Character.isWhitespace(src.charAt(pos))) pos++; }
    private char peek() {
        if (pos >= src.length()) throw new JsonException("Unexpected end of input");
        return src.charAt(pos);
    }
    private char next() { char c = peek(); pos++; return c; }
    private void expect(char c) {
        if (next() != c) throw new JsonException("Expected '" + c + "' at position " + (pos - 1));
    }

    // ---------- writing ----------

    /** Serialise a flat map to a single-line JSON object (for CLI output). */
    public static String write(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append('"').append(escape(e.getKey())).append("\": ");
            Object v = e.getValue();
            if (v == null) sb.append("null");
            else if (v instanceof Number || v instanceof Boolean) sb.append(v);
            else sb.append('"').append(escape(v.toString())).append('"');
        }
        return sb.append('}').toString();
    }

    public static String escape(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\t' -> sb.append("\\t");
                case '\r' -> sb.append("\\r");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        return sb.toString();
    }
}
