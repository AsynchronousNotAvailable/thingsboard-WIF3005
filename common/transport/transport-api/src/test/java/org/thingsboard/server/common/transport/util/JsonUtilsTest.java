package org.thingsboard.server.common.transport.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class JsonUtilsTest {

    @Test
    void testParseInteger() {
        JsonElement element = JsonUtils.parse(10);
        assertEquals("10", element.getAsString());
    }

    @Test
    void testParseString() {
        JsonElement element = JsonUtils.parse("{\"a\":1}");
        assertTrue(element.isJsonObject());
        assertEquals(1, element.getAsJsonObject().get("a").getAsInt());
    }

    @Test
    void testParseBase64String() {
        String base64 = "SGVsbG8="; // "Hello" base64
        JsonElement element = JsonUtils.parse(base64);
        assertEquals(base64, element.getAsString());
    }

    @Test
    void testConvertToJsonObject() {
        Map<String, Object> values = new HashMap<>();
        values.put("int", 5);
        values.put("str", "hello");
        values.put("bool", true);

        JsonObject json = JsonUtils.convertToJsonObject(values);

        assertEquals(5, json.get("int").getAsInt());
        assertEquals("hello", json.get("str").getAsString());
        assertTrue(json.get("bool").getAsBoolean());
    }

    @Test
    void testIsBase64() {
        assertTrue(JsonUtils.isBase64("SGVsbG8=")); // valid base64
        assertFalse(JsonUtils.isBase64("Hello")); // not base64
    }

    @Test
    void testGetJsonObject() {
        TransportProtos.KeyValueProto kv1 = TransportProtos.KeyValueProto.newBuilder()
                .setKey("temp")
                .setType(TransportProtos.KeyValueType.LONG_V)
                .setLongV(25)
                .build();

        TransportProtos.KeyValueProto kv2 = TransportProtos.KeyValueProto.newBuilder()
                .setKey("active")
                .setType(TransportProtos.KeyValueType.BOOLEAN_V)
                .setBoolV(true)
                .build();

        List<TransportProtos.KeyValueProto> list = Arrays.asList(kv1, kv2);

        JsonObject json = JsonUtils.getJsonObject(list);

        assertEquals(25, json.get("temp").getAsLong());
        assertTrue(json.get("active").getAsBoolean());
    }
}
