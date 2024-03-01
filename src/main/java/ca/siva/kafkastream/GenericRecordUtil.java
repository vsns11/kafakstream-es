package ca.siva.kafkastream;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class GenericRecordUtil {

    public static Map<String, Object> convertGenericRecordToMap(GenericRecord record) {
        Map<String, Object> map = new HashMap<>();
        record.getSchema().getFields().forEach(field -> {
            Object value = record.get(field.name());
            map.put(field.name(), convertValue(value));
        });
        return map;
    }

    public static Object convertValue(Object value) {
        if (value instanceof Utf8 || value instanceof String) {
            return value.toString();
        } else if (value instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) value;
            byte[] bytesArray = new byte[buffer.remaining()];
            buffer.get(bytesArray);
            return new String(bytesArray, StandardCharsets.UTF_8);
        } else if (value instanceof byte[]) {
            return new String((byte[]) value, StandardCharsets.UTF_8);
        } else if (value instanceof GenericRecord) {
            return convertGenericRecordToMap((GenericRecord) value);
        } else if (value instanceof List<?>) {
            return ((List<?>) value).stream().map(GenericRecordUtil::convertValue).collect(Collectors.toList());
        } else if (value instanceof Map<?, ?>) {
            Map<?, ?> originalMap = (Map<?, ?>) value;
            Map<String, Object> convertedMap = new HashMap<>();
            originalMap.forEach((key, val) -> convertedMap.put(key.toString(), convertValue(val)));
            return convertedMap;
        }
        return value;
    }

    public static Object findNestedValue(GenericRecord record, String fieldName) {
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            if (field.name().equals(fieldName)) {
                return value;
            } else if (value instanceof GenericRecord) {
                Object nestedValue = findNestedValue((GenericRecord) value, fieldName);
                if (nestedValue != null) {
                    return nestedValue;
                }
            } else if (value instanceof List<?>) {
                for (Object element : (List<?>) value) {
                    if (element instanceof GenericRecord) {
                        Object nestedValue = findNestedValue((GenericRecord) element, fieldName);
                        if (nestedValue != null) {
                            return nestedValue;
                        }
                    }
                }
            }
        }
        return null;
    }
}