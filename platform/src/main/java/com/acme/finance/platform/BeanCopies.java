package com.acme.finance.platform;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/** Compatibility adapter for protobuf beans received from older finance integrations. */
public final class BeanCopies {
    private BeanCopies() {}

    public static <T extends Message> T copy(T source, Class<T> type) {
        Message.Builder target = source.newBuilderForType();
        for (FieldDescriptor field : source.getAllFields().keySet()) {
            String property = property(field.getName());
            String getter = "get" + property + (field.isMapField() ? "Map" : field.isRepeated() ? "List" : "");
            String setter = (field.isMapField() ? "putAll" : field.isRepeated() ? "addAll" : "set") + property;
            try {
                Method read = source.getClass().getMethod(getter);
                Class<?> argument = field.isMapField() ? Map.class : field.isRepeated() ? Iterable.class
                        : read.getReturnType();
                Method write = target.getClass().getMethod(setter, argument);
                write.invoke(target, read.invoke(source));
            } catch (ReflectiveOperationException failure) {
                Throwable cause = failure instanceof InvocationTargetException invocation
                        ? invocation.getTargetException() : failure;
                throw new IllegalArgumentException("Cannot copy property " + property, cause);
            }
        }
        return type.cast(target.build());
    }

    private static String property(String field) {
        StringBuilder value = new StringBuilder();
        boolean upper = true;
        for (char character : field.toCharArray()) {
            if (character == '_') {
                upper = true;
            } else {
                value.append(upper ? Character.toUpperCase(character) : character);
                upper = Character.isDigit(character);
            }
        }
        return value.toString();
    }
}
