package com.acme.finance.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/** Real grpc-java stubs use this channel; field assertions examine the messages at the transport boundary. */
public final class ProbeSupport {
    private ProbeSupport() {}

    @SuppressWarnings("unchecked")
    public static <T extends Message> T sample(T prototype) {
        return (T) fill(prototype.newBuilderForType(), 6).build();
    }

    private static Message.Builder fill(Message.Builder builder, int remaining) {
        if (remaining == 0 || builder.getDescriptorForType().getFullName().startsWith("google.protobuf.")) {
            return builder;
        }
        var selected = new HashSet<String>();
        for (var field : builder.getDescriptorForType().getFields()) {
            // Import streams start with a header, but record frames exercise their repeated field fan-out.
            if (field.getContainingOneof() != null && field.getContainingOneof().getName().equals("chunk")
                    && !field.getName().equals("record")) {
                continue;
            }
            if (field.getContainingOneof() != null && !selected.add(field.getContainingOneof().getName())) {
                continue;
            }
            Object value = switch (field.getJavaType()) {
                case STRING -> "sample-" + field.getFullName();
                case INT -> 7;
                case LONG -> 7L;
                case DOUBLE -> 1.25;
                case FLOAT -> 1.25F;
                case BOOLEAN -> true;
                case BYTE_STRING -> ByteString.copyFromUtf8("snapshot");
                case ENUM -> field.getEnumType().getValues().get(1);
                case MESSAGE -> fill(builder.newBuilderForField(field), remaining - 1).build();
            };
            if (field.isMapField()) {
                var entry = builder.newBuilderForField(field);
                entry.setField(field.getMessageType().findFieldByName("key"), "primary");
                var valueField = field.getMessageType().findFieldByName("value");
                entry.setField(valueField, ((Message) value).getField(valueField));
                builder.addRepeatedField(field, entry.build());
            } else if (field.isRepeated()) {
                builder.addRepeatedField(field, value);
                builder.addRepeatedField(field, value);
            } else {
                builder.setField(field, value);
            }
        }
        return builder;
    }

    public static Object value(Message root, String... path) {
        Message current = root;
        for (int index = 0; index < path.length; index++) {
            FieldDescriptor field = current.getDescriptorForType().findFieldByName(path[index]);
            if (field == null) { throw new AssertionError("Unknown field " + path[index]); }
            Object value = current.getField(field);
            if (field.isMapField()) {
                Map<String, Object> entries = new HashMap<>();
                for (Object item : (List<?>) value) {
                    Message entry = (Message) item;
                    entries.put((String) entry.getField(field.getMessageType().findFieldByName("key")),
                            entry.getField(field.getMessageType().findFieldByName("value")));
                }
                if (index == path.length - 1) { return entries; }
                var descriptor = field.getMessageType().findFieldByName("value").getMessageType();
                current = (Message) entries.getOrDefault("primary", DynamicMessage.getDefaultInstance(descriptor));
            } else if (index == path.length - 1) {
                return value;
            } else if (field.isRepeated()) {
                List<?> values = (List<?>) value;
                current = values.isEmpty() ? DynamicMessage.getDefaultInstance(field.getMessageType())
                        : (Message) values.getFirst();
            } else {
                current = (Message) value;
            }
        }
        return current;
    }

    public static void assertValue(Object source, Object target) {
        if (source instanceof Number left && target instanceof Number right) {
            assertEquals(left.doubleValue(), right.doubleValue());
        } else if (source instanceof String && target instanceof List<?>) {
            assertEquals(List.of(source), target);
        } else {
            assertEquals(source, target);
        }
    }

    public static final class RecordingChannel extends Channel {
        private final Map<String, List<Message>> requests = new HashMap<>();
        private final Map<String, Message> responses = new HashMap<>();
        private final List<Runnable> held = new ArrayList<>();
        private boolean holding;

        /** Keeps the replies of the calls made from now on until {@link #release()}, as a slow server would. */
        public void hold() { holding = true; }

        /** Sends the replies kept, and those of the calls made from now on at once. */
        public void release() {
            holding = false;
            held.forEach(Runnable::run);
            held.clear();
        }

        @Override
        public String authority() { return "fixture.test"; }

        public Message request(String method) {
            List<Message> values = requests.getOrDefault(method, List.of());
            assertFalse(values.isEmpty(), "Missing call " + method);
            return values.getFirst();
        }

        public Message response(String method) { return responses.get(method); }

        @Override
        @SuppressWarnings("unchecked")
        public <Req, Res> ClientCall<Req, Res> newCall(MethodDescriptor<Req, Res> descriptor, CallOptions options) {
            return new ClientCall<>() {
                private Listener<Res> listener;
                public void start(Listener<Res> listener, Metadata headers) { this.listener = listener; }
                public void request(int count) {}
                public void cancel(String message, Throwable cause) {
                    listener.onClose(Status.CANCELLED.withCause(cause), new Metadata());
                }
                public void sendMessage(Req request) {
                    requests.computeIfAbsent(descriptor.getFullMethodName(), ignored -> new ArrayList<>())
                            .add((Message) request);
                }
                public void halfClose() {
                    Res prototype = descriptor.getResponseMarshaller().parse(new ByteArrayInputStream(new byte[0]));
                    Message response = sample((Message) prototype);
                    responses.put(descriptor.getFullMethodName(), response);
                    Runnable reply = () -> {
                        listener.onMessage((Res) response);
                        listener.onClose(Status.OK, new Metadata());
                    };
                    if (holding) {
                        held.add(reply);
                    } else {
                        reply.run();
                    }
                }
            };
        }
    }
}
