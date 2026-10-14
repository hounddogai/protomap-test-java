package com.acme.finance.platform;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;

/** The audit interchange format preserves protobuf field names and sorts map keys. */
public final class JsonCodec {
    private JsonCodec() {}

    public static ByteString encode(Message value) {
        try {
            var registry = JsonFormat.TypeRegistry.newBuilder().add(value.getDescriptorForType()).build();
            return ByteString.copyFromUtf8(JsonFormat.printer().usingTypeRegistry(registry)
                    .preservingProtoFieldNames().sortingMapKeys()
                    .omittingInsignificantWhitespace().print(value));
        } catch (InvalidProtocolBufferException failure) {
            throw Status.INVALID_ARGUMENT.withDescription("unsupported JSON envelope").withCause(failure)
                    .asRuntimeException();
        }
    }

    public static <T extends Message> T decode(ByteString bytes, T prototype, Class<T> type) {
        Message.Builder target = prototype.newBuilderForType();
        try {
            var registry = JsonFormat.TypeRegistry.newBuilder().add(prototype.getDescriptorForType()).build();
            JsonFormat.parser().usingTypeRegistry(registry).merge(bytes.toStringUtf8(), target);
            return type.cast(target.build());
        } catch (InvalidProtocolBufferException failure) {
            throw Status.INVALID_ARGUMENT.withDescription("invalid snapshot").withCause(failure).asRuntimeException();
        }
    }

    public static <T extends Message> T roundTrip(T value, Class<T> type) {
        return decode(encode(value), value, type);
    }
}
