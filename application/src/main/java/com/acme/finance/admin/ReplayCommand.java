package com.acme.finance.admin;

import io.grpc.CallOptions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/** Operator replay utility: target, service, method, and a serialized protobuf request file. */
public final class ReplayCommand {
    private ReplayCommand() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 4) {
            throw new IllegalArgumentException("usage: ReplayCommand TARGET SERVICE METHOD REQUEST_FILE");
        }
        // The operator's own credential and tenant, which every server of the mesh needs.
        var credentials = new Metadata();
        credentials.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                "Bearer " + System.getenv().getOrDefault("ACME_TOKEN", ""));
        credentials.put(Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER),
                System.getenv().getOrDefault("ACME_TENANT", ""));
        var channel = ManagedChannelBuilder.forTarget(args[0]).usePlaintext()
                .intercept(MetadataUtils.newAttachHeadersInterceptor(credentials)).build();
        try {
            var descriptor = MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(args[1] + "/" + args[2])
                    .setRequestMarshaller(new WireBytes()).setResponseMarshaller(new WireBytes()).build();
            byte[] request = Files.readAllBytes(Path.of(args[3]));
            byte[] response = ClientCalls.blockingUnaryCall(channel, descriptor,
                    CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS), request);
            System.out.write(response);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static final class WireBytes implements MethodDescriptor.Marshaller<byte[]> {
        @Override
        public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream input) {
            try {
                return input.readAllBytes();
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }
    }
}
