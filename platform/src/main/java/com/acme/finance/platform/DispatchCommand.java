package com.acme.finance.platform;

import java.util.Objects;

/** An immutable command crossing the application and transport layers. */
public record DispatchCommand<T>(String destination, T payload) {
    public DispatchCommand {
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(payload, "payload");
    }
}
