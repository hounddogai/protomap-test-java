package com.acme.finance.domains.collections;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class WriteOffValidationEndpoint extends AbstractWriteOffValidationEndpoint {
    public WriteOffValidationEndpoint(WriteOffValidationApplication application) {
        super(application);
    }
}
