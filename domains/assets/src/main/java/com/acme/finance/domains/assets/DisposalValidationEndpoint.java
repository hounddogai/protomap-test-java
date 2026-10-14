package com.acme.finance.domains.assets;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class DisposalValidationEndpoint extends AbstractDisposalValidationEndpoint {
    public DisposalValidationEndpoint(DisposalValidationApplication application) {
        super(application);
    }
}
