package com.acme.finance.domains.compliance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class AttestationValidationEndpoint extends AbstractAttestationValidationEndpoint {
    public AttestationValidationEndpoint(AttestationValidationApplication application) {
        super(application);
    }
}
