package com.acme.finance.domains.audit;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class AuditTrailQueryEndpoint extends AbstractAuditTrailQueryEndpoint {
    public AuditTrailQueryEndpoint(AuditTrailQueryApplication application) {
        super(application);
    }
}
