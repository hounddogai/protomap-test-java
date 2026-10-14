package com.acme.finance.domains.audit;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class AuditEventValidationEndpoint extends AbstractAuditEventValidationEndpoint {
    public AuditEventValidationEndpoint(AuditEventValidationApplication application) {
        super(application);
    }
}
