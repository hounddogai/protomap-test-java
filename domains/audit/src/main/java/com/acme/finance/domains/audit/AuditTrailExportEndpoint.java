package com.acme.finance.domains.audit;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class AuditTrailExportEndpoint extends AbstractAuditTrailExportEndpoint {
    public AuditTrailExportEndpoint(AuditTrailExportApplication application) {
        super(application);
    }
}
