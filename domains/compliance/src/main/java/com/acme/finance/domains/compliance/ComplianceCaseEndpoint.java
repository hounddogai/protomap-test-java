package com.acme.finance.domains.compliance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ComplianceCaseEndpoint extends AbstractComplianceCaseEndpoint {
    public ComplianceCaseEndpoint(ComplianceCaseApplication application) {
        super(application);
    }
}
