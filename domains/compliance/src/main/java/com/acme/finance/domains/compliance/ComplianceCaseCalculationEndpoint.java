package com.acme.finance.domains.compliance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ComplianceCaseCalculationEndpoint extends AbstractComplianceCaseCalculationEndpoint {
    public ComplianceCaseCalculationEndpoint(ComplianceCaseCalculationApplication application) {
        super(application);
    }
}
