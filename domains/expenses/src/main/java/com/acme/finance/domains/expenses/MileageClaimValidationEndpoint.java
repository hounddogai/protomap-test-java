package com.acme.finance.domains.expenses;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class MileageClaimValidationEndpoint extends AbstractMileageClaimValidationEndpoint {
    public MileageClaimValidationEndpoint(MileageClaimValidationApplication application) {
        super(application);
    }
}
