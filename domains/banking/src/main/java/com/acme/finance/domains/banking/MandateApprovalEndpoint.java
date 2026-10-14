package com.acme.finance.domains.banking;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class MandateApprovalEndpoint extends AbstractMandateApprovalEndpoint {
    public MandateApprovalEndpoint(MandateApprovalApplication application) {
        super(application);
    }
}
