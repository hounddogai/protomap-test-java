package com.acme.finance.domains.risk;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ExposureWorkflowEndpoint extends AbstractExposureWorkflowEndpoint {
    public ExposureWorkflowEndpoint(ExposureWorkflowApplication application) {
        super(application);
    }
}
