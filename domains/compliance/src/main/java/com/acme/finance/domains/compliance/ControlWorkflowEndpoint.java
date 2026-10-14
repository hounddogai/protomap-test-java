package com.acme.finance.domains.compliance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ControlWorkflowEndpoint extends AbstractControlWorkflowEndpoint {
    public ControlWorkflowEndpoint(ControlWorkflowApplication application) {
        super(application);
    }
}
