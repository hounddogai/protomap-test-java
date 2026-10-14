package com.acme.finance.domains.audit;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class RetentionPolicyAdminEndpoint extends AbstractRetentionPolicyAdminEndpoint {
    public RetentionPolicyAdminEndpoint(RetentionPolicyAdminApplication application) {
        super(application);
    }
}
