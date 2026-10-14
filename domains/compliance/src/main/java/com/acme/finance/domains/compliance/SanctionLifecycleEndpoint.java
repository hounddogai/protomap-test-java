package com.acme.finance.domains.compliance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class SanctionLifecycleEndpoint extends AbstractSanctionLifecycleEndpoint {
    public SanctionLifecycleEndpoint(SanctionLifecycleApplication application) {
        super(application);
    }
}
