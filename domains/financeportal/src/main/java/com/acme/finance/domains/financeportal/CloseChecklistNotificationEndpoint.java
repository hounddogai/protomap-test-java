package com.acme.finance.domains.financeportal;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class CloseChecklistNotificationEndpoint extends AbstractCloseChecklistNotificationEndpoint {
    public CloseChecklistNotificationEndpoint(CloseChecklistNotificationApplication application) {
        super(application);
    }
}
