package com.acme.finance.domains.ledger;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class LedgerAccountNotificationEndpoint extends AbstractLedgerAccountNotificationEndpoint {
    public LedgerAccountNotificationEndpoint(LedgerAccountNotificationApplication application) {
        super(application);
    }
}
