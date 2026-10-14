package com.acme.finance.domains.ledger;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class JournalEntryQueryEndpoint extends AbstractJournalEntryQueryEndpoint {
    public JournalEntryQueryEndpoint(JournalEntryQueryApplication application) {
        super(application);
    }
}
