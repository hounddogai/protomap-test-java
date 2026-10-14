package com.acme.finance.domains.ledger;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class PostingBatchHistoryEndpoint extends AbstractPostingBatchHistoryEndpoint {
    public PostingBatchHistoryEndpoint(PostingBatchHistoryApplication application) {
        super(application);
    }
}
