package com.acme.finance.domains.collections;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class CollectionCaseQueryEndpoint extends AbstractCollectionCaseQueryEndpoint {
    public CollectionCaseQueryEndpoint(CollectionCaseQueryApplication application) {
        super(application);
    }
}
