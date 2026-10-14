package com.acme.finance.domains.accounting;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ChartOfAccountsQueryEndpoint extends AbstractChartOfAccountsQueryEndpoint {
    public ChartOfAccountsQueryEndpoint(ChartOfAccountsQueryApplication application) {
        super(application);
    }
}
