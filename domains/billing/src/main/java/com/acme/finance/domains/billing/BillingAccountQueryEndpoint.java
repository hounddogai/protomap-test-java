package com.acme.finance.domains.billing;

import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.BrowseBillingAccountsRequest;
import com.acme.billing.v1.BrowseBillingAccountsResponse;
import com.acme.billing.v1.CountBillingAccountsRequest;
import com.acme.billing.v1.CountBillingAccountsResponse;
import com.acme.billing.v1.ExportBillingAccountsRequest;
import com.acme.billing.v1.ExportBillingAccountsResponse;
import com.acme.billing.v1.FindBillingAccountsRequest;
import com.acme.billing.v1.FindBillingAccountsResponse;
import com.acme.billing.v1.LookupBillingAccountRequest;
import com.acme.billing.v1.LookupBillingAccountResponse;
import com.acme.billing.v1.PeekBillingAccountRequest;
import com.acme.billing.v1.PeekBillingAccountResponse;
import com.acme.billing.v1.ResolveBillingAccountsRequest;
import com.acme.billing.v1.ResolveBillingAccountsResponse;
import com.acme.billing.v1.SuggestBillingAccountsRequest;
import com.acme.billing.v1.SuggestBillingAccountsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about billing accounts, for screens and reports.
 */
@GrpcService
public class BillingAccountQueryEndpoint extends BillingAccountQueryServiceGrpc.BillingAccountQueryServiceImplBase {
    protected final BillingAccountQueryApplication application;

    public BillingAccountQueryEndpoint(BillingAccountQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a billing account.
     */
    @Override
    public void lookupBillingAccount(LookupBillingAccountRequest request, StreamObserver<LookupBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupBillingAccount(request));
    }

    /**
     * Lists billing accounts a page at a time.
     */
    @Override
    public void browseBillingAccounts(BrowseBillingAccountsRequest request, StreamObserver<BrowseBillingAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseBillingAccounts(request));
    }

    /**
     * Lists billing accounts a page at a time.
     */
    @Override
    public void findBillingAccounts(FindBillingAccountsRequest request, StreamObserver<FindBillingAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findBillingAccounts(request));
    }

    /**
     * Counts billing accounts by status.
     */
    @Override
    public void countBillingAccounts(CountBillingAccountsRequest request, StreamObserver<CountBillingAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countBillingAccounts(request));
    }

    /**
     * Returns several billing accounts at once.
     */
    @Override
    public void resolveBillingAccounts(ResolveBillingAccountsRequest request, StreamObserver<ResolveBillingAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveBillingAccounts(request));
    }

    /**
     * Streams every matching billing account.
     */
    @Override
    public void exportBillingAccounts(ExportBillingAccountsRequest request, StreamObserver<ExportBillingAccountsResponse> observer) {
        try {
            var snapshot = application.exportBillingAccounts(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists billing accounts a page at a time.
     */
    @Override
    public void suggestBillingAccounts(SuggestBillingAccountsRequest request, StreamObserver<SuggestBillingAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestBillingAccounts(request));
    }

    /**
     * Returns a billing account.
     */
    @Override
    public void peekBillingAccount(PeekBillingAccountRequest request, StreamObserver<PeekBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekBillingAccount(request));
    }
}
