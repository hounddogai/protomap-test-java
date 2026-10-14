package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.CountLedgerAccountsRequest;
import com.acme.ledger.v1.CountLedgerAccountsResponse;
import com.acme.ledger.v1.ExportLedgerAccountsRequest;
import com.acme.ledger.v1.ExportLedgerAccountsResponse;
import com.acme.ledger.v1.FindLedgerAccountsRequest;
import com.acme.ledger.v1.FindLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.LookupLedgerAccountRequest;
import com.acme.ledger.v1.LookupLedgerAccountResponse;
import com.acme.ledger.v1.PeekLedgerAccountRequest;
import com.acme.ledger.v1.PeekLedgerAccountResponse;
import com.acme.ledger.v1.ResolveLedgerAccountsRequest;
import com.acme.ledger.v1.ResolveLedgerAccountsResponse;
import com.acme.ledger.v1.SuggestLedgerAccountsRequest;
import com.acme.ledger.v1.SuggestLedgerAccountsResponse;
import com.acme.ledger.v1.SummarizeLedgerAccountsRequest;
import com.acme.ledger.v1.SummarizeLedgerAccountsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about ledger accounts, for screens and reports.
 */
@GrpcService
public class LedgerAccountQueryEndpoint extends LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceImplBase {
    protected final LedgerAccountQueryApplication application;

    public LedgerAccountQueryEndpoint(LedgerAccountQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a ledger account.
     */
    @Override
    public void lookupLedgerAccount(LookupLedgerAccountRequest request, StreamObserver<LookupLedgerAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupLedgerAccount(request));
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    @Override
    public void browseLedgerAccounts(BrowseLedgerAccountsRequest request, StreamObserver<BrowseLedgerAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseLedgerAccounts(request));
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    @Override
    public void findLedgerAccounts(FindLedgerAccountsRequest request, StreamObserver<FindLedgerAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findLedgerAccounts(request));
    }

    /**
     * Counts ledger accounts by status.
     */
    @Override
    public void countLedgerAccounts(CountLedgerAccountsRequest request, StreamObserver<CountLedgerAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countLedgerAccounts(request));
    }

    /**
     * Returns several ledger accounts at once.
     */
    @Override
    public void resolveLedgerAccounts(ResolveLedgerAccountsRequest request, StreamObserver<ResolveLedgerAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveLedgerAccounts(request));
    }

    /**
     * Streams every matching ledger account.
     */
    @Override
    public void exportLedgerAccounts(ExportLedgerAccountsRequest request, StreamObserver<ExportLedgerAccountsResponse> observer) {
        try {
            var snapshot = application.exportLedgerAccounts(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a ledger account.
     */
    @Override
    public void summarizeLedgerAccounts(SummarizeLedgerAccountsRequest request, StreamObserver<SummarizeLedgerAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeLedgerAccounts(request));
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    @Override
    public void suggestLedgerAccounts(SuggestLedgerAccountsRequest request, StreamObserver<SuggestLedgerAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestLedgerAccounts(request));
    }

    /**
     * Returns a ledger account.
     */
    @Override
    public void peekLedgerAccount(PeekLedgerAccountRequest request, StreamObserver<PeekLedgerAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekLedgerAccount(request));
    }
}
