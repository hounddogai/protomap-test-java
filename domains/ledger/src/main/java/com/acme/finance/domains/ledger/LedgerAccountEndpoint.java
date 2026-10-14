package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.CreateLedgerAccountRequest;
import com.acme.ledger.v1.CreateLedgerAccountResponse;
import com.acme.ledger.v1.GetLedgerAccountRequest;
import com.acme.ledger.v1.GetLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.ledger.v1.SearchLedgerAccountsRequest;
import com.acme.ledger.v1.SearchLedgerAccountsResponse;
import com.acme.ledger.v1.WatchLedgerAccountsRequest;
import com.acme.ledger.v1.WatchLedgerAccountsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages ledger accounts: creates, reads, changes, and lists them.
 */
@GrpcService
public class LedgerAccountEndpoint extends LedgerAccountServiceGrpc.LedgerAccountServiceImplBase {
    protected final LedgerAccountApplication application;

    public LedgerAccountEndpoint(LedgerAccountApplication application) {
        this.application = application;
    }

    /**
     * Creates a ledger account.
     */
    @Override
    public void createLedgerAccount(CreateLedgerAccountRequest request, StreamObserver<CreateLedgerAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.createLedgerAccount(request));
    }

    /**
     * Returns a ledger account.
     */
    @Override
    public void getLedgerAccount(GetLedgerAccountRequest request, StreamObserver<GetLedgerAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLedgerAccount(request));
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    @Override
    public void listLedgerAccounts(ListLedgerAccountsRequest request, StreamObserver<ListLedgerAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listLedgerAccounts(request));
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    @Override
    public void searchLedgerAccounts(SearchLedgerAccountsRequest request, StreamObserver<SearchLedgerAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchLedgerAccounts(request));
    }

    /**
     * Streams the changes of ledger accounts as they happen.
     */
    @Override
    public void watchLedgerAccounts(WatchLedgerAccountsRequest request, StreamObserver<WatchLedgerAccountsResponse> observer) {
        try {
            var snapshot = application.watchLedgerAccounts(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Creates a ledger account.
     */
    @Override
    public void cloneLedgerAccount(CloneLedgerAccountRequest request, StreamObserver<CloneLedgerAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneLedgerAccount(request));
    }
}
