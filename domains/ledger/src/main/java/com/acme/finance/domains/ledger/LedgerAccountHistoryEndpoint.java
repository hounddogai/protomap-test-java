package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.CompareLedgerAccountVersionsRequest;
import com.acme.ledger.v1.CompareLedgerAccountVersionsResponse;
import com.acme.ledger.v1.GetLedgerAccountVersionRequest;
import com.acme.ledger.v1.GetLedgerAccountVersionResponse;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountVersionsRequest;
import com.acme.ledger.v1.ListLedgerAccountVersionsResponse;
import com.acme.ledger.v1.PruneLedgerAccountVersionsRequest;
import com.acme.ledger.v1.PruneLedgerAccountVersionsResponse;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.ledger.v1.RevertLedgerAccountRequest;
import com.acme.ledger.v1.RevertLedgerAccountResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps every version of a ledger account and restores earlier ones.
 */
@GrpcService
public class LedgerAccountHistoryEndpoint extends LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceImplBase {
    protected final LedgerAccountHistoryApplication application;

    public LedgerAccountHistoryEndpoint(LedgerAccountHistoryApplication application) {
        this.application = application;
    }

    /**
     * Records a ledger account.
     */
    @Override
    public void recordLedgerAccountVersion(RecordLedgerAccountVersionRequest request, StreamObserver<RecordLedgerAccountVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recordLedgerAccountVersion(request));
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    @Override
    public void listLedgerAccountVersions(ListLedgerAccountVersionsRequest request, StreamObserver<ListLedgerAccountVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listLedgerAccountVersions(request));
    }

    /**
     * Returns a ledger account.
     */
    @Override
    public void getLedgerAccountVersion(GetLedgerAccountVersionRequest request, StreamObserver<GetLedgerAccountVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLedgerAccountVersion(request));
    }

    /**
     * Compares a ledger account.
     */
    @Override
    public void compareLedgerAccountVersions(CompareLedgerAccountVersionsRequest request, StreamObserver<CompareLedgerAccountVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareLedgerAccountVersions(request));
    }

    /**
     * Reverts a ledger account.
     */
    @Override
    public void revertLedgerAccount(RevertLedgerAccountRequest request, StreamObserver<RevertLedgerAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.revertLedgerAccount(request));
    }

    /**
     * Annotates a ledger account.
     */
    @Override
    public void annotateLedgerAccountVersion(AnnotateLedgerAccountVersionRequest request, StreamObserver<AnnotateLedgerAccountVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.annotateLedgerAccountVersion(request));
    }

    /**
     * Deletes a ledger account.
     */
    @Override
    public void pruneLedgerAccountVersions(PruneLedgerAccountVersionsRequest request, StreamObserver<PruneLedgerAccountVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pruneLedgerAccountVersions(request));
    }
}
