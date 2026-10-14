package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.ledger.v1.PruneJournalEntryVersionsRequest;
import com.acme.ledger.v1.PruneJournalEntryVersionsResponse;
import com.acme.ledger.v1.RecordJournalEntryVersionRequest;
import com.acme.ledger.v1.RecordJournalEntryVersionResponse;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps every version of a journal entry and restores earlier ones.
 */
@GrpcService
public class JournalEntryHistoryEndpoint extends JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceImplBase {
    protected final JournalEntryHistoryApplication application;

    public JournalEntryHistoryEndpoint(JournalEntryHistoryApplication application) {
        this.application = application;
    }

    /**
     * Records a journal entry.
     */
    @Override
    public void recordJournalEntryVersion(RecordJournalEntryVersionRequest request, StreamObserver<RecordJournalEntryVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recordJournalEntryVersion(request));
    }

    /**
     * Lists journal entries a page at a time.
     */
    @Override
    public void listJournalEntryVersions(ListJournalEntryVersionsRequest request, StreamObserver<ListJournalEntryVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listJournalEntryVersions(request));
    }

    /**
     * Compares a journal entry.
     */
    @Override
    public void compareJournalEntryVersions(CompareJournalEntryVersionsRequest request, StreamObserver<CompareJournalEntryVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareJournalEntryVersions(request));
    }

    /**
     * Reverts a journal entry.
     */
    @Override
    public void revertJournalEntry(RevertJournalEntryRequest request, StreamObserver<RevertJournalEntryResponse> observer) {
        RpcSupport.unary(observer, () -> application.revertJournalEntry(request));
    }

    /**
     * Annotates a journal entry.
     */
    @Override
    public void annotateJournalEntryVersion(AnnotateJournalEntryVersionRequest request, StreamObserver<AnnotateJournalEntryVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.annotateJournalEntryVersion(request));
    }

    /**
     * Deletes a journal entry.
     */
    @Override
    public void pruneJournalEntryVersions(PruneJournalEntryVersionsRequest request, StreamObserver<PruneJournalEntryVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pruneJournalEntryVersions(request));
    }
}
