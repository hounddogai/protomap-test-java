package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.BrowseJournalEntriesRequest;
import com.acme.ledger.v1.BrowseJournalEntriesResponse;
import com.acme.ledger.v1.ExportJournalEntriesRequest;
import com.acme.ledger.v1.ExportJournalEntriesResponse;
import com.acme.ledger.v1.FindJournalEntriesRequest;
import com.acme.ledger.v1.FindJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.LookupJournalEntryRequest;
import com.acme.ledger.v1.LookupJournalEntryResponse;
import com.acme.ledger.v1.PeekJournalEntryRequest;
import com.acme.ledger.v1.PeekJournalEntryResponse;
import com.acme.ledger.v1.ResolveJournalEntriesRequest;
import com.acme.ledger.v1.ResolveJournalEntriesResponse;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import io.grpc.stub.StreamObserver;

/**
 * Answers read-only questions about journal entries, for screens and reports.
 */
public abstract class AbstractJournalEntryQueryEndpoint extends JournalEntryQueryServiceGrpc.JournalEntryQueryServiceImplBase {
    protected final JournalEntryQueryApplication application;

    protected AbstractJournalEntryQueryEndpoint(JournalEntryQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a journal entry.
     */
    @Override
    public void lookupJournalEntry(LookupJournalEntryRequest request, StreamObserver<LookupJournalEntryResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupJournalEntry(request));
    }

    /**
     * Lists journal entries a page at a time.
     */
    @Override
    public void browseJournalEntries(BrowseJournalEntriesRequest request, StreamObserver<BrowseJournalEntriesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseJournalEntries(request));
    }

    /**
     * Lists journal entries a page at a time.
     */
    @Override
    public void findJournalEntries(FindJournalEntriesRequest request, StreamObserver<FindJournalEntriesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findJournalEntries(request));
    }

    /**
     * Returns several journal entries at once.
     */
    @Override
    public void resolveJournalEntries(ResolveJournalEntriesRequest request, StreamObserver<ResolveJournalEntriesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveJournalEntries(request));
    }

    /**
     * Streams every matching journal entry.
     */
    @Override
    public void exportJournalEntries(ExportJournalEntriesRequest request, StreamObserver<ExportJournalEntriesResponse> observer) {
        try {
            var snapshot = application.exportJournalEntries(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists journal entries a page at a time.
     */
    @Override
    public void suggestJournalEntries(SuggestJournalEntriesRequest request, StreamObserver<SuggestJournalEntriesResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestJournalEntries(request));
    }

    /**
     * Returns a journal entry.
     */
    @Override
    public void peekJournalEntry(PeekJournalEntryRequest request, StreamObserver<PeekJournalEntryResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekJournalEntry(request));
    }
}
