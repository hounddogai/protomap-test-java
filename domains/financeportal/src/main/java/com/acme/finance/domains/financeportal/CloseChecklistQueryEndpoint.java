package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.BrowseCloseChecklistsRequest;
import com.acme.financeportal.v1.BrowseCloseChecklistsResponse;
import com.acme.financeportal.v1.CloseChecklistQueryServiceGrpc;
import com.acme.financeportal.v1.ExportCloseChecklistsRequest;
import com.acme.financeportal.v1.ExportCloseChecklistsResponse;
import com.acme.financeportal.v1.LookupCloseChecklistRequest;
import com.acme.financeportal.v1.LookupCloseChecklistResponse;
import com.acme.financeportal.v1.PeekCloseChecklistRequest;
import com.acme.financeportal.v1.PeekCloseChecklistResponse;
import com.acme.financeportal.v1.ResolveCloseChecklistsRequest;
import com.acme.financeportal.v1.ResolveCloseChecklistsResponse;
import com.acme.financeportal.v1.SuggestCloseChecklistsRequest;
import com.acme.financeportal.v1.SuggestCloseChecklistsResponse;
import com.acme.financeportal.v1.SummarizeCloseChecklistsRequest;
import com.acme.financeportal.v1.SummarizeCloseChecklistsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about close checklists, for screens and reports.
 */
@GrpcService
public class CloseChecklistQueryEndpoint extends CloseChecklistQueryServiceGrpc.CloseChecklistQueryServiceImplBase {
    protected final CloseChecklistQueryApplication application;

    public CloseChecklistQueryEndpoint(CloseChecklistQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a close checklist.
     */
    @Override
    public void lookupCloseChecklist(LookupCloseChecklistRequest request, StreamObserver<LookupCloseChecklistResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupCloseChecklist(request));
    }

    /**
     * Lists close checklists a page at a time.
     */
    @Override
    public void browseCloseChecklists(BrowseCloseChecklistsRequest request, StreamObserver<BrowseCloseChecklistsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseCloseChecklists(request));
    }

    /**
     * Returns several close checklists at once.
     */
    @Override
    public void resolveCloseChecklists(ResolveCloseChecklistsRequest request, StreamObserver<ResolveCloseChecklistsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveCloseChecklists(request));
    }

    /**
     * Streams every matching close checklist.
     */
    @Override
    public void exportCloseChecklists(ExportCloseChecklistsRequest request, StreamObserver<ExportCloseChecklistsResponse> observer) {
        try {
            var snapshot = application.exportCloseChecklists(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a close checklist.
     */
    @Override
    public void summarizeCloseChecklists(SummarizeCloseChecklistsRequest request, StreamObserver<SummarizeCloseChecklistsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeCloseChecklists(request));
    }

    /**
     * Lists close checklists a page at a time.
     */
    @Override
    public void suggestCloseChecklists(SuggestCloseChecklistsRequest request, StreamObserver<SuggestCloseChecklistsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestCloseChecklists(request));
    }

    /**
     * Returns a close checklist.
     */
    @Override
    public void peekCloseChecklist(PeekCloseChecklistRequest request, StreamObserver<PeekCloseChecklistResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekCloseChecklist(request));
    }
}
