package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.ApprovalInboxQueryServiceGrpc;
import com.acme.financeportal.v1.BrowseApprovalInboxesRequest;
import com.acme.financeportal.v1.BrowseApprovalInboxesResponse;
import com.acme.financeportal.v1.CountApprovalInboxesRequest;
import com.acme.financeportal.v1.CountApprovalInboxesResponse;
import com.acme.financeportal.v1.ExportApprovalInboxesRequest;
import com.acme.financeportal.v1.ExportApprovalInboxesResponse;
import com.acme.financeportal.v1.FindApprovalInboxesRequest;
import com.acme.financeportal.v1.FindApprovalInboxesResponse;
import com.acme.financeportal.v1.LookupApprovalInboxRequest;
import com.acme.financeportal.v1.LookupApprovalInboxResponse;
import com.acme.financeportal.v1.PeekApprovalInboxRequest;
import com.acme.financeportal.v1.PeekApprovalInboxResponse;
import com.acme.financeportal.v1.ResolveApprovalInboxesRequest;
import com.acme.financeportal.v1.ResolveApprovalInboxesResponse;
import com.acme.financeportal.v1.SuggestApprovalInboxesRequest;
import com.acme.financeportal.v1.SuggestApprovalInboxesResponse;
import com.acme.financeportal.v1.SummarizeApprovalInboxesRequest;
import com.acme.financeportal.v1.SummarizeApprovalInboxesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about approval inboxes, for screens and reports.
 */
@GrpcService
public class ApprovalInboxQueryEndpoint extends ApprovalInboxQueryServiceGrpc.ApprovalInboxQueryServiceImplBase {
    protected final ApprovalInboxQueryApplication application;

    public ApprovalInboxQueryEndpoint(ApprovalInboxQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns an approval inbox.
     */
    @Override
    public void lookupApprovalInbox(LookupApprovalInboxRequest request, StreamObserver<LookupApprovalInboxResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupApprovalInbox(request));
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    @Override
    public void browseApprovalInboxes(BrowseApprovalInboxesRequest request, StreamObserver<BrowseApprovalInboxesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseApprovalInboxes(request));
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    @Override
    public void findApprovalInboxes(FindApprovalInboxesRequest request, StreamObserver<FindApprovalInboxesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findApprovalInboxes(request));
    }

    /**
     * Counts approval inboxes by status.
     */
    @Override
    public void countApprovalInboxes(CountApprovalInboxesRequest request, StreamObserver<CountApprovalInboxesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countApprovalInboxes(request));
    }

    /**
     * Returns several approval inboxes at once.
     */
    @Override
    public void resolveApprovalInboxes(ResolveApprovalInboxesRequest request, StreamObserver<ResolveApprovalInboxesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveApprovalInboxes(request));
    }

    /**
     * Streams every matching approval inbox.
     */
    @Override
    public void exportApprovalInboxes(ExportApprovalInboxesRequest request, StreamObserver<ExportApprovalInboxesResponse> observer) {
        try {
            var snapshot = application.exportApprovalInboxes(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes an approval inbox.
     */
    @Override
    public void summarizeApprovalInboxes(SummarizeApprovalInboxesRequest request, StreamObserver<SummarizeApprovalInboxesResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeApprovalInboxes(request));
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    @Override
    public void suggestApprovalInboxes(SuggestApprovalInboxesRequest request, StreamObserver<SuggestApprovalInboxesResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestApprovalInboxes(request));
    }

    /**
     * Returns an approval inbox.
     */
    @Override
    public void peekApprovalInbox(PeekApprovalInboxRequest request, StreamObserver<PeekApprovalInboxResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekApprovalInbox(request));
    }
}
