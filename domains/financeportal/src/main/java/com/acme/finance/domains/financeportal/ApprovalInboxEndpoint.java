package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.ApprovalInboxServiceGrpc;
import com.acme.financeportal.v1.BatchGetApprovalInboxesRequest;
import com.acme.financeportal.v1.BatchGetApprovalInboxesResponse;
import com.acme.financeportal.v1.CloneApprovalInboxRequest;
import com.acme.financeportal.v1.CloneApprovalInboxResponse;
import com.acme.financeportal.v1.CreateApprovalInboxRequest;
import com.acme.financeportal.v1.CreateApprovalInboxResponse;
import com.acme.financeportal.v1.DeleteApprovalInboxRequest;
import com.acme.financeportal.v1.DeleteApprovalInboxResponse;
import com.acme.financeportal.v1.GetApprovalInboxRequest;
import com.acme.financeportal.v1.GetApprovalInboxResponse;
import com.acme.financeportal.v1.ListApprovalInboxesRequest;
import com.acme.financeportal.v1.ListApprovalInboxesResponse;
import com.acme.financeportal.v1.SearchApprovalInboxesRequest;
import com.acme.financeportal.v1.SearchApprovalInboxesResponse;
import com.acme.financeportal.v1.UpdateApprovalInboxRequest;
import com.acme.financeportal.v1.UpdateApprovalInboxResponse;
import com.acme.financeportal.v1.WatchApprovalInboxesRequest;
import com.acme.financeportal.v1.WatchApprovalInboxesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages approval inboxes: creates, reads, changes, and lists them.
 */
@GrpcService
public class ApprovalInboxEndpoint extends ApprovalInboxServiceGrpc.ApprovalInboxServiceImplBase {
    protected final ApprovalInboxApplication application;

    public ApprovalInboxEndpoint(ApprovalInboxApplication application) {
        this.application = application;
    }

    /**
     * Creates an approval inbox.
     */
    @Override
    public void createApprovalInbox(CreateApprovalInboxRequest request, StreamObserver<CreateApprovalInboxResponse> observer) {
        RpcSupport.unary(observer, () -> application.createApprovalInbox(request));
    }

    /**
     * Returns an approval inbox.
     */
    @Override
    public void getApprovalInbox(GetApprovalInboxRequest request, StreamObserver<GetApprovalInboxResponse> observer) {
        RpcSupport.unary(observer, () -> application.getApprovalInbox(request));
    }

    /**
     * Changes an approval inbox.
     */
    @Override
    public void updateApprovalInbox(UpdateApprovalInboxRequest request, StreamObserver<UpdateApprovalInboxResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateApprovalInbox(request));
    }

    /**
     * Deletes an approval inbox.
     */
    @Override
    public void deleteApprovalInbox(DeleteApprovalInboxRequest request, StreamObserver<DeleteApprovalInboxResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteApprovalInbox(request));
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    @Override
    public void listApprovalInboxes(ListApprovalInboxesRequest request, StreamObserver<ListApprovalInboxesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listApprovalInboxes(request));
    }

    /**
     * Returns several approval inboxes at once.
     */
    @Override
    public void batchGetApprovalInboxes(BatchGetApprovalInboxesRequest request, StreamObserver<BatchGetApprovalInboxesResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetApprovalInboxes(request));
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    @Override
    public void searchApprovalInboxes(SearchApprovalInboxesRequest request, StreamObserver<SearchApprovalInboxesResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchApprovalInboxes(request));
    }

    /**
     * Streams the changes of approval inboxes as they happen.
     */
    @Override
    public void watchApprovalInboxes(WatchApprovalInboxesRequest request, StreamObserver<WatchApprovalInboxesResponse> observer) {
        try {
            var snapshot = application.watchApprovalInboxes(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Creates an approval inbox.
     */
    @Override
    public void cloneApprovalInbox(CloneApprovalInboxRequest request, StreamObserver<CloneApprovalInboxResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneApprovalInbox(request));
    }
}
