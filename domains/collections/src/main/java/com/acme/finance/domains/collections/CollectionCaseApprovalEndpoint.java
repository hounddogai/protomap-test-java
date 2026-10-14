package com.acme.finance.domains.collections;

import com.acme.collections.v1.ApproveCollectionCaseRequest;
import com.acme.collections.v1.ApproveCollectionCaseResponse;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.EscalateCollectionCaseRequest;
import com.acme.collections.v1.EscalateCollectionCaseResponse;
import com.acme.collections.v1.GetCollectionCaseApprovalStatusRequest;
import com.acme.collections.v1.GetCollectionCaseApprovalStatusResponse;
import com.acme.collections.v1.ListPendingCollectionCasesRequest;
import com.acme.collections.v1.ListPendingCollectionCasesResponse;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.collections.v1.SubmitCollectionCaseRequest;
import com.acme.collections.v1.SubmitCollectionCaseResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes collection cases through approval, with escalation and delegation.
 */
@GrpcService
public class CollectionCaseApprovalEndpoint extends CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceImplBase {
    protected final CollectionCaseApprovalApplication application;

    public CollectionCaseApprovalEndpoint(CollectionCaseApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a collection case.
     */
    @Override
    public void submitCollectionCase(SubmitCollectionCaseRequest request, StreamObserver<SubmitCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitCollectionCase(request));
    }

    /**
     * Approves a collection case.
     */
    @Override
    public void approveCollectionCase(ApproveCollectionCaseRequest request, StreamObserver<ApproveCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveCollectionCase(request));
    }

    /**
     * Rejects a collection case.
     */
    @Override
    public void rejectCollectionCase(RejectCollectionCaseRequest request, StreamObserver<RejectCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectCollectionCase(request));
    }

    /**
     * Escalates a collection case.
     */
    @Override
    public void escalateCollectionCase(EscalateCollectionCaseRequest request, StreamObserver<EscalateCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateCollectionCase(request));
    }

    /**
     * Lists collection cases a page at a time.
     */
    @Override
    public void listPendingCollectionCases(ListPendingCollectionCasesRequest request, StreamObserver<ListPendingCollectionCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingCollectionCases(request));
    }

    /**
     * Returns a collection case.
     */
    @Override
    public void getCollectionCaseApprovalStatus(GetCollectionCaseApprovalStatusRequest request, StreamObserver<GetCollectionCaseApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCollectionCaseApprovalStatus(request));
    }
}
