package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.ApproveSupplierRatingRequest;
import com.acme.suppliers.v1.ApproveSupplierRatingResponse;
import com.acme.suppliers.v1.CommentSupplierRatingApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierRatingApprovalResponse;
import com.acme.suppliers.v1.DelegateSupplierRatingApprovalRequest;
import com.acme.suppliers.v1.DelegateSupplierRatingApprovalResponse;
import com.acme.suppliers.v1.EscalateSupplierRatingRequest;
import com.acme.suppliers.v1.EscalateSupplierRatingResponse;
import com.acme.suppliers.v1.GetSupplierRatingApprovalStatusRequest;
import com.acme.suppliers.v1.GetSupplierRatingApprovalStatusResponse;
import com.acme.suppliers.v1.ListPendingSupplierRatingsRequest;
import com.acme.suppliers.v1.ListPendingSupplierRatingsResponse;
import com.acme.suppliers.v1.RecallSupplierRatingRequest;
import com.acme.suppliers.v1.RecallSupplierRatingResponse;
import com.acme.suppliers.v1.RejectSupplierRatingRequest;
import com.acme.suppliers.v1.RejectSupplierRatingResponse;
import com.acme.suppliers.v1.SubmitSupplierRatingRequest;
import com.acme.suppliers.v1.SubmitSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes supplier ratings through approval, with escalation and delegation.
 */
@GrpcService
public class SupplierRatingApprovalEndpoint extends SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceImplBase {
    protected final SupplierRatingApprovalApplication application;

    public SupplierRatingApprovalEndpoint(SupplierRatingApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a supplier rating.
     */
    @Override
    public void submitSupplierRating(SubmitSupplierRatingRequest request, StreamObserver<SubmitSupplierRatingResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitSupplierRating(request));
    }

    /**
     * Approves a supplier rating.
     */
    @Override
    public void approveSupplierRating(ApproveSupplierRatingRequest request, StreamObserver<ApproveSupplierRatingResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveSupplierRating(request));
    }

    /**
     * Rejects a supplier rating.
     */
    @Override
    public void rejectSupplierRating(RejectSupplierRatingRequest request, StreamObserver<RejectSupplierRatingResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectSupplierRating(request));
    }

    /**
     * Escalates a supplier rating.
     */
    @Override
    public void escalateSupplierRating(EscalateSupplierRatingRequest request, StreamObserver<EscalateSupplierRatingResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateSupplierRating(request));
    }

    /**
     * Lists supplier ratings a page at a time.
     */
    @Override
    public void listPendingSupplierRatings(ListPendingSupplierRatingsRequest request, StreamObserver<ListPendingSupplierRatingsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingSupplierRatings(request));
    }

    /**
     * Delegates a supplier rating.
     */
    @Override
    public void delegateSupplierRatingApproval(DelegateSupplierRatingApprovalRequest request, StreamObserver<DelegateSupplierRatingApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateSupplierRatingApproval(request));
    }

    /**
     * Recalls a supplier rating.
     */
    @Override
    public void recallSupplierRating(RecallSupplierRatingRequest request, StreamObserver<RecallSupplierRatingResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallSupplierRating(request));
    }

    /**
     * Returns a supplier rating.
     */
    @Override
    public void getSupplierRatingApprovalStatus(GetSupplierRatingApprovalStatusRequest request, StreamObserver<GetSupplierRatingApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getSupplierRatingApprovalStatus(request));
    }

    /**
     * Comments a supplier rating.
     */
    @Override
    public void commentSupplierRatingApproval(CommentSupplierRatingApprovalRequest request, StreamObserver<CommentSupplierRatingApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentSupplierRatingApproval(request));
    }
}
