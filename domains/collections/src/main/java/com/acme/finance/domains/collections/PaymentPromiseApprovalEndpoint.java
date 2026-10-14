package com.acme.finance.domains.collections;

import com.acme.collections.v1.ApprovePaymentPromiseRequest;
import com.acme.collections.v1.ApprovePaymentPromiseResponse;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.EscalatePaymentPromiseRequest;
import com.acme.collections.v1.EscalatePaymentPromiseResponse;
import com.acme.collections.v1.GetPaymentPromiseApprovalStatusRequest;
import com.acme.collections.v1.GetPaymentPromiseApprovalStatusResponse;
import com.acme.collections.v1.ListPendingPaymentPromisesRequest;
import com.acme.collections.v1.ListPendingPaymentPromisesResponse;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.RecallPaymentPromiseRequest;
import com.acme.collections.v1.RecallPaymentPromiseResponse;
import com.acme.collections.v1.RejectPaymentPromiseRequest;
import com.acme.collections.v1.RejectPaymentPromiseResponse;
import com.acme.collections.v1.SubmitPaymentPromiseRequest;
import com.acme.collections.v1.SubmitPaymentPromiseResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes payment promises through approval, with escalation and delegation.
 */
@GrpcService
public class PaymentPromiseApprovalEndpoint extends PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceImplBase {
    protected final PaymentPromiseApprovalApplication application;

    public PaymentPromiseApprovalEndpoint(PaymentPromiseApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a payment promise.
     */
    @Override
    public void submitPaymentPromise(SubmitPaymentPromiseRequest request, StreamObserver<SubmitPaymentPromiseResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitPaymentPromise(request));
    }

    /**
     * Approves a payment promise.
     */
    @Override
    public void approvePaymentPromise(ApprovePaymentPromiseRequest request, StreamObserver<ApprovePaymentPromiseResponse> observer) {
        RpcSupport.unary(observer, () -> application.approvePaymentPromise(request));
    }

    /**
     * Rejects a payment promise.
     */
    @Override
    public void rejectPaymentPromise(RejectPaymentPromiseRequest request, StreamObserver<RejectPaymentPromiseResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectPaymentPromise(request));
    }

    /**
     * Escalates a payment promise.
     */
    @Override
    public void escalatePaymentPromise(EscalatePaymentPromiseRequest request, StreamObserver<EscalatePaymentPromiseResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalatePaymentPromise(request));
    }

    /**
     * Lists payment promises a page at a time.
     */
    @Override
    public void listPendingPaymentPromises(ListPendingPaymentPromisesRequest request, StreamObserver<ListPendingPaymentPromisesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingPaymentPromises(request));
    }

    /**
     * Recalls a payment promise.
     */
    @Override
    public void recallPaymentPromise(RecallPaymentPromiseRequest request, StreamObserver<RecallPaymentPromiseResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallPaymentPromise(request));
    }

    /**
     * Returns a payment promise.
     */
    @Override
    public void getPaymentPromiseApprovalStatus(GetPaymentPromiseApprovalStatusRequest request, StreamObserver<GetPaymentPromiseApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPaymentPromiseApprovalStatus(request));
    }

    /**
     * Comments a payment promise.
     */
    @Override
    public void commentPaymentPromiseApproval(CommentPaymentPromiseApprovalRequest request, StreamObserver<CommentPaymentPromiseApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentPaymentPromiseApproval(request));
    }
}
