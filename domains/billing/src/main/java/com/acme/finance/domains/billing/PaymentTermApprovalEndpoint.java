package com.acme.finance.domains.billing;

import com.acme.billing.v1.ApprovePaymentTermRequest;
import com.acme.billing.v1.ApprovePaymentTermResponse;
import com.acme.billing.v1.DelegatePaymentTermApprovalRequest;
import com.acme.billing.v1.DelegatePaymentTermApprovalResponse;
import com.acme.billing.v1.GetPaymentTermApprovalStatusRequest;
import com.acme.billing.v1.GetPaymentTermApprovalStatusResponse;
import com.acme.billing.v1.ListPendingPaymentTermsRequest;
import com.acme.billing.v1.ListPendingPaymentTermsResponse;
import com.acme.billing.v1.PaymentTermApprovalServiceGrpc;
import com.acme.billing.v1.RecallPaymentTermRequest;
import com.acme.billing.v1.RecallPaymentTermResponse;
import com.acme.billing.v1.SubmitPaymentTermRequest;
import com.acme.billing.v1.SubmitPaymentTermResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes payment terms through approval, with escalation and delegation.
 */
@GrpcService
public class PaymentTermApprovalEndpoint extends PaymentTermApprovalServiceGrpc.PaymentTermApprovalServiceImplBase {
    protected final PaymentTermApprovalApplication application;

    public PaymentTermApprovalEndpoint(PaymentTermApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a payment term.
     */
    @Override
    public void submitPaymentTerm(SubmitPaymentTermRequest request, StreamObserver<SubmitPaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitPaymentTerm(request));
    }

    /**
     * Approves a payment term.
     */
    @Override
    public void approvePaymentTerm(ApprovePaymentTermRequest request, StreamObserver<ApprovePaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.approvePaymentTerm(request));
    }

    /**
     * Lists payment terms a page at a time.
     */
    @Override
    public void listPendingPaymentTerms(ListPendingPaymentTermsRequest request, StreamObserver<ListPendingPaymentTermsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingPaymentTerms(request));
    }

    /**
     * Delegates a payment term.
     */
    @Override
    public void delegatePaymentTermApproval(DelegatePaymentTermApprovalRequest request, StreamObserver<DelegatePaymentTermApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegatePaymentTermApproval(request));
    }

    /**
     * Recalls a payment term.
     */
    @Override
    public void recallPaymentTerm(RecallPaymentTermRequest request, StreamObserver<RecallPaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallPaymentTerm(request));
    }

    /**
     * Returns a payment term.
     */
    @Override
    public void getPaymentTermApprovalStatus(GetPaymentTermApprovalStatusRequest request, StreamObserver<GetPaymentTermApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPaymentTermApprovalStatus(request));
    }
}
