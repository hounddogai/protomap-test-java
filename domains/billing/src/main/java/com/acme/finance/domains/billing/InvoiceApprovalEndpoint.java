package com.acme.finance.domains.billing;

import com.acme.billing.v1.ApproveInvoiceRequest;
import com.acme.billing.v1.ApproveInvoiceResponse;
import com.acme.billing.v1.CommentInvoiceApprovalRequest;
import com.acme.billing.v1.CommentInvoiceApprovalResponse;
import com.acme.billing.v1.DelegateInvoiceApprovalRequest;
import com.acme.billing.v1.DelegateInvoiceApprovalResponse;
import com.acme.billing.v1.EscalateInvoiceRequest;
import com.acme.billing.v1.EscalateInvoiceResponse;
import com.acme.billing.v1.GetInvoiceApprovalStatusRequest;
import com.acme.billing.v1.GetInvoiceApprovalStatusResponse;
import com.acme.billing.v1.InvoiceApprovalServiceGrpc;
import com.acme.billing.v1.RecallInvoiceRequest;
import com.acme.billing.v1.RecallInvoiceResponse;
import com.acme.billing.v1.RejectInvoiceRequest;
import com.acme.billing.v1.RejectInvoiceResponse;
import com.acme.billing.v1.SubmitInvoiceRequest;
import com.acme.billing.v1.SubmitInvoiceResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes invoices through approval, with escalation and delegation.
 */
@GrpcService
public class InvoiceApprovalEndpoint extends InvoiceApprovalServiceGrpc.InvoiceApprovalServiceImplBase {
    protected final InvoiceApprovalApplication application;

    public InvoiceApprovalEndpoint(InvoiceApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an invoice.
     */
    @Override
    public void submitInvoice(SubmitInvoiceRequest request, StreamObserver<SubmitInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitInvoice(request));
    }

    /**
     * Approves an invoice.
     */
    @Override
    public void approveInvoice(ApproveInvoiceRequest request, StreamObserver<ApproveInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveInvoice(request));
    }

    /**
     * Rejects an invoice.
     */
    @Override
    public void rejectInvoice(RejectInvoiceRequest request, StreamObserver<RejectInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectInvoice(request));
    }

    /**
     * Escalates an invoice.
     */
    @Override
    public void escalateInvoice(EscalateInvoiceRequest request, StreamObserver<EscalateInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateInvoice(request));
    }

    /**
     * Delegates an invoice.
     */
    @Override
    public void delegateInvoiceApproval(DelegateInvoiceApprovalRequest request, StreamObserver<DelegateInvoiceApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateInvoiceApproval(request));
    }

    /**
     * Recalls an invoice.
     */
    @Override
    public void recallInvoice(RecallInvoiceRequest request, StreamObserver<RecallInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallInvoice(request));
    }

    /**
     * Returns an invoice.
     */
    @Override
    public void getInvoiceApprovalStatus(GetInvoiceApprovalStatusRequest request, StreamObserver<GetInvoiceApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getInvoiceApprovalStatus(request));
    }

    /**
     * Comments an invoice.
     */
    @Override
    public void commentInvoiceApproval(CommentInvoiceApprovalRequest request, StreamObserver<CommentInvoiceApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentInvoiceApproval(request));
    }
}
