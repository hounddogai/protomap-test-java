package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.ApproveInvoiceBatchRequest;
import com.acme.invoicing.v1.ApproveInvoiceBatchResponse;
import com.acme.invoicing.v1.CommentInvoiceBatchApprovalRequest;
import com.acme.invoicing.v1.CommentInvoiceBatchApprovalResponse;
import com.acme.invoicing.v1.DelegateInvoiceBatchApprovalRequest;
import com.acme.invoicing.v1.DelegateInvoiceBatchApprovalResponse;
import com.acme.invoicing.v1.EscalateInvoiceBatchRequest;
import com.acme.invoicing.v1.EscalateInvoiceBatchResponse;
import com.acme.invoicing.v1.InvoiceBatchApprovalServiceGrpc;
import com.acme.invoicing.v1.RecallInvoiceBatchRequest;
import com.acme.invoicing.v1.RecallInvoiceBatchResponse;
import com.acme.invoicing.v1.RejectInvoiceBatchRequest;
import com.acme.invoicing.v1.RejectInvoiceBatchResponse;
import com.acme.invoicing.v1.SubmitInvoiceBatchRequest;
import com.acme.invoicing.v1.SubmitInvoiceBatchResponse;
import io.grpc.stub.StreamObserver;

/**
 * Routes invoice batches through approval, with escalation and delegation.
 */
public abstract class AbstractInvoiceBatchApprovalEndpoint extends InvoiceBatchApprovalServiceGrpc.InvoiceBatchApprovalServiceImplBase {
    protected final InvoiceBatchApprovalApplication application;

    protected AbstractInvoiceBatchApprovalEndpoint(InvoiceBatchApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an invoice batch.
     */
    @Override
    public void submitInvoiceBatch(SubmitInvoiceBatchRequest request, StreamObserver<SubmitInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitInvoiceBatch(request));
    }

    /**
     * Approves an invoice batch.
     */
    @Override
    public void approveInvoiceBatch(ApproveInvoiceBatchRequest request, StreamObserver<ApproveInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveInvoiceBatch(request));
    }

    /**
     * Rejects an invoice batch.
     */
    @Override
    public void rejectInvoiceBatch(RejectInvoiceBatchRequest request, StreamObserver<RejectInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectInvoiceBatch(request));
    }

    /**
     * Escalates an invoice batch.
     */
    @Override
    public void escalateInvoiceBatch(EscalateInvoiceBatchRequest request, StreamObserver<EscalateInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateInvoiceBatch(request));
    }

    /**
     * Delegates an invoice batch.
     */
    @Override
    public void delegateInvoiceBatchApproval(DelegateInvoiceBatchApprovalRequest request, StreamObserver<DelegateInvoiceBatchApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateInvoiceBatchApproval(request));
    }

    /**
     * Recalls an invoice batch.
     */
    @Override
    public void recallInvoiceBatch(RecallInvoiceBatchRequest request, StreamObserver<RecallInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallInvoiceBatch(request));
    }

    /**
     * Comments an invoice batch.
     */
    @Override
    public void commentInvoiceBatchApproval(CommentInvoiceBatchApprovalRequest request, StreamObserver<CommentInvoiceBatchApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentInvoiceBatchApproval(request));
    }
}
