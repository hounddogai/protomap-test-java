package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.ApproveEInvoiceRequest;
import com.acme.invoicing.v1.ApproveEInvoiceResponse;
import com.acme.invoicing.v1.CommentEInvoiceApprovalRequest;
import com.acme.invoicing.v1.CommentEInvoiceApprovalResponse;
import com.acme.invoicing.v1.DelegateEInvoiceApprovalRequest;
import com.acme.invoicing.v1.DelegateEInvoiceApprovalResponse;
import com.acme.invoicing.v1.EInvoiceApprovalServiceGrpc;
import com.acme.invoicing.v1.EscalateEInvoiceRequest;
import com.acme.invoicing.v1.EscalateEInvoiceResponse;
import com.acme.invoicing.v1.GetEInvoiceApprovalStatusRequest;
import com.acme.invoicing.v1.GetEInvoiceApprovalStatusResponse;
import com.acme.invoicing.v1.ListPendingEInvoicesRequest;
import com.acme.invoicing.v1.ListPendingEInvoicesResponse;
import com.acme.invoicing.v1.RecallEInvoiceRequest;
import com.acme.invoicing.v1.RecallEInvoiceResponse;
import com.acme.invoicing.v1.RejectEInvoiceRequest;
import com.acme.invoicing.v1.RejectEInvoiceResponse;
import com.acme.invoicing.v1.SubmitEInvoiceRequest;
import com.acme.invoicing.v1.SubmitEInvoiceResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes e invoices through approval, with escalation and delegation.
 */
@GrpcService
public class EInvoiceApprovalEndpoint extends EInvoiceApprovalServiceGrpc.EInvoiceApprovalServiceImplBase {
    protected final EInvoiceApprovalApplication application;

    public EInvoiceApprovalEndpoint(EInvoiceApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an e invoice.
     */
    @Override
    public void submitEInvoice(SubmitEInvoiceRequest request, StreamObserver<SubmitEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitEInvoice(request));
    }

    /**
     * Approves an e invoice.
     */
    @Override
    public void approveEInvoice(ApproveEInvoiceRequest request, StreamObserver<ApproveEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveEInvoice(request));
    }

    /**
     * Rejects an e invoice.
     */
    @Override
    public void rejectEInvoice(RejectEInvoiceRequest request, StreamObserver<RejectEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectEInvoice(request));
    }

    /**
     * Escalates an e invoice.
     */
    @Override
    public void escalateEInvoice(EscalateEInvoiceRequest request, StreamObserver<EscalateEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateEInvoice(request));
    }

    /**
     * Lists e invoices a page at a time.
     */
    @Override
    public void listPendingEInvoices(ListPendingEInvoicesRequest request, StreamObserver<ListPendingEInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingEInvoices(request));
    }

    /**
     * Delegates an e invoice.
     */
    @Override
    public void delegateEInvoiceApproval(DelegateEInvoiceApprovalRequest request, StreamObserver<DelegateEInvoiceApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateEInvoiceApproval(request));
    }

    /**
     * Recalls an e invoice.
     */
    @Override
    public void recallEInvoice(RecallEInvoiceRequest request, StreamObserver<RecallEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallEInvoice(request));
    }

    /**
     * Returns an e invoice.
     */
    @Override
    public void getEInvoiceApprovalStatus(GetEInvoiceApprovalStatusRequest request, StreamObserver<GetEInvoiceApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getEInvoiceApprovalStatus(request));
    }

    /**
     * Comments an e invoice.
     */
    @Override
    public void commentEInvoiceApproval(CommentEInvoiceApprovalRequest request, StreamObserver<CommentEInvoiceApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentEInvoiceApproval(request));
    }
}
