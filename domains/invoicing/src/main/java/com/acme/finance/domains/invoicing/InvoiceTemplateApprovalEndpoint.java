package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.ApproveInvoiceTemplateRequest;
import com.acme.invoicing.v1.ApproveInvoiceTemplateResponse;
import com.acme.invoicing.v1.CommentInvoiceTemplateApprovalRequest;
import com.acme.invoicing.v1.CommentInvoiceTemplateApprovalResponse;
import com.acme.invoicing.v1.DelegateInvoiceTemplateApprovalRequest;
import com.acme.invoicing.v1.DelegateInvoiceTemplateApprovalResponse;
import com.acme.invoicing.v1.EscalateInvoiceTemplateRequest;
import com.acme.invoicing.v1.EscalateInvoiceTemplateResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateApprovalStatusRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateApprovalStatusResponse;
import com.acme.invoicing.v1.InvoiceTemplateApprovalServiceGrpc;
import com.acme.invoicing.v1.ListPendingInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ListPendingInvoiceTemplatesResponse;
import com.acme.invoicing.v1.RecallInvoiceTemplateRequest;
import com.acme.invoicing.v1.RecallInvoiceTemplateResponse;
import com.acme.invoicing.v1.RejectInvoiceTemplateRequest;
import com.acme.invoicing.v1.RejectInvoiceTemplateResponse;
import com.acme.invoicing.v1.SubmitInvoiceTemplateRequest;
import com.acme.invoicing.v1.SubmitInvoiceTemplateResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes invoice templates through approval, with escalation and delegation.
 */
@GrpcService
public class InvoiceTemplateApprovalEndpoint extends InvoiceTemplateApprovalServiceGrpc.InvoiceTemplateApprovalServiceImplBase {
    protected final InvoiceTemplateApprovalApplication application;

    public InvoiceTemplateApprovalEndpoint(InvoiceTemplateApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an invoice template.
     */
    @Override
    public void submitInvoiceTemplate(SubmitInvoiceTemplateRequest request, StreamObserver<SubmitInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitInvoiceTemplate(request));
    }

    /**
     * Approves an invoice template.
     */
    @Override
    public void approveInvoiceTemplate(ApproveInvoiceTemplateRequest request, StreamObserver<ApproveInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveInvoiceTemplate(request));
    }

    /**
     * Rejects an invoice template.
     */
    @Override
    public void rejectInvoiceTemplate(RejectInvoiceTemplateRequest request, StreamObserver<RejectInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectInvoiceTemplate(request));
    }

    /**
     * Escalates an invoice template.
     */
    @Override
    public void escalateInvoiceTemplate(EscalateInvoiceTemplateRequest request, StreamObserver<EscalateInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateInvoiceTemplate(request));
    }

    /**
     * Lists invoice templates a page at a time.
     */
    @Override
    public void listPendingInvoiceTemplates(ListPendingInvoiceTemplatesRequest request, StreamObserver<ListPendingInvoiceTemplatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingInvoiceTemplates(request));
    }

    /**
     * Delegates an invoice template.
     */
    @Override
    public void delegateInvoiceTemplateApproval(DelegateInvoiceTemplateApprovalRequest request, StreamObserver<DelegateInvoiceTemplateApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateInvoiceTemplateApproval(request));
    }

    /**
     * Recalls an invoice template.
     */
    @Override
    public void recallInvoiceTemplate(RecallInvoiceTemplateRequest request, StreamObserver<RecallInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallInvoiceTemplate(request));
    }

    /**
     * Returns an invoice template.
     */
    @Override
    public void getInvoiceTemplateApprovalStatus(GetInvoiceTemplateApprovalStatusRequest request, StreamObserver<GetInvoiceTemplateApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getInvoiceTemplateApprovalStatus(request));
    }

    /**
     * Comments an invoice template.
     */
    @Override
    public void commentInvoiceTemplateApproval(CommentInvoiceTemplateApprovalRequest request, StreamObserver<CommentInvoiceTemplateApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentInvoiceTemplateApproval(request));
    }
}
