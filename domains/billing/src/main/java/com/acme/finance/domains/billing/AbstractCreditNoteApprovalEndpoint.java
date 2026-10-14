package com.acme.finance.domains.billing;

import com.acme.billing.v1.ApproveCreditNoteRequest;
import com.acme.billing.v1.ApproveCreditNoteResponse;
import com.acme.billing.v1.CommentCreditNoteApprovalRequest;
import com.acme.billing.v1.CommentCreditNoteApprovalResponse;
import com.acme.billing.v1.CreditNoteApprovalServiceGrpc;
import com.acme.billing.v1.GetCreditNoteApprovalStatusRequest;
import com.acme.billing.v1.GetCreditNoteApprovalStatusResponse;
import com.acme.billing.v1.ListPendingCreditNotesRequest;
import com.acme.billing.v1.ListPendingCreditNotesResponse;
import com.acme.billing.v1.RejectCreditNoteRequest;
import com.acme.billing.v1.RejectCreditNoteResponse;
import com.acme.billing.v1.SubmitCreditNoteRequest;
import com.acme.billing.v1.SubmitCreditNoteResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Routes credit notes through approval, with escalation and delegation.
 */
public abstract class AbstractCreditNoteApprovalEndpoint extends CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceImplBase {
    protected final CreditNoteApprovalApplication application;

    protected AbstractCreditNoteApprovalEndpoint(CreditNoteApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a credit note.
     */
    @Override
    public void submitCreditNote(SubmitCreditNoteRequest request, StreamObserver<SubmitCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitCreditNote(request));
    }

    /**
     * Approves a credit note.
     */
    @Override
    public void approveCreditNote(ApproveCreditNoteRequest request, StreamObserver<ApproveCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveCreditNote(request));
    }

    /**
     * Rejects a credit note.
     */
    @Override
    public void rejectCreditNote(RejectCreditNoteRequest request, StreamObserver<RejectCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectCreditNote(request));
    }

    /**
     * Lists credit notes a page at a time.
     */
    @Override
    public void listPendingCreditNotes(ListPendingCreditNotesRequest request, StreamObserver<ListPendingCreditNotesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingCreditNotes(request));
    }

    /**
     * Returns a credit note.
     */
    @Override
    public void getCreditNoteApprovalStatus(GetCreditNoteApprovalStatusRequest request, StreamObserver<GetCreditNoteApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCreditNoteApprovalStatus(request));
    }

    /**
     * Comments a credit note.
     */
    @Override
    public void commentCreditNoteApproval(CommentCreditNoteApprovalRequest request, StreamObserver<CommentCreditNoteApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentCreditNoteApproval(request));
    }
}
