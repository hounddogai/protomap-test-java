package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ApproveReceiptRequest;
import com.acme.expenses.v1.ApproveReceiptResponse;
import com.acme.expenses.v1.CommentReceiptApprovalRequest;
import com.acme.expenses.v1.CommentReceiptApprovalResponse;
import com.acme.expenses.v1.DelegateReceiptApprovalRequest;
import com.acme.expenses.v1.DelegateReceiptApprovalResponse;
import com.acme.expenses.v1.EscalateReceiptRequest;
import com.acme.expenses.v1.EscalateReceiptResponse;
import com.acme.expenses.v1.GetReceiptApprovalStatusRequest;
import com.acme.expenses.v1.GetReceiptApprovalStatusResponse;
import com.acme.expenses.v1.ListPendingReceiptsRequest;
import com.acme.expenses.v1.ListPendingReceiptsResponse;
import com.acme.expenses.v1.ReceiptApprovalServiceGrpc;
import com.acme.expenses.v1.RejectReceiptRequest;
import com.acme.expenses.v1.RejectReceiptResponse;
import com.acme.expenses.v1.SubmitReceiptRequest;
import com.acme.expenses.v1.SubmitReceiptResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes receipts through approval, with escalation and delegation.
 */
@GrpcService
public class ReceiptApprovalEndpoint extends ReceiptApprovalServiceGrpc.ReceiptApprovalServiceImplBase {
    protected final ReceiptApprovalApplication application;

    public ReceiptApprovalEndpoint(ReceiptApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a receipt.
     */
    @Override
    public void submitReceipt(SubmitReceiptRequest request, StreamObserver<SubmitReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitReceipt(request));
    }

    /**
     * Approves a receipt.
     */
    @Override
    public void approveReceipt(ApproveReceiptRequest request, StreamObserver<ApproveReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveReceipt(request));
    }

    /**
     * Rejects a receipt.
     */
    @Override
    public void rejectReceipt(RejectReceiptRequest request, StreamObserver<RejectReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectReceipt(request));
    }

    /**
     * Escalates a receipt.
     */
    @Override
    public void escalateReceipt(EscalateReceiptRequest request, StreamObserver<EscalateReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateReceipt(request));
    }

    /**
     * Lists receipts a page at a time.
     */
    @Override
    public void listPendingReceipts(ListPendingReceiptsRequest request, StreamObserver<ListPendingReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingReceipts(request));
    }

    /**
     * Delegates a receipt.
     */
    @Override
    public void delegateReceiptApproval(DelegateReceiptApprovalRequest request, StreamObserver<DelegateReceiptApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateReceiptApproval(request));
    }

    /**
     * Returns a receipt.
     */
    @Override
    public void getReceiptApprovalStatus(GetReceiptApprovalStatusRequest request, StreamObserver<GetReceiptApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getReceiptApprovalStatus(request));
    }

    /**
     * Comments a receipt.
     */
    @Override
    public void commentReceiptApproval(CommentReceiptApprovalRequest request, StreamObserver<CommentReceiptApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentReceiptApproval(request));
    }
}
