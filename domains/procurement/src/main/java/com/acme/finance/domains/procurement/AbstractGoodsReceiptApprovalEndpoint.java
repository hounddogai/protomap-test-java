package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.ApproveGoodsReceiptRequest;
import com.acme.procurement.v1.ApproveGoodsReceiptResponse;
import com.acme.procurement.v1.CommentGoodsReceiptApprovalRequest;
import com.acme.procurement.v1.CommentGoodsReceiptApprovalResponse;
import com.acme.procurement.v1.DelegateGoodsReceiptApprovalRequest;
import com.acme.procurement.v1.DelegateGoodsReceiptApprovalResponse;
import com.acme.procurement.v1.EscalateGoodsReceiptRequest;
import com.acme.procurement.v1.EscalateGoodsReceiptResponse;
import com.acme.procurement.v1.GetGoodsReceiptApprovalStatusRequest;
import com.acme.procurement.v1.GetGoodsReceiptApprovalStatusResponse;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.ListPendingGoodsReceiptsRequest;
import com.acme.procurement.v1.ListPendingGoodsReceiptsResponse;
import com.acme.procurement.v1.RecallGoodsReceiptRequest;
import com.acme.procurement.v1.RecallGoodsReceiptResponse;
import com.acme.procurement.v1.RejectGoodsReceiptRequest;
import com.acme.procurement.v1.RejectGoodsReceiptResponse;
import com.acme.procurement.v1.SubmitGoodsReceiptRequest;
import com.acme.procurement.v1.SubmitGoodsReceiptResponse;
import io.grpc.stub.StreamObserver;

/**
 * Routes goods receipts through approval, with escalation and delegation.
 */
public abstract class AbstractGoodsReceiptApprovalEndpoint extends GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceImplBase {
    protected final GoodsReceiptApprovalApplication application;

    protected AbstractGoodsReceiptApprovalEndpoint(GoodsReceiptApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a goods receipt.
     */
    @Override
    public void submitGoodsReceipt(SubmitGoodsReceiptRequest request, StreamObserver<SubmitGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitGoodsReceipt(request));
    }

    /**
     * Approves a goods receipt.
     */
    @Override
    public void approveGoodsReceipt(ApproveGoodsReceiptRequest request, StreamObserver<ApproveGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveGoodsReceipt(request));
    }

    /**
     * Rejects a goods receipt.
     */
    @Override
    public void rejectGoodsReceipt(RejectGoodsReceiptRequest request, StreamObserver<RejectGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectGoodsReceipt(request));
    }

    /**
     * Escalates a goods receipt.
     */
    @Override
    public void escalateGoodsReceipt(EscalateGoodsReceiptRequest request, StreamObserver<EscalateGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateGoodsReceipt(request));
    }

    /**
     * Lists goods receipts a page at a time.
     */
    @Override
    public void listPendingGoodsReceipts(ListPendingGoodsReceiptsRequest request, StreamObserver<ListPendingGoodsReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingGoodsReceipts(request));
    }

    /**
     * Delegates a goods receipt.
     */
    @Override
    public void delegateGoodsReceiptApproval(DelegateGoodsReceiptApprovalRequest request, StreamObserver<DelegateGoodsReceiptApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateGoodsReceiptApproval(request));
    }

    /**
     * Recalls a goods receipt.
     */
    @Override
    public void recallGoodsReceipt(RecallGoodsReceiptRequest request, StreamObserver<RecallGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallGoodsReceipt(request));
    }

    /**
     * Returns a goods receipt.
     */
    @Override
    public void getGoodsReceiptApprovalStatus(GetGoodsReceiptApprovalStatusRequest request, StreamObserver<GetGoodsReceiptApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getGoodsReceiptApprovalStatus(request));
    }

    /**
     * Comments a goods receipt.
     */
    @Override
    public void commentGoodsReceiptApproval(CommentGoodsReceiptApprovalRequest request, StreamObserver<CommentGoodsReceiptApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentGoodsReceiptApproval(request));
    }
}
