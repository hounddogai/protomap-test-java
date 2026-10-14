package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.ApprovePurchaseRequisitionRequest;
import com.acme.procurement.v1.ApprovePurchaseRequisitionResponse;
import com.acme.procurement.v1.CommentPurchaseRequisitionApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseRequisitionApprovalResponse;
import com.acme.procurement.v1.DelegatePurchaseRequisitionApprovalRequest;
import com.acme.procurement.v1.DelegatePurchaseRequisitionApprovalResponse;
import com.acme.procurement.v1.EscalatePurchaseRequisitionRequest;
import com.acme.procurement.v1.EscalatePurchaseRequisitionResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionApprovalStatusRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionApprovalStatusResponse;
import com.acme.procurement.v1.ListPendingPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPendingPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionApprovalServiceGrpc;
import com.acme.procurement.v1.RecallPurchaseRequisitionRequest;
import com.acme.procurement.v1.RecallPurchaseRequisitionResponse;
import com.acme.procurement.v1.RejectPurchaseRequisitionRequest;
import com.acme.procurement.v1.RejectPurchaseRequisitionResponse;
import com.acme.procurement.v1.SubmitPurchaseRequisitionRequest;
import com.acme.procurement.v1.SubmitPurchaseRequisitionResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes purchase requisitions through approval, with escalation and delegation.
 */
@GrpcService
public class PurchaseRequisitionApprovalEndpoint extends PurchaseRequisitionApprovalServiceGrpc.PurchaseRequisitionApprovalServiceImplBase {
    protected final PurchaseRequisitionApprovalApplication application;

    public PurchaseRequisitionApprovalEndpoint(PurchaseRequisitionApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a purchase requisition.
     */
    @Override
    public void submitPurchaseRequisition(SubmitPurchaseRequisitionRequest request, StreamObserver<SubmitPurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitPurchaseRequisition(request));
    }

    /**
     * Approves a purchase requisition.
     */
    @Override
    public void approvePurchaseRequisition(ApprovePurchaseRequisitionRequest request, StreamObserver<ApprovePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.approvePurchaseRequisition(request));
    }

    /**
     * Rejects a purchase requisition.
     */
    @Override
    public void rejectPurchaseRequisition(RejectPurchaseRequisitionRequest request, StreamObserver<RejectPurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectPurchaseRequisition(request));
    }

    /**
     * Escalates a purchase requisition.
     */
    @Override
    public void escalatePurchaseRequisition(EscalatePurchaseRequisitionRequest request, StreamObserver<EscalatePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalatePurchaseRequisition(request));
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    @Override
    public void listPendingPurchaseRequisitions(ListPendingPurchaseRequisitionsRequest request, StreamObserver<ListPendingPurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingPurchaseRequisitions(request));
    }

    /**
     * Delegates a purchase requisition.
     */
    @Override
    public void delegatePurchaseRequisitionApproval(DelegatePurchaseRequisitionApprovalRequest request, StreamObserver<DelegatePurchaseRequisitionApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegatePurchaseRequisitionApproval(request));
    }

    /**
     * Recalls a purchase requisition.
     */
    @Override
    public void recallPurchaseRequisition(RecallPurchaseRequisitionRequest request, StreamObserver<RecallPurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallPurchaseRequisition(request));
    }

    /**
     * Returns a purchase requisition.
     */
    @Override
    public void getPurchaseRequisitionApprovalStatus(GetPurchaseRequisitionApprovalStatusRequest request, StreamObserver<GetPurchaseRequisitionApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPurchaseRequisitionApprovalStatus(request));
    }

    /**
     * Comments a purchase requisition.
     */
    @Override
    public void commentPurchaseRequisitionApproval(CommentPurchaseRequisitionApprovalRequest request, StreamObserver<CommentPurchaseRequisitionApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentPurchaseRequisitionApproval(request));
    }
}
