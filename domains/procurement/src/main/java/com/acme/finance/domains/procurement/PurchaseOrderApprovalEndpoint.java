package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.ApprovePurchaseOrderRequest;
import com.acme.procurement.v1.ApprovePurchaseOrderResponse;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalResponse;
import com.acme.procurement.v1.EscalatePurchaseOrderRequest;
import com.acme.procurement.v1.EscalatePurchaseOrderResponse;
import com.acme.procurement.v1.GetPurchaseOrderApprovalStatusRequest;
import com.acme.procurement.v1.GetPurchaseOrderApprovalStatusResponse;
import com.acme.procurement.v1.ListPendingPurchaseOrdersRequest;
import com.acme.procurement.v1.ListPendingPurchaseOrdersResponse;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.RecallPurchaseOrderRequest;
import com.acme.procurement.v1.RecallPurchaseOrderResponse;
import com.acme.procurement.v1.RejectPurchaseOrderRequest;
import com.acme.procurement.v1.RejectPurchaseOrderResponse;
import com.acme.procurement.v1.SubmitPurchaseOrderRequest;
import com.acme.procurement.v1.SubmitPurchaseOrderResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes purchase orders through approval, with escalation and delegation.
 */
@GrpcService
public class PurchaseOrderApprovalEndpoint extends PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceImplBase {
    protected final PurchaseOrderApprovalApplication application;

    public PurchaseOrderApprovalEndpoint(PurchaseOrderApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a purchase order.
     */
    @Override
    public void submitPurchaseOrder(SubmitPurchaseOrderRequest request, StreamObserver<SubmitPurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitPurchaseOrder(request));
    }

    /**
     * Approves a purchase order.
     */
    @Override
    public void approvePurchaseOrder(ApprovePurchaseOrderRequest request, StreamObserver<ApprovePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.approvePurchaseOrder(request));
    }

    /**
     * Rejects a purchase order.
     */
    @Override
    public void rejectPurchaseOrder(RejectPurchaseOrderRequest request, StreamObserver<RejectPurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectPurchaseOrder(request));
    }

    /**
     * Escalates a purchase order.
     */
    @Override
    public void escalatePurchaseOrder(EscalatePurchaseOrderRequest request, StreamObserver<EscalatePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalatePurchaseOrder(request));
    }

    /**
     * Lists purchase orders a page at a time.
     */
    @Override
    public void listPendingPurchaseOrders(ListPendingPurchaseOrdersRequest request, StreamObserver<ListPendingPurchaseOrdersResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingPurchaseOrders(request));
    }

    /**
     * Recalls a purchase order.
     */
    @Override
    public void recallPurchaseOrder(RecallPurchaseOrderRequest request, StreamObserver<RecallPurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallPurchaseOrder(request));
    }

    /**
     * Returns a purchase order.
     */
    @Override
    public void getPurchaseOrderApprovalStatus(GetPurchaseOrderApprovalStatusRequest request, StreamObserver<GetPurchaseOrderApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPurchaseOrderApprovalStatus(request));
    }

    /**
     * Comments a purchase order.
     */
    @Override
    public void commentPurchaseOrderApproval(CommentPurchaseOrderApprovalRequest request, StreamObserver<CommentPurchaseOrderApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentPurchaseOrderApproval(request));
    }
}
