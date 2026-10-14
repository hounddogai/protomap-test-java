package com.acme.finance.domains.banking;

import com.acme.banking.v1.ApproveStandingOrderRequest;
import com.acme.banking.v1.ApproveStandingOrderResponse;
import com.acme.banking.v1.DelegateStandingOrderApprovalRequest;
import com.acme.banking.v1.DelegateStandingOrderApprovalResponse;
import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.ListPendingStandingOrdersRequest;
import com.acme.banking.v1.ListPendingStandingOrdersResponse;
import com.acme.banking.v1.RecallStandingOrderRequest;
import com.acme.banking.v1.RecallStandingOrderResponse;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.SubmitStandingOrderRequest;
import com.acme.banking.v1.SubmitStandingOrderResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes standing orders through approval, with escalation and delegation.
 */
@GrpcService
public class StandingOrderApprovalEndpoint extends StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceImplBase {
    protected final StandingOrderApprovalApplication application;

    public StandingOrderApprovalEndpoint(StandingOrderApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a standing order.
     */
    @Override
    public void submitStandingOrder(SubmitStandingOrderRequest request, StreamObserver<SubmitStandingOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitStandingOrder(request));
    }

    /**
     * Approves a standing order.
     */
    @Override
    public void approveStandingOrder(ApproveStandingOrderRequest request, StreamObserver<ApproveStandingOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveStandingOrder(request));
    }

    /**
     * Escalates a standing order.
     */
    @Override
    public void escalateStandingOrder(EscalateStandingOrderRequest request, StreamObserver<EscalateStandingOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateStandingOrder(request));
    }

    /**
     * Lists standing orders a page at a time.
     */
    @Override
    public void listPendingStandingOrders(ListPendingStandingOrdersRequest request, StreamObserver<ListPendingStandingOrdersResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingStandingOrders(request));
    }

    /**
     * Delegates a standing order.
     */
    @Override
    public void delegateStandingOrderApproval(DelegateStandingOrderApprovalRequest request, StreamObserver<DelegateStandingOrderApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateStandingOrderApproval(request));
    }

    /**
     * Recalls a standing order.
     */
    @Override
    public void recallStandingOrder(RecallStandingOrderRequest request, StreamObserver<RecallStandingOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallStandingOrder(request));
    }
}
