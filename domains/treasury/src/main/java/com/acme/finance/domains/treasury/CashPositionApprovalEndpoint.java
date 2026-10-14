package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.ApproveCashPositionRequest;
import com.acme.treasury.v1.ApproveCashPositionResponse;
import com.acme.treasury.v1.CashPositionApprovalServiceGrpc;
import com.acme.treasury.v1.CommentCashPositionApprovalRequest;
import com.acme.treasury.v1.CommentCashPositionApprovalResponse;
import com.acme.treasury.v1.DelegateCashPositionApprovalRequest;
import com.acme.treasury.v1.DelegateCashPositionApprovalResponse;
import com.acme.treasury.v1.GetCashPositionApprovalStatusRequest;
import com.acme.treasury.v1.GetCashPositionApprovalStatusResponse;
import com.acme.treasury.v1.ListPendingCashPositionsRequest;
import com.acme.treasury.v1.ListPendingCashPositionsResponse;
import com.acme.treasury.v1.RecallCashPositionRequest;
import com.acme.treasury.v1.RecallCashPositionResponse;
import com.acme.treasury.v1.SubmitCashPositionRequest;
import com.acme.treasury.v1.SubmitCashPositionResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes cash positions through approval, with escalation and delegation.
 */
@GrpcService
public class CashPositionApprovalEndpoint extends CashPositionApprovalServiceGrpc.CashPositionApprovalServiceImplBase {
    protected final CashPositionApprovalApplication application;

    public CashPositionApprovalEndpoint(CashPositionApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a cash position.
     */
    @Override
    public void submitCashPosition(SubmitCashPositionRequest request, StreamObserver<SubmitCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitCashPosition(request));
    }

    /**
     * Approves a cash position.
     */
    @Override
    public void approveCashPosition(ApproveCashPositionRequest request, StreamObserver<ApproveCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveCashPosition(request));
    }

    /**
     * Lists cash positions a page at a time.
     */
    @Override
    public void listPendingCashPositions(ListPendingCashPositionsRequest request, StreamObserver<ListPendingCashPositionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingCashPositions(request));
    }

    /**
     * Delegates a cash position.
     */
    @Override
    public void delegateCashPositionApproval(DelegateCashPositionApprovalRequest request, StreamObserver<DelegateCashPositionApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateCashPositionApproval(request));
    }

    /**
     * Recalls a cash position.
     */
    @Override
    public void recallCashPosition(RecallCashPositionRequest request, StreamObserver<RecallCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallCashPosition(request));
    }

    /**
     * Returns a cash position.
     */
    @Override
    public void getCashPositionApprovalStatus(GetCashPositionApprovalStatusRequest request, StreamObserver<GetCashPositionApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCashPositionApprovalStatus(request));
    }

    /**
     * Comments a cash position.
     */
    @Override
    public void commentCashPositionApproval(CommentCashPositionApprovalRequest request, StreamObserver<CommentCashPositionApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentCashPositionApproval(request));
    }
}
