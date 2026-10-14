package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ApproveBudgetLineRequest;
import com.acme.budgeting.v1.ApproveBudgetLineResponse;
import com.acme.budgeting.v1.BudgetLineApprovalServiceGrpc;
import com.acme.budgeting.v1.CommentBudgetLineApprovalRequest;
import com.acme.budgeting.v1.CommentBudgetLineApprovalResponse;
import com.acme.budgeting.v1.DelegateBudgetLineApprovalRequest;
import com.acme.budgeting.v1.DelegateBudgetLineApprovalResponse;
import com.acme.budgeting.v1.EscalateBudgetLineRequest;
import com.acme.budgeting.v1.EscalateBudgetLineResponse;
import com.acme.budgeting.v1.GetBudgetLineApprovalStatusRequest;
import com.acme.budgeting.v1.GetBudgetLineApprovalStatusResponse;
import com.acme.budgeting.v1.ListPendingBudgetLinesRequest;
import com.acme.budgeting.v1.ListPendingBudgetLinesResponse;
import com.acme.budgeting.v1.RecallBudgetLineRequest;
import com.acme.budgeting.v1.RecallBudgetLineResponse;
import com.acme.budgeting.v1.RejectBudgetLineRequest;
import com.acme.budgeting.v1.RejectBudgetLineResponse;
import com.acme.budgeting.v1.SubmitBudgetLineRequest;
import com.acme.budgeting.v1.SubmitBudgetLineResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes budget lines through approval, with escalation and delegation.
 */
@GrpcService
public class BudgetLineApprovalEndpoint extends BudgetLineApprovalServiceGrpc.BudgetLineApprovalServiceImplBase {
    protected final BudgetLineApprovalApplication application;

    public BudgetLineApprovalEndpoint(BudgetLineApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a budget line.
     */
    @Override
    public void submitBudgetLine(SubmitBudgetLineRequest request, StreamObserver<SubmitBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitBudgetLine(request));
    }

    /**
     * Approves a budget line.
     */
    @Override
    public void approveBudgetLine(ApproveBudgetLineRequest request, StreamObserver<ApproveBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveBudgetLine(request));
    }

    /**
     * Rejects a budget line.
     */
    @Override
    public void rejectBudgetLine(RejectBudgetLineRequest request, StreamObserver<RejectBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectBudgetLine(request));
    }

    /**
     * Escalates a budget line.
     */
    @Override
    public void escalateBudgetLine(EscalateBudgetLineRequest request, StreamObserver<EscalateBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateBudgetLine(request));
    }

    /**
     * Lists budget lines a page at a time.
     */
    @Override
    public void listPendingBudgetLines(ListPendingBudgetLinesRequest request, StreamObserver<ListPendingBudgetLinesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingBudgetLines(request));
    }

    /**
     * Delegates a budget line.
     */
    @Override
    public void delegateBudgetLineApproval(DelegateBudgetLineApprovalRequest request, StreamObserver<DelegateBudgetLineApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateBudgetLineApproval(request));
    }

    /**
     * Recalls a budget line.
     */
    @Override
    public void recallBudgetLine(RecallBudgetLineRequest request, StreamObserver<RecallBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallBudgetLine(request));
    }

    /**
     * Returns a budget line.
     */
    @Override
    public void getBudgetLineApprovalStatus(GetBudgetLineApprovalStatusRequest request, StreamObserver<GetBudgetLineApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBudgetLineApprovalStatus(request));
    }

    /**
     * Comments a budget line.
     */
    @Override
    public void commentBudgetLineApproval(CommentBudgetLineApprovalRequest request, StreamObserver<CommentBudgetLineApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentBudgetLineApproval(request));
    }
}
