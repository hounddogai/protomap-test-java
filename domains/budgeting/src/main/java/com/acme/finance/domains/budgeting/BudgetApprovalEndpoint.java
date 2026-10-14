package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ApproveBudgetRequest;
import com.acme.budgeting.v1.ApproveBudgetResponse;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.CommentBudgetApprovalRequest;
import com.acme.budgeting.v1.CommentBudgetApprovalResponse;
import com.acme.budgeting.v1.DelegateBudgetApprovalRequest;
import com.acme.budgeting.v1.DelegateBudgetApprovalResponse;
import com.acme.budgeting.v1.EscalateBudgetRequest;
import com.acme.budgeting.v1.EscalateBudgetResponse;
import com.acme.budgeting.v1.GetBudgetApprovalStatusRequest;
import com.acme.budgeting.v1.GetBudgetApprovalStatusResponse;
import com.acme.budgeting.v1.ListPendingBudgetsRequest;
import com.acme.budgeting.v1.ListPendingBudgetsResponse;
import com.acme.budgeting.v1.RecallBudgetRequest;
import com.acme.budgeting.v1.RecallBudgetResponse;
import com.acme.budgeting.v1.RejectBudgetRequest;
import com.acme.budgeting.v1.RejectBudgetResponse;
import com.acme.budgeting.v1.SubmitBudgetRequest;
import com.acme.budgeting.v1.SubmitBudgetResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes budgets through approval, with escalation and delegation.
 */
@GrpcService
public class BudgetApprovalEndpoint extends BudgetApprovalServiceGrpc.BudgetApprovalServiceImplBase {
    protected final BudgetApprovalApplication application;

    public BudgetApprovalEndpoint(BudgetApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a budget.
     */
    @Override
    public void submitBudget(SubmitBudgetRequest request, StreamObserver<SubmitBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitBudget(request));
    }

    /**
     * Approves a budget.
     */
    @Override
    public void approveBudget(ApproveBudgetRequest request, StreamObserver<ApproveBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveBudget(request));
    }

    /**
     * Rejects a budget.
     */
    @Override
    public void rejectBudget(RejectBudgetRequest request, StreamObserver<RejectBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectBudget(request));
    }

    /**
     * Escalates a budget.
     */
    @Override
    public void escalateBudget(EscalateBudgetRequest request, StreamObserver<EscalateBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateBudget(request));
    }

    /**
     * Lists budgets a page at a time.
     */
    @Override
    public void listPendingBudgets(ListPendingBudgetsRequest request, StreamObserver<ListPendingBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingBudgets(request));
    }

    /**
     * Delegates a budget.
     */
    @Override
    public void delegateBudgetApproval(DelegateBudgetApprovalRequest request, StreamObserver<DelegateBudgetApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateBudgetApproval(request));
    }

    /**
     * Recalls a budget.
     */
    @Override
    public void recallBudget(RecallBudgetRequest request, StreamObserver<RecallBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallBudget(request));
    }

    /**
     * Returns a budget.
     */
    @Override
    public void getBudgetApprovalStatus(GetBudgetApprovalStatusRequest request, StreamObserver<GetBudgetApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBudgetApprovalStatus(request));
    }

    /**
     * Comments a budget.
     */
    @Override
    public void commentBudgetApproval(CommentBudgetApprovalRequest request, StreamObserver<CommentBudgetApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentBudgetApproval(request));
    }
}
