package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ApproveExpensePolicyRequest;
import com.acme.expenses.v1.ApproveExpensePolicyResponse;
import com.acme.expenses.v1.CommentExpensePolicyApprovalRequest;
import com.acme.expenses.v1.CommentExpensePolicyApprovalResponse;
import com.acme.expenses.v1.DelegateExpensePolicyApprovalRequest;
import com.acme.expenses.v1.DelegateExpensePolicyApprovalResponse;
import com.acme.expenses.v1.EscalateExpensePolicyRequest;
import com.acme.expenses.v1.EscalateExpensePolicyResponse;
import com.acme.expenses.v1.ExpensePolicyApprovalServiceGrpc;
import com.acme.expenses.v1.ListPendingExpensePoliciesRequest;
import com.acme.expenses.v1.ListPendingExpensePoliciesResponse;
import com.acme.expenses.v1.RecallExpensePolicyRequest;
import com.acme.expenses.v1.RecallExpensePolicyResponse;
import com.acme.expenses.v1.RejectExpensePolicyRequest;
import com.acme.expenses.v1.RejectExpensePolicyResponse;
import com.acme.expenses.v1.SubmitExpensePolicyRequest;
import com.acme.expenses.v1.SubmitExpensePolicyResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes expense policies through approval, with escalation and delegation.
 */
@GrpcService
public class ExpensePolicyApprovalEndpoint extends ExpensePolicyApprovalServiceGrpc.ExpensePolicyApprovalServiceImplBase {
    protected final ExpensePolicyApprovalApplication application;

    public ExpensePolicyApprovalEndpoint(ExpensePolicyApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an expense policy.
     */
    @Override
    public void submitExpensePolicy(SubmitExpensePolicyRequest request, StreamObserver<SubmitExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitExpensePolicy(request));
    }

    /**
     * Approves an expense policy.
     */
    @Override
    public void approveExpensePolicy(ApproveExpensePolicyRequest request, StreamObserver<ApproveExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveExpensePolicy(request));
    }

    /**
     * Rejects an expense policy.
     */
    @Override
    public void rejectExpensePolicy(RejectExpensePolicyRequest request, StreamObserver<RejectExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectExpensePolicy(request));
    }

    /**
     * Escalates an expense policy.
     */
    @Override
    public void escalateExpensePolicy(EscalateExpensePolicyRequest request, StreamObserver<EscalateExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateExpensePolicy(request));
    }

    /**
     * Lists expense policies a page at a time.
     */
    @Override
    public void listPendingExpensePolicies(ListPendingExpensePoliciesRequest request, StreamObserver<ListPendingExpensePoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingExpensePolicies(request));
    }

    /**
     * Delegates an expense policy.
     */
    @Override
    public void delegateExpensePolicyApproval(DelegateExpensePolicyApprovalRequest request, StreamObserver<DelegateExpensePolicyApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateExpensePolicyApproval(request));
    }

    /**
     * Recalls an expense policy.
     */
    @Override
    public void recallExpensePolicy(RecallExpensePolicyRequest request, StreamObserver<RecallExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallExpensePolicy(request));
    }

    /**
     * Comments an expense policy.
     */
    @Override
    public void commentExpensePolicyApproval(CommentExpensePolicyApprovalRequest request, StreamObserver<CommentExpensePolicyApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentExpensePolicyApproval(request));
    }
}
