package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ApproveExpenseReportRequest;
import com.acme.expenses.v1.ApproveExpenseReportResponse;
import com.acme.expenses.v1.CommentExpenseReportApprovalRequest;
import com.acme.expenses.v1.CommentExpenseReportApprovalResponse;
import com.acme.expenses.v1.DelegateExpenseReportApprovalRequest;
import com.acme.expenses.v1.DelegateExpenseReportApprovalResponse;
import com.acme.expenses.v1.EscalateExpenseReportRequest;
import com.acme.expenses.v1.EscalateExpenseReportResponse;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ListPendingExpenseReportsRequest;
import com.acme.expenses.v1.ListPendingExpenseReportsResponse;
import com.acme.expenses.v1.RecallExpenseReportRequest;
import com.acme.expenses.v1.RecallExpenseReportResponse;
import com.acme.expenses.v1.RejectExpenseReportRequest;
import com.acme.expenses.v1.RejectExpenseReportResponse;
import com.acme.expenses.v1.SubmitExpenseReportRequest;
import com.acme.expenses.v1.SubmitExpenseReportResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes expense reports through approval, with escalation and delegation.
 */
@GrpcService
public class ExpenseReportApprovalEndpoint extends ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceImplBase {
    protected final ExpenseReportApprovalApplication application;

    public ExpenseReportApprovalEndpoint(ExpenseReportApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an expense report.
     */
    @Override
    public void submitExpenseReport(SubmitExpenseReportRequest request, StreamObserver<SubmitExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitExpenseReport(request));
    }

    /**
     * Approves an expense report.
     */
    @Override
    public void approveExpenseReport(ApproveExpenseReportRequest request, StreamObserver<ApproveExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveExpenseReport(request));
    }

    /**
     * Rejects an expense report.
     */
    @Override
    public void rejectExpenseReport(RejectExpenseReportRequest request, StreamObserver<RejectExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectExpenseReport(request));
    }

    /**
     * Escalates an expense report.
     */
    @Override
    public void escalateExpenseReport(EscalateExpenseReportRequest request, StreamObserver<EscalateExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateExpenseReport(request));
    }

    /**
     * Lists expense reports a page at a time.
     */
    @Override
    public void listPendingExpenseReports(ListPendingExpenseReportsRequest request, StreamObserver<ListPendingExpenseReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingExpenseReports(request));
    }

    /**
     * Delegates an expense report.
     */
    @Override
    public void delegateExpenseReportApproval(DelegateExpenseReportApprovalRequest request, StreamObserver<DelegateExpenseReportApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateExpenseReportApproval(request));
    }

    /**
     * Recalls an expense report.
     */
    @Override
    public void recallExpenseReport(RecallExpenseReportRequest request, StreamObserver<RecallExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallExpenseReport(request));
    }

    /**
     * Comments an expense report.
     */
    @Override
    public void commentExpenseReportApproval(CommentExpenseReportApprovalRequest request, StreamObserver<CommentExpenseReportApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentExpenseReportApproval(request));
    }
}
