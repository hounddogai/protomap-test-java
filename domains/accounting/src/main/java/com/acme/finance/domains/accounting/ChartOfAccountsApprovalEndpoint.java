package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.ApproveChartOfAccountsRequest;
import com.acme.accounting.v1.ApproveChartOfAccountsResponse;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.CommentChartOfAccountsApprovalRequest;
import com.acme.accounting.v1.CommentChartOfAccountsApprovalResponse;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalRequest;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalResponse;
import com.acme.accounting.v1.GetChartOfAccountsApprovalStatusRequest;
import com.acme.accounting.v1.GetChartOfAccountsApprovalStatusResponse;
import com.acme.accounting.v1.RecallChartOfAccountsRequest;
import com.acme.accounting.v1.RecallChartOfAccountsResponse;
import com.acme.accounting.v1.RejectChartOfAccountsRequest;
import com.acme.accounting.v1.RejectChartOfAccountsResponse;
import com.acme.accounting.v1.SubmitChartOfAccountsRequest;
import com.acme.accounting.v1.SubmitChartOfAccountsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes chart of accountses through approval, with escalation and delegation.
 */
@GrpcService
public class ChartOfAccountsApprovalEndpoint extends ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceImplBase {
    protected final ChartOfAccountsApprovalApplication application;

    public ChartOfAccountsApprovalEndpoint(ChartOfAccountsApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a chart of accounts.
     */
    @Override
    public void submitChartOfAccounts(SubmitChartOfAccountsRequest request, StreamObserver<SubmitChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitChartOfAccounts(request));
    }

    /**
     * Approves a chart of accounts.
     */
    @Override
    public void approveChartOfAccounts(ApproveChartOfAccountsRequest request, StreamObserver<ApproveChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveChartOfAccounts(request));
    }

    /**
     * Rejects a chart of accounts.
     */
    @Override
    public void rejectChartOfAccounts(RejectChartOfAccountsRequest request, StreamObserver<RejectChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectChartOfAccounts(request));
    }

    /**
     * Delegates a chart of accounts.
     */
    @Override
    public void delegateChartOfAccountsApproval(DelegateChartOfAccountsApprovalRequest request, StreamObserver<DelegateChartOfAccountsApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateChartOfAccountsApproval(request));
    }

    /**
     * Recalls a chart of accounts.
     */
    @Override
    public void recallChartOfAccounts(RecallChartOfAccountsRequest request, StreamObserver<RecallChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallChartOfAccounts(request));
    }

    /**
     * Returns a chart of accounts.
     */
    @Override
    public void getChartOfAccountsApprovalStatus(GetChartOfAccountsApprovalStatusRequest request, StreamObserver<GetChartOfAccountsApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getChartOfAccountsApprovalStatus(request));
    }

    /**
     * Comments a chart of accounts.
     */
    @Override
    public void commentChartOfAccountsApproval(CommentChartOfAccountsApprovalRequest request, StreamObserver<CommentChartOfAccountsApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentChartOfAccountsApproval(request));
    }
}
