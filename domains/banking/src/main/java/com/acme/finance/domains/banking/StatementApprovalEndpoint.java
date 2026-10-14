package com.acme.finance.domains.banking;

import com.acme.banking.v1.ApproveStatementRequest;
import com.acme.banking.v1.ApproveStatementResponse;
import com.acme.banking.v1.CommentStatementApprovalRequest;
import com.acme.banking.v1.CommentStatementApprovalResponse;
import com.acme.banking.v1.EscalateStatementRequest;
import com.acme.banking.v1.EscalateStatementResponse;
import com.acme.banking.v1.GetStatementApprovalStatusRequest;
import com.acme.banking.v1.GetStatementApprovalStatusResponse;
import com.acme.banking.v1.ListPendingStatementsRequest;
import com.acme.banking.v1.ListPendingStatementsResponse;
import com.acme.banking.v1.RecallStatementRequest;
import com.acme.banking.v1.RecallStatementResponse;
import com.acme.banking.v1.RejectStatementRequest;
import com.acme.banking.v1.RejectStatementResponse;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.banking.v1.SubmitStatementRequest;
import com.acme.banking.v1.SubmitStatementResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes statements through approval, with escalation and delegation.
 */
@GrpcService
public class StatementApprovalEndpoint extends StatementApprovalServiceGrpc.StatementApprovalServiceImplBase {
    protected final StatementApprovalApplication application;

    public StatementApprovalEndpoint(StatementApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a statement.
     */
    @Override
    public void submitStatement(SubmitStatementRequest request, StreamObserver<SubmitStatementResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitStatement(request));
    }

    /**
     * Approves a statement.
     */
    @Override
    public void approveStatement(ApproveStatementRequest request, StreamObserver<ApproveStatementResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveStatement(request));
    }

    /**
     * Rejects a statement.
     */
    @Override
    public void rejectStatement(RejectStatementRequest request, StreamObserver<RejectStatementResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectStatement(request));
    }

    /**
     * Escalates a statement.
     */
    @Override
    public void escalateStatement(EscalateStatementRequest request, StreamObserver<EscalateStatementResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateStatement(request));
    }

    /**
     * Lists statements a page at a time.
     */
    @Override
    public void listPendingStatements(ListPendingStatementsRequest request, StreamObserver<ListPendingStatementsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingStatements(request));
    }

    /**
     * Recalls a statement.
     */
    @Override
    public void recallStatement(RecallStatementRequest request, StreamObserver<RecallStatementResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallStatement(request));
    }

    /**
     * Returns a statement.
     */
    @Override
    public void getStatementApprovalStatus(GetStatementApprovalStatusRequest request, StreamObserver<GetStatementApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getStatementApprovalStatus(request));
    }

    /**
     * Comments a statement.
     */
    @Override
    public void commentStatementApproval(CommentStatementApprovalRequest request, StreamObserver<CommentStatementApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentStatementApproval(request));
    }
}
