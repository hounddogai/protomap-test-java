package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.ApproveContractClauseRequest;
import com.acme.contracts.v1.ApproveContractClauseResponse;
import com.acme.contracts.v1.CommentContractClauseApprovalRequest;
import com.acme.contracts.v1.CommentContractClauseApprovalResponse;
import com.acme.contracts.v1.ContractClauseApprovalServiceGrpc;
import com.acme.contracts.v1.DelegateContractClauseApprovalRequest;
import com.acme.contracts.v1.DelegateContractClauseApprovalResponse;
import com.acme.contracts.v1.EscalateContractClauseRequest;
import com.acme.contracts.v1.EscalateContractClauseResponse;
import com.acme.contracts.v1.GetContractClauseApprovalStatusRequest;
import com.acme.contracts.v1.GetContractClauseApprovalStatusResponse;
import com.acme.contracts.v1.ListPendingContractClausesRequest;
import com.acme.contracts.v1.ListPendingContractClausesResponse;
import com.acme.contracts.v1.RejectContractClauseRequest;
import com.acme.contracts.v1.RejectContractClauseResponse;
import com.acme.contracts.v1.SubmitContractClauseRequest;
import com.acme.contracts.v1.SubmitContractClauseResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes contract clauses through approval, with escalation and delegation.
 */
@GrpcService
public class ContractClauseApprovalEndpoint extends ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceImplBase {
    protected final ContractClauseApprovalApplication application;

    public ContractClauseApprovalEndpoint(ContractClauseApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a contract clause.
     */
    @Override
    public void submitContractClause(SubmitContractClauseRequest request, StreamObserver<SubmitContractClauseResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitContractClause(request));
    }

    /**
     * Approves a contract clause.
     */
    @Override
    public void approveContractClause(ApproveContractClauseRequest request, StreamObserver<ApproveContractClauseResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveContractClause(request));
    }

    /**
     * Rejects a contract clause.
     */
    @Override
    public void rejectContractClause(RejectContractClauseRequest request, StreamObserver<RejectContractClauseResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectContractClause(request));
    }

    /**
     * Escalates a contract clause.
     */
    @Override
    public void escalateContractClause(EscalateContractClauseRequest request, StreamObserver<EscalateContractClauseResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateContractClause(request));
    }

    /**
     * Lists contract clauses a page at a time.
     */
    @Override
    public void listPendingContractClauses(ListPendingContractClausesRequest request, StreamObserver<ListPendingContractClausesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingContractClauses(request));
    }

    /**
     * Delegates a contract clause.
     */
    @Override
    public void delegateContractClauseApproval(DelegateContractClauseApprovalRequest request, StreamObserver<DelegateContractClauseApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateContractClauseApproval(request));
    }

    /**
     * Returns a contract clause.
     */
    @Override
    public void getContractClauseApprovalStatus(GetContractClauseApprovalStatusRequest request, StreamObserver<GetContractClauseApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getContractClauseApprovalStatus(request));
    }

    /**
     * Comments a contract clause.
     */
    @Override
    public void commentContractClauseApproval(CommentContractClauseApprovalRequest request, StreamObserver<CommentContractClauseApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentContractClauseApproval(request));
    }
}
