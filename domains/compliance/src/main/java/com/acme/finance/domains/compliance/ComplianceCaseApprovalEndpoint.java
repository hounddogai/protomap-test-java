package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ApproveComplianceCaseRequest;
import com.acme.compliance.v1.ApproveComplianceCaseResponse;
import com.acme.compliance.v1.CommentComplianceCaseApprovalRequest;
import com.acme.compliance.v1.CommentComplianceCaseApprovalResponse;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.DelegateComplianceCaseApprovalRequest;
import com.acme.compliance.v1.DelegateComplianceCaseApprovalResponse;
import com.acme.compliance.v1.EscalateComplianceCaseRequest;
import com.acme.compliance.v1.EscalateComplianceCaseResponse;
import com.acme.compliance.v1.GetComplianceCaseApprovalStatusRequest;
import com.acme.compliance.v1.GetComplianceCaseApprovalStatusResponse;
import com.acme.compliance.v1.ListPendingComplianceCasesRequest;
import com.acme.compliance.v1.ListPendingComplianceCasesResponse;
import com.acme.compliance.v1.RecallComplianceCaseRequest;
import com.acme.compliance.v1.RecallComplianceCaseResponse;
import com.acme.compliance.v1.RejectComplianceCaseRequest;
import com.acme.compliance.v1.RejectComplianceCaseResponse;
import com.acme.compliance.v1.SubmitComplianceCaseRequest;
import com.acme.compliance.v1.SubmitComplianceCaseResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes compliance cases through approval, with escalation and delegation.
 */
@GrpcService
public class ComplianceCaseApprovalEndpoint extends ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceImplBase {
    protected final ComplianceCaseApprovalApplication application;

    public ComplianceCaseApprovalEndpoint(ComplianceCaseApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a compliance case.
     */
    @Override
    public void submitComplianceCase(SubmitComplianceCaseRequest request, StreamObserver<SubmitComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitComplianceCase(request));
    }

    /**
     * Approves a compliance case.
     */
    @Override
    public void approveComplianceCase(ApproveComplianceCaseRequest request, StreamObserver<ApproveComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveComplianceCase(request));
    }

    /**
     * Rejects a compliance case.
     */
    @Override
    public void rejectComplianceCase(RejectComplianceCaseRequest request, StreamObserver<RejectComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectComplianceCase(request));
    }

    /**
     * Escalates a compliance case.
     */
    @Override
    public void escalateComplianceCase(EscalateComplianceCaseRequest request, StreamObserver<EscalateComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateComplianceCase(request));
    }

    /**
     * Lists compliance cases a page at a time.
     */
    @Override
    public void listPendingComplianceCases(ListPendingComplianceCasesRequest request, StreamObserver<ListPendingComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingComplianceCases(request));
    }

    /**
     * Delegates a compliance case.
     */
    @Override
    public void delegateComplianceCaseApproval(DelegateComplianceCaseApprovalRequest request, StreamObserver<DelegateComplianceCaseApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateComplianceCaseApproval(request));
    }

    /**
     * Recalls a compliance case.
     */
    @Override
    public void recallComplianceCase(RecallComplianceCaseRequest request, StreamObserver<RecallComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallComplianceCase(request));
    }

    /**
     * Returns a compliance case.
     */
    @Override
    public void getComplianceCaseApprovalStatus(GetComplianceCaseApprovalStatusRequest request, StreamObserver<GetComplianceCaseApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getComplianceCaseApprovalStatus(request));
    }

    /**
     * Comments a compliance case.
     */
    @Override
    public void commentComplianceCaseApproval(CommentComplianceCaseApprovalRequest request, StreamObserver<CommentComplianceCaseApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentComplianceCaseApproval(request));
    }
}
