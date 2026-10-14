package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ApproveSanctionRequest;
import com.acme.compliance.v1.ApproveSanctionResponse;
import com.acme.compliance.v1.CommentSanctionApprovalRequest;
import com.acme.compliance.v1.CommentSanctionApprovalResponse;
import com.acme.compliance.v1.DelegateSanctionApprovalRequest;
import com.acme.compliance.v1.DelegateSanctionApprovalResponse;
import com.acme.compliance.v1.EscalateSanctionRequest;
import com.acme.compliance.v1.EscalateSanctionResponse;
import com.acme.compliance.v1.GetSanctionApprovalStatusRequest;
import com.acme.compliance.v1.GetSanctionApprovalStatusResponse;
import com.acme.compliance.v1.ListPendingSanctionsRequest;
import com.acme.compliance.v1.ListPendingSanctionsResponse;
import com.acme.compliance.v1.RecallSanctionRequest;
import com.acme.compliance.v1.RecallSanctionResponse;
import com.acme.compliance.v1.RejectSanctionRequest;
import com.acme.compliance.v1.RejectSanctionResponse;
import com.acme.compliance.v1.SanctionApprovalServiceGrpc;
import com.acme.compliance.v1.SubmitSanctionRequest;
import com.acme.compliance.v1.SubmitSanctionResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes sanctions through approval, with escalation and delegation.
 */
@GrpcService
public class SanctionApprovalEndpoint extends SanctionApprovalServiceGrpc.SanctionApprovalServiceImplBase {
    protected final SanctionApprovalApplication application;

    public SanctionApprovalEndpoint(SanctionApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a sanction.
     */
    @Override
    public void submitSanction(SubmitSanctionRequest request, StreamObserver<SubmitSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitSanction(request));
    }

    /**
     * Approves a sanction.
     */
    @Override
    public void approveSanction(ApproveSanctionRequest request, StreamObserver<ApproveSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveSanction(request));
    }

    /**
     * Rejects a sanction.
     */
    @Override
    public void rejectSanction(RejectSanctionRequest request, StreamObserver<RejectSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectSanction(request));
    }

    /**
     * Escalates a sanction.
     */
    @Override
    public void escalateSanction(EscalateSanctionRequest request, StreamObserver<EscalateSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateSanction(request));
    }

    /**
     * Lists sanctions a page at a time.
     */
    @Override
    public void listPendingSanctions(ListPendingSanctionsRequest request, StreamObserver<ListPendingSanctionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingSanctions(request));
    }

    /**
     * Delegates a sanction.
     */
    @Override
    public void delegateSanctionApproval(DelegateSanctionApprovalRequest request, StreamObserver<DelegateSanctionApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateSanctionApproval(request));
    }

    /**
     * Recalls a sanction.
     */
    @Override
    public void recallSanction(RecallSanctionRequest request, StreamObserver<RecallSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallSanction(request));
    }

    /**
     * Returns a sanction.
     */
    @Override
    public void getSanctionApprovalStatus(GetSanctionApprovalStatusRequest request, StreamObserver<GetSanctionApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getSanctionApprovalStatus(request));
    }

    /**
     * Comments a sanction.
     */
    @Override
    public void commentSanctionApproval(CommentSanctionApprovalRequest request, StreamObserver<CommentSanctionApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentSanctionApproval(request));
    }
}
