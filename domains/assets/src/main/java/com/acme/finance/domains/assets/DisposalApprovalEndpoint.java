package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveDisposalRequest;
import com.acme.assets.v1.ApproveDisposalResponse;
import com.acme.assets.v1.CommentDisposalApprovalRequest;
import com.acme.assets.v1.CommentDisposalApprovalResponse;
import com.acme.assets.v1.DelegateDisposalApprovalRequest;
import com.acme.assets.v1.DelegateDisposalApprovalResponse;
import com.acme.assets.v1.DisposalApprovalServiceGrpc;
import com.acme.assets.v1.GetDisposalApprovalStatusRequest;
import com.acme.assets.v1.GetDisposalApprovalStatusResponse;
import com.acme.assets.v1.ListPendingDisposalsRequest;
import com.acme.assets.v1.ListPendingDisposalsResponse;
import com.acme.assets.v1.RecallDisposalRequest;
import com.acme.assets.v1.RecallDisposalResponse;
import com.acme.assets.v1.RejectDisposalRequest;
import com.acme.assets.v1.RejectDisposalResponse;
import com.acme.assets.v1.SubmitDisposalRequest;
import com.acme.assets.v1.SubmitDisposalResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes disposals through approval, with escalation and delegation.
 */
@GrpcService
public class DisposalApprovalEndpoint extends DisposalApprovalServiceGrpc.DisposalApprovalServiceImplBase {
    protected final DisposalApprovalApplication application;

    public DisposalApprovalEndpoint(DisposalApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a disposal.
     */
    @Override
    public void submitDisposal(SubmitDisposalRequest request, StreamObserver<SubmitDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitDisposal(request));
    }

    /**
     * Approves a disposal.
     */
    @Override
    public void approveDisposal(ApproveDisposalRequest request, StreamObserver<ApproveDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveDisposal(request));
    }

    /**
     * Rejects a disposal.
     */
    @Override
    public void rejectDisposal(RejectDisposalRequest request, StreamObserver<RejectDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectDisposal(request));
    }

    /**
     * Lists disposals a page at a time.
     */
    @Override
    public void listPendingDisposals(ListPendingDisposalsRequest request, StreamObserver<ListPendingDisposalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingDisposals(request));
    }

    /**
     * Delegates a disposal.
     */
    @Override
    public void delegateDisposalApproval(DelegateDisposalApprovalRequest request, StreamObserver<DelegateDisposalApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateDisposalApproval(request));
    }

    /**
     * Recalls a disposal.
     */
    @Override
    public void recallDisposal(RecallDisposalRequest request, StreamObserver<RecallDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallDisposal(request));
    }

    /**
     * Returns a disposal.
     */
    @Override
    public void getDisposalApprovalStatus(GetDisposalApprovalStatusRequest request, StreamObserver<GetDisposalApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDisposalApprovalStatus(request));
    }

    /**
     * Comments a disposal.
     */
    @Override
    public void commentDisposalApproval(CommentDisposalApprovalRequest request, StreamObserver<CommentDisposalApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentDisposalApproval(request));
    }
}
