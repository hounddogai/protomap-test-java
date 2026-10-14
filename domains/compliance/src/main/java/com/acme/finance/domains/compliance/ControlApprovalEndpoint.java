package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ApproveControlRequest;
import com.acme.compliance.v1.ApproveControlResponse;
import com.acme.compliance.v1.CommentControlApprovalRequest;
import com.acme.compliance.v1.CommentControlApprovalResponse;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.EscalateControlRequest;
import com.acme.compliance.v1.EscalateControlResponse;
import com.acme.compliance.v1.GetControlApprovalStatusRequest;
import com.acme.compliance.v1.GetControlApprovalStatusResponse;
import com.acme.compliance.v1.ListPendingControlsRequest;
import com.acme.compliance.v1.ListPendingControlsResponse;
import com.acme.compliance.v1.RecallControlRequest;
import com.acme.compliance.v1.RecallControlResponse;
import com.acme.compliance.v1.RejectControlRequest;
import com.acme.compliance.v1.RejectControlResponse;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes controls through approval, with escalation and delegation.
 */
@GrpcService
public class ControlApprovalEndpoint extends ControlApprovalServiceGrpc.ControlApprovalServiceImplBase {
    protected final ControlApprovalApplication application;

    public ControlApprovalEndpoint(ControlApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a control.
     */
    @Override
    public void submitControl(SubmitControlRequest request, StreamObserver<SubmitControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitControl(request));
    }

    /**
     * Approves a control.
     */
    @Override
    public void approveControl(ApproveControlRequest request, StreamObserver<ApproveControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveControl(request));
    }

    /**
     * Rejects a control.
     */
    @Override
    public void rejectControl(RejectControlRequest request, StreamObserver<RejectControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectControl(request));
    }

    /**
     * Escalates a control.
     */
    @Override
    public void escalateControl(EscalateControlRequest request, StreamObserver<EscalateControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateControl(request));
    }

    /**
     * Lists controls a page at a time.
     */
    @Override
    public void listPendingControls(ListPendingControlsRequest request, StreamObserver<ListPendingControlsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingControls(request));
    }

    /**
     * Recalls a control.
     */
    @Override
    public void recallControl(RecallControlRequest request, StreamObserver<RecallControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallControl(request));
    }

    /**
     * Returns a control.
     */
    @Override
    public void getControlApprovalStatus(GetControlApprovalStatusRequest request, StreamObserver<GetControlApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getControlApprovalStatus(request));
    }

    /**
     * Comments a control.
     */
    @Override
    public void commentControlApproval(CommentControlApprovalRequest request, StreamObserver<CommentControlApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentControlApproval(request));
    }
}
