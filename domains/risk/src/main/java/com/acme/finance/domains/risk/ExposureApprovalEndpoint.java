package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.ApproveExposureRequest;
import com.acme.risk.v1.ApproveExposureResponse;
import com.acme.risk.v1.CommentExposureApprovalRequest;
import com.acme.risk.v1.CommentExposureApprovalResponse;
import com.acme.risk.v1.DelegateExposureApprovalRequest;
import com.acme.risk.v1.DelegateExposureApprovalResponse;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.GetExposureApprovalStatusRequest;
import com.acme.risk.v1.GetExposureApprovalStatusResponse;
import com.acme.risk.v1.ListPendingExposuresRequest;
import com.acme.risk.v1.ListPendingExposuresResponse;
import com.acme.risk.v1.RecallExposureRequest;
import com.acme.risk.v1.RecallExposureResponse;
import com.acme.risk.v1.RejectExposureRequest;
import com.acme.risk.v1.RejectExposureResponse;
import com.acme.risk.v1.SubmitExposureRequest;
import com.acme.risk.v1.SubmitExposureResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes exposures through approval, with escalation and delegation.
 */
@GrpcService
public class ExposureApprovalEndpoint extends ExposureApprovalServiceGrpc.ExposureApprovalServiceImplBase {
    protected final ExposureApprovalApplication application;

    public ExposureApprovalEndpoint(ExposureApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an exposure.
     */
    @Override
    public void submitExposure(SubmitExposureRequest request, StreamObserver<SubmitExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitExposure(request));
    }

    /**
     * Approves an exposure.
     */
    @Override
    public void approveExposure(ApproveExposureRequest request, StreamObserver<ApproveExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveExposure(request));
    }

    /**
     * Rejects an exposure.
     */
    @Override
    public void rejectExposure(RejectExposureRequest request, StreamObserver<RejectExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectExposure(request));
    }

    /**
     * Lists exposures a page at a time.
     */
    @Override
    public void listPendingExposures(ListPendingExposuresRequest request, StreamObserver<ListPendingExposuresResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingExposures(request));
    }

    /**
     * Delegates an exposure.
     */
    @Override
    public void delegateExposureApproval(DelegateExposureApprovalRequest request, StreamObserver<DelegateExposureApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateExposureApproval(request));
    }

    /**
     * Recalls an exposure.
     */
    @Override
    public void recallExposure(RecallExposureRequest request, StreamObserver<RecallExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallExposure(request));
    }

    /**
     * Returns an exposure.
     */
    @Override
    public void getExposureApprovalStatus(GetExposureApprovalStatusRequest request, StreamObserver<GetExposureApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getExposureApprovalStatus(request));
    }

    /**
     * Comments an exposure.
     */
    @Override
    public void commentExposureApproval(CommentExposureApprovalRequest request, StreamObserver<CommentExposureApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentExposureApproval(request));
    }
}
