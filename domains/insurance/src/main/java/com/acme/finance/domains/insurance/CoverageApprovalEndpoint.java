package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ApproveCoverageRequest;
import com.acme.insurance.v1.ApproveCoverageResponse;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.DelegateCoverageApprovalRequest;
import com.acme.insurance.v1.DelegateCoverageApprovalResponse;
import com.acme.insurance.v1.EscalateCoverageRequest;
import com.acme.insurance.v1.EscalateCoverageResponse;
import com.acme.insurance.v1.ListPendingCoveragesRequest;
import com.acme.insurance.v1.ListPendingCoveragesResponse;
import com.acme.insurance.v1.RecallCoverageRequest;
import com.acme.insurance.v1.RecallCoverageResponse;
import com.acme.insurance.v1.RejectCoverageRequest;
import com.acme.insurance.v1.RejectCoverageResponse;
import com.acme.insurance.v1.SubmitCoverageRequest;
import com.acme.insurance.v1.SubmitCoverageResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes coverages through approval, with escalation and delegation.
 */
@GrpcService
public class CoverageApprovalEndpoint extends CoverageApprovalServiceGrpc.CoverageApprovalServiceImplBase {
    protected final CoverageApprovalApplication application;

    public CoverageApprovalEndpoint(CoverageApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a coverage.
     */
    @Override
    public void submitCoverage(SubmitCoverageRequest request, StreamObserver<SubmitCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitCoverage(request));
    }

    /**
     * Approves a coverage.
     */
    @Override
    public void approveCoverage(ApproveCoverageRequest request, StreamObserver<ApproveCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveCoverage(request));
    }

    /**
     * Rejects a coverage.
     */
    @Override
    public void rejectCoverage(RejectCoverageRequest request, StreamObserver<RejectCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectCoverage(request));
    }

    /**
     * Escalates a coverage.
     */
    @Override
    public void escalateCoverage(EscalateCoverageRequest request, StreamObserver<EscalateCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateCoverage(request));
    }

    /**
     * Lists coverages a page at a time.
     */
    @Override
    public void listPendingCoverages(ListPendingCoveragesRequest request, StreamObserver<ListPendingCoveragesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingCoverages(request));
    }

    /**
     * Delegates a coverage.
     */
    @Override
    public void delegateCoverageApproval(DelegateCoverageApprovalRequest request, StreamObserver<DelegateCoverageApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateCoverageApproval(request));
    }

    /**
     * Recalls a coverage.
     */
    @Override
    public void recallCoverage(RecallCoverageRequest request, StreamObserver<RecallCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallCoverage(request));
    }
}
