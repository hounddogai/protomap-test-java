package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ApproveMileageClaimRequest;
import com.acme.expenses.v1.ApproveMileageClaimResponse;
import com.acme.expenses.v1.CommentMileageClaimApprovalRequest;
import com.acme.expenses.v1.CommentMileageClaimApprovalResponse;
import com.acme.expenses.v1.EscalateMileageClaimRequest;
import com.acme.expenses.v1.EscalateMileageClaimResponse;
import com.acme.expenses.v1.GetMileageClaimApprovalStatusRequest;
import com.acme.expenses.v1.GetMileageClaimApprovalStatusResponse;
import com.acme.expenses.v1.MileageClaimApprovalServiceGrpc;
import com.acme.expenses.v1.RejectMileageClaimRequest;
import com.acme.expenses.v1.RejectMileageClaimResponse;
import com.acme.expenses.v1.SubmitMileageClaimRequest;
import com.acme.expenses.v1.SubmitMileageClaimResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes mileage claims through approval, with escalation and delegation.
 */
@GrpcService
public class MileageClaimApprovalEndpoint extends MileageClaimApprovalServiceGrpc.MileageClaimApprovalServiceImplBase {
    protected final MileageClaimApprovalApplication application;

    public MileageClaimApprovalEndpoint(MileageClaimApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a mileage claim.
     */
    @Override
    public void submitMileageClaim(SubmitMileageClaimRequest request, StreamObserver<SubmitMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitMileageClaim(request));
    }

    /**
     * Approves a mileage claim.
     */
    @Override
    public void approveMileageClaim(ApproveMileageClaimRequest request, StreamObserver<ApproveMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveMileageClaim(request));
    }

    /**
     * Rejects a mileage claim.
     */
    @Override
    public void rejectMileageClaim(RejectMileageClaimRequest request, StreamObserver<RejectMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectMileageClaim(request));
    }

    /**
     * Escalates a mileage claim.
     */
    @Override
    public void escalateMileageClaim(EscalateMileageClaimRequest request, StreamObserver<EscalateMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateMileageClaim(request));
    }

    /**
     * Returns a mileage claim.
     */
    @Override
    public void getMileageClaimApprovalStatus(GetMileageClaimApprovalStatusRequest request, StreamObserver<GetMileageClaimApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getMileageClaimApprovalStatus(request));
    }

    /**
     * Comments a mileage claim.
     */
    @Override
    public void commentMileageClaimApproval(CommentMileageClaimApprovalRequest request, StreamObserver<CommentMileageClaimApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentMileageClaimApproval(request));
    }
}
