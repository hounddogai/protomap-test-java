package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.AdvanceMileageClaimWorkflowRequest;
import com.acme.expenses.v1.AdvanceMileageClaimWorkflowResponse;
import com.acme.expenses.v1.CancelMileageClaimWorkflowRequest;
import com.acme.expenses.v1.CancelMileageClaimWorkflowResponse;
import com.acme.expenses.v1.InspectMileageClaimWorkflowRequest;
import com.acme.expenses.v1.InspectMileageClaimWorkflowResponse;
import com.acme.expenses.v1.MileageClaimWorkflowServiceGrpc;
import com.acme.expenses.v1.RetryMileageClaimWorkflowRequest;
import com.acme.expenses.v1.RetryMileageClaimWorkflowResponse;
import com.acme.expenses.v1.SignalMileageClaimWorkflowRequest;
import com.acme.expenses.v1.SignalMileageClaimWorkflowResponse;
import com.acme.expenses.v1.StartMileageClaimWorkflowRequest;
import com.acme.expenses.v1.StartMileageClaimWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a mileage claim from start to finish.
 */
@GrpcService
public class MileageClaimWorkflowEndpoint extends MileageClaimWorkflowServiceGrpc.MileageClaimWorkflowServiceImplBase {
    protected final MileageClaimWorkflowApplication application;

    public MileageClaimWorkflowEndpoint(MileageClaimWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a mileage claim.
     */
    @Override
    public void startMileageClaimWorkflow(StartMileageClaimWorkflowRequest request, StreamObserver<StartMileageClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startMileageClaimWorkflow(request));
    }

    /**
     * Advances a mileage claim.
     */
    @Override
    public void advanceMileageClaimWorkflow(AdvanceMileageClaimWorkflowRequest request, StreamObserver<AdvanceMileageClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceMileageClaimWorkflow(request));
    }

    /**
     * Cancels a mileage claim.
     */
    @Override
    public void cancelMileageClaimWorkflow(CancelMileageClaimWorkflowRequest request, StreamObserver<CancelMileageClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelMileageClaimWorkflow(request));
    }

    /**
     * Retrys a mileage claim.
     */
    @Override
    public void retryMileageClaimWorkflow(RetryMileageClaimWorkflowRequest request, StreamObserver<RetryMileageClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryMileageClaimWorkflow(request));
    }

    /**
     * Returns a mileage claim.
     */
    @Override
    public void inspectMileageClaimWorkflow(InspectMileageClaimWorkflowRequest request, StreamObserver<InspectMileageClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectMileageClaimWorkflow(request));
    }

    /**
     * Signals a mileage claim.
     */
    @Override
    public void signalMileageClaimWorkflow(SignalMileageClaimWorkflowRequest request, StreamObserver<SignalMileageClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalMileageClaimWorkflow(request));
    }
}
