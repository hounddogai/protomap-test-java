package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.AdvanceCollateralWorkflowRequest;
import com.acme.lending.v1.AdvanceCollateralWorkflowResponse;
import com.acme.lending.v1.CancelCollateralWorkflowRequest;
import com.acme.lending.v1.CancelCollateralWorkflowResponse;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.lending.v1.PauseCollateralWorkflowRequest;
import com.acme.lending.v1.PauseCollateralWorkflowResponse;
import com.acme.lending.v1.ResumeCollateralWorkflowRequest;
import com.acme.lending.v1.ResumeCollateralWorkflowResponse;
import com.acme.lending.v1.SignalCollateralWorkflowRequest;
import com.acme.lending.v1.SignalCollateralWorkflowResponse;
import com.acme.lending.v1.StartCollateralWorkflowRequest;
import com.acme.lending.v1.StartCollateralWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a collateral from start to finish.
 */
@GrpcService
public class CollateralWorkflowEndpoint extends CollateralWorkflowServiceGrpc.CollateralWorkflowServiceImplBase {
    protected final CollateralWorkflowApplication application;

    public CollateralWorkflowEndpoint(CollateralWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a collateral.
     */
    @Override
    public void startCollateralWorkflow(StartCollateralWorkflowRequest request, StreamObserver<StartCollateralWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startCollateralWorkflow(request));
    }

    /**
     * Advances a collateral.
     */
    @Override
    public void advanceCollateralWorkflow(AdvanceCollateralWorkflowRequest request, StreamObserver<AdvanceCollateralWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceCollateralWorkflow(request));
    }

    /**
     * Cancels a collateral.
     */
    @Override
    public void cancelCollateralWorkflow(CancelCollateralWorkflowRequest request, StreamObserver<CancelCollateralWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelCollateralWorkflow(request));
    }

    /**
     * Pauses a collateral.
     */
    @Override
    public void pauseCollateralWorkflow(PauseCollateralWorkflowRequest request, StreamObserver<PauseCollateralWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseCollateralWorkflow(request));
    }

    /**
     * Resumes a collateral.
     */
    @Override
    public void resumeCollateralWorkflow(ResumeCollateralWorkflowRequest request, StreamObserver<ResumeCollateralWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeCollateralWorkflow(request));
    }

    /**
     * Lists collaterals a page at a time.
     */
    @Override
    public void listCollateralWorkflows(ListCollateralWorkflowsRequest request, StreamObserver<ListCollateralWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listCollateralWorkflows(request));
    }

    /**
     * Signals a collateral.
     */
    @Override
    public void signalCollateralWorkflow(SignalCollateralWorkflowRequest request, StreamObserver<SignalCollateralWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalCollateralWorkflow(request));
    }
}
