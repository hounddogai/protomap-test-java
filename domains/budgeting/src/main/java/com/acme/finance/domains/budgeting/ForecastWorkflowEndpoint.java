package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.AdvanceForecastWorkflowRequest;
import com.acme.budgeting.v1.AdvanceForecastWorkflowResponse;
import com.acme.budgeting.v1.CancelForecastWorkflowRequest;
import com.acme.budgeting.v1.CancelForecastWorkflowResponse;
import com.acme.budgeting.v1.ForecastWorkflowServiceGrpc;
import com.acme.budgeting.v1.InspectForecastWorkflowRequest;
import com.acme.budgeting.v1.InspectForecastWorkflowResponse;
import com.acme.budgeting.v1.ListForecastWorkflowsRequest;
import com.acme.budgeting.v1.ListForecastWorkflowsResponse;
import com.acme.budgeting.v1.PauseForecastWorkflowRequest;
import com.acme.budgeting.v1.PauseForecastWorkflowResponse;
import com.acme.budgeting.v1.ResumeForecastWorkflowRequest;
import com.acme.budgeting.v1.ResumeForecastWorkflowResponse;
import com.acme.budgeting.v1.RetryForecastWorkflowRequest;
import com.acme.budgeting.v1.RetryForecastWorkflowResponse;
import com.acme.budgeting.v1.StartForecastWorkflowRequest;
import com.acme.budgeting.v1.StartForecastWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a forecast from start to finish.
 */
@GrpcService
public class ForecastWorkflowEndpoint extends ForecastWorkflowServiceGrpc.ForecastWorkflowServiceImplBase {
    protected final ForecastWorkflowApplication application;

    public ForecastWorkflowEndpoint(ForecastWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a forecast.
     */
    @Override
    public void startForecastWorkflow(StartForecastWorkflowRequest request, StreamObserver<StartForecastWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startForecastWorkflow(request));
    }

    /**
     * Advances a forecast.
     */
    @Override
    public void advanceForecastWorkflow(AdvanceForecastWorkflowRequest request, StreamObserver<AdvanceForecastWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceForecastWorkflow(request));
    }

    /**
     * Cancels a forecast.
     */
    @Override
    public void cancelForecastWorkflow(CancelForecastWorkflowRequest request, StreamObserver<CancelForecastWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelForecastWorkflow(request));
    }

    /**
     * Retrys a forecast.
     */
    @Override
    public void retryForecastWorkflow(RetryForecastWorkflowRequest request, StreamObserver<RetryForecastWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryForecastWorkflow(request));
    }

    /**
     * Pauses a forecast.
     */
    @Override
    public void pauseForecastWorkflow(PauseForecastWorkflowRequest request, StreamObserver<PauseForecastWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseForecastWorkflow(request));
    }

    /**
     * Resumes a forecast.
     */
    @Override
    public void resumeForecastWorkflow(ResumeForecastWorkflowRequest request, StreamObserver<ResumeForecastWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeForecastWorkflow(request));
    }

    /**
     * Returns a forecast.
     */
    @Override
    public void inspectForecastWorkflow(InspectForecastWorkflowRequest request, StreamObserver<InspectForecastWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectForecastWorkflow(request));
    }

    /**
     * Lists forecasts a page at a time.
     */
    @Override
    public void listForecastWorkflows(ListForecastWorkflowsRequest request, StreamObserver<ListForecastWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listForecastWorkflows(request));
    }
}
