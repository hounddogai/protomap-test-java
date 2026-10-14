package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AdvanceChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.AdvanceChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.CancelChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.CancelChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.ChartOfAccountsWorkflowServiceGrpc;
import com.acme.accounting.v1.InspectChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.InspectChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.ListChartOfAccountsWorkflowsRequest;
import com.acme.accounting.v1.ListChartOfAccountsWorkflowsResponse;
import com.acme.accounting.v1.PauseChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.PauseChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.ResumeChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.ResumeChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.RetryChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.RetryChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.SignalChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.SignalChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.StartChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.StartChartOfAccountsWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a chart of accounts from start to finish.
 */
@GrpcService
public class ChartOfAccountsWorkflowEndpoint extends ChartOfAccountsWorkflowServiceGrpc.ChartOfAccountsWorkflowServiceImplBase {
    protected final ChartOfAccountsWorkflowApplication application;

    public ChartOfAccountsWorkflowEndpoint(ChartOfAccountsWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a chart of accounts.
     */
    @Override
    public void startChartOfAccountsWorkflow(StartChartOfAccountsWorkflowRequest request, StreamObserver<StartChartOfAccountsWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startChartOfAccountsWorkflow(request));
    }

    /**
     * Advances a chart of accounts.
     */
    @Override
    public void advanceChartOfAccountsWorkflow(AdvanceChartOfAccountsWorkflowRequest request, StreamObserver<AdvanceChartOfAccountsWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceChartOfAccountsWorkflow(request));
    }

    /**
     * Cancels a chart of accounts.
     */
    @Override
    public void cancelChartOfAccountsWorkflow(CancelChartOfAccountsWorkflowRequest request, StreamObserver<CancelChartOfAccountsWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelChartOfAccountsWorkflow(request));
    }

    /**
     * Retrys a chart of accounts.
     */
    @Override
    public void retryChartOfAccountsWorkflow(RetryChartOfAccountsWorkflowRequest request, StreamObserver<RetryChartOfAccountsWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryChartOfAccountsWorkflow(request));
    }

    /**
     * Pauses a chart of accounts.
     */
    @Override
    public void pauseChartOfAccountsWorkflow(PauseChartOfAccountsWorkflowRequest request, StreamObserver<PauseChartOfAccountsWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseChartOfAccountsWorkflow(request));
    }

    /**
     * Resumes a chart of accounts.
     */
    @Override
    public void resumeChartOfAccountsWorkflow(ResumeChartOfAccountsWorkflowRequest request, StreamObserver<ResumeChartOfAccountsWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeChartOfAccountsWorkflow(request));
    }

    /**
     * Returns a chart of accounts.
     */
    @Override
    public void inspectChartOfAccountsWorkflow(InspectChartOfAccountsWorkflowRequest request, StreamObserver<InspectChartOfAccountsWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectChartOfAccountsWorkflow(request));
    }

    /**
     * Lists chart of accountses a page at a time.
     */
    @Override
    public void listChartOfAccountsWorkflows(ListChartOfAccountsWorkflowsRequest request, StreamObserver<ListChartOfAccountsWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listChartOfAccountsWorkflows(request));
    }

    /**
     * Signals a chart of accounts.
     */
    @Override
    public void signalChartOfAccountsWorkflow(SignalChartOfAccountsWorkflowRequest request, StreamObserver<SignalChartOfAccountsWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalChartOfAccountsWorkflow(request));
    }
}
