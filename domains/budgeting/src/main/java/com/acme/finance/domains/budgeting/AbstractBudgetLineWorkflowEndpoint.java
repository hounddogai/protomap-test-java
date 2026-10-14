package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.AdvanceBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.AdvanceBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.BudgetLineWorkflowServiceGrpc;
import com.acme.budgeting.v1.CancelBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.CancelBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.InspectBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.InspectBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.ListBudgetLineWorkflowsRequest;
import com.acme.budgeting.v1.ListBudgetLineWorkflowsResponse;
import com.acme.budgeting.v1.ResumeBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.ResumeBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.RetryBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.RetryBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.SignalBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.SignalBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.StartBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.StartBudgetLineWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes a budget line from start to finish.
 */
public abstract class AbstractBudgetLineWorkflowEndpoint extends BudgetLineWorkflowServiceGrpc.BudgetLineWorkflowServiceImplBase {
    protected final BudgetLineWorkflowApplication application;

    protected AbstractBudgetLineWorkflowEndpoint(BudgetLineWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a budget line.
     */
    @Override
    public void startBudgetLineWorkflow(StartBudgetLineWorkflowRequest request, StreamObserver<StartBudgetLineWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startBudgetLineWorkflow(request));
    }

    /**
     * Advances a budget line.
     */
    @Override
    public void advanceBudgetLineWorkflow(AdvanceBudgetLineWorkflowRequest request, StreamObserver<AdvanceBudgetLineWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceBudgetLineWorkflow(request));
    }

    /**
     * Cancels a budget line.
     */
    @Override
    public void cancelBudgetLineWorkflow(CancelBudgetLineWorkflowRequest request, StreamObserver<CancelBudgetLineWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelBudgetLineWorkflow(request));
    }

    /**
     * Retrys a budget line.
     */
    @Override
    public void retryBudgetLineWorkflow(RetryBudgetLineWorkflowRequest request, StreamObserver<RetryBudgetLineWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryBudgetLineWorkflow(request));
    }

    /**
     * Resumes a budget line.
     */
    @Override
    public void resumeBudgetLineWorkflow(ResumeBudgetLineWorkflowRequest request, StreamObserver<ResumeBudgetLineWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeBudgetLineWorkflow(request));
    }

    /**
     * Returns a budget line.
     */
    @Override
    public void inspectBudgetLineWorkflow(InspectBudgetLineWorkflowRequest request, StreamObserver<InspectBudgetLineWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectBudgetLineWorkflow(request));
    }

    /**
     * Lists budget lines a page at a time.
     */
    @Override
    public void listBudgetLineWorkflows(ListBudgetLineWorkflowsRequest request, StreamObserver<ListBudgetLineWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listBudgetLineWorkflows(request));
    }

    /**
     * Signals a budget line.
     */
    @Override
    public void signalBudgetLineWorkflow(SignalBudgetLineWorkflowRequest request, StreamObserver<SignalBudgetLineWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalBudgetLineWorkflow(request));
    }
}
