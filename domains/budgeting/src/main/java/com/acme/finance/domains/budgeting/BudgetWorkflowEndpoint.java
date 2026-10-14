package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.AdvanceBudgetWorkflowRequest;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowResponse;
import com.acme.budgeting.v1.BudgetWorkflowServiceGrpc;
import com.acme.budgeting.v1.CancelBudgetWorkflowRequest;
import com.acme.budgeting.v1.CancelBudgetWorkflowResponse;
import com.acme.budgeting.v1.InspectBudgetWorkflowRequest;
import com.acme.budgeting.v1.InspectBudgetWorkflowResponse;
import com.acme.budgeting.v1.ListBudgetWorkflowsRequest;
import com.acme.budgeting.v1.ListBudgetWorkflowsResponse;
import com.acme.budgeting.v1.PauseBudgetWorkflowRequest;
import com.acme.budgeting.v1.PauseBudgetWorkflowResponse;
import com.acme.budgeting.v1.ResumeBudgetWorkflowRequest;
import com.acme.budgeting.v1.ResumeBudgetWorkflowResponse;
import com.acme.budgeting.v1.RetryBudgetWorkflowRequest;
import com.acme.budgeting.v1.RetryBudgetWorkflowResponse;
import com.acme.budgeting.v1.SignalBudgetWorkflowRequest;
import com.acme.budgeting.v1.SignalBudgetWorkflowResponse;
import com.acme.budgeting.v1.StartBudgetWorkflowRequest;
import com.acme.budgeting.v1.StartBudgetWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a budget from start to finish.
 */
@GrpcService
public class BudgetWorkflowEndpoint extends BudgetWorkflowServiceGrpc.BudgetWorkflowServiceImplBase {
    protected final BudgetWorkflowApplication application;

    public BudgetWorkflowEndpoint(BudgetWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a budget.
     */
    @Override
    public void startBudgetWorkflow(StartBudgetWorkflowRequest request, StreamObserver<StartBudgetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startBudgetWorkflow(request));
    }

    /**
     * Advances a budget.
     */
    @Override
    public void advanceBudgetWorkflow(AdvanceBudgetWorkflowRequest request, StreamObserver<AdvanceBudgetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceBudgetWorkflow(request));
    }

    /**
     * Cancels a budget.
     */
    @Override
    public void cancelBudgetWorkflow(CancelBudgetWorkflowRequest request, StreamObserver<CancelBudgetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelBudgetWorkflow(request));
    }

    /**
     * Retrys a budget.
     */
    @Override
    public void retryBudgetWorkflow(RetryBudgetWorkflowRequest request, StreamObserver<RetryBudgetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryBudgetWorkflow(request));
    }

    /**
     * Pauses a budget.
     */
    @Override
    public void pauseBudgetWorkflow(PauseBudgetWorkflowRequest request, StreamObserver<PauseBudgetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseBudgetWorkflow(request));
    }

    /**
     * Resumes a budget.
     */
    @Override
    public void resumeBudgetWorkflow(ResumeBudgetWorkflowRequest request, StreamObserver<ResumeBudgetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeBudgetWorkflow(request));
    }

    /**
     * Returns a budget.
     */
    @Override
    public void inspectBudgetWorkflow(InspectBudgetWorkflowRequest request, StreamObserver<InspectBudgetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectBudgetWorkflow(request));
    }

    /**
     * Lists budgets a page at a time.
     */
    @Override
    public void listBudgetWorkflows(ListBudgetWorkflowsRequest request, StreamObserver<ListBudgetWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listBudgetWorkflows(request));
    }

    /**
     * Signals a budget.
     */
    @Override
    public void signalBudgetWorkflow(SignalBudgetWorkflowRequest request, StreamObserver<SignalBudgetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalBudgetWorkflow(request));
    }
}
