package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.AdvanceExpenseReportWorkflowRequest;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowResponse;
import com.acme.expenses.v1.CancelExpenseReportWorkflowRequest;
import com.acme.expenses.v1.CancelExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ExpenseReportWorkflowServiceGrpc;
import com.acme.expenses.v1.InspectExpenseReportWorkflowRequest;
import com.acme.expenses.v1.InspectExpenseReportWorkflowResponse;
import com.acme.expenses.v1.PauseExpenseReportWorkflowRequest;
import com.acme.expenses.v1.PauseExpenseReportWorkflowResponse;
import com.acme.expenses.v1.RetryExpenseReportWorkflowRequest;
import com.acme.expenses.v1.RetryExpenseReportWorkflowResponse;
import com.acme.expenses.v1.SignalExpenseReportWorkflowRequest;
import com.acme.expenses.v1.SignalExpenseReportWorkflowResponse;
import com.acme.expenses.v1.StartExpenseReportWorkflowRequest;
import com.acme.expenses.v1.StartExpenseReportWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes an expense report from start to finish.
 */
@GrpcService
public class ExpenseReportWorkflowEndpoint extends ExpenseReportWorkflowServiceGrpc.ExpenseReportWorkflowServiceImplBase {
    protected final ExpenseReportWorkflowApplication application;

    public ExpenseReportWorkflowEndpoint(ExpenseReportWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an expense report.
     */
    @Override
    public void startExpenseReportWorkflow(StartExpenseReportWorkflowRequest request, StreamObserver<StartExpenseReportWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startExpenseReportWorkflow(request));
    }

    /**
     * Advances an expense report.
     */
    @Override
    public void advanceExpenseReportWorkflow(AdvanceExpenseReportWorkflowRequest request, StreamObserver<AdvanceExpenseReportWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceExpenseReportWorkflow(request));
    }

    /**
     * Cancels an expense report.
     */
    @Override
    public void cancelExpenseReportWorkflow(CancelExpenseReportWorkflowRequest request, StreamObserver<CancelExpenseReportWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelExpenseReportWorkflow(request));
    }

    /**
     * Retrys an expense report.
     */
    @Override
    public void retryExpenseReportWorkflow(RetryExpenseReportWorkflowRequest request, StreamObserver<RetryExpenseReportWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryExpenseReportWorkflow(request));
    }

    /**
     * Pauses an expense report.
     */
    @Override
    public void pauseExpenseReportWorkflow(PauseExpenseReportWorkflowRequest request, StreamObserver<PauseExpenseReportWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseExpenseReportWorkflow(request));
    }

    /**
     * Returns an expense report.
     */
    @Override
    public void inspectExpenseReportWorkflow(InspectExpenseReportWorkflowRequest request, StreamObserver<InspectExpenseReportWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectExpenseReportWorkflow(request));
    }

    /**
     * Signals an expense report.
     */
    @Override
    public void signalExpenseReportWorkflow(SignalExpenseReportWorkflowRequest request, StreamObserver<SignalExpenseReportWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalExpenseReportWorkflow(request));
    }
}
