package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.AdvanceLoanWorkflowRequest;
import com.acme.lending.v1.AdvanceLoanWorkflowResponse;
import com.acme.lending.v1.CancelLoanWorkflowRequest;
import com.acme.lending.v1.CancelLoanWorkflowResponse;
import com.acme.lending.v1.InspectLoanWorkflowRequest;
import com.acme.lending.v1.InspectLoanWorkflowResponse;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.acme.lending.v1.RetryLoanWorkflowRequest;
import com.acme.lending.v1.RetryLoanWorkflowResponse;
import com.acme.lending.v1.SignalLoanWorkflowRequest;
import com.acme.lending.v1.SignalLoanWorkflowResponse;
import com.acme.lending.v1.StartLoanWorkflowRequest;
import com.acme.lending.v1.StartLoanWorkflowResponse;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes a loan from start to finish.
 */
public abstract class AbstractLoanWorkflowEndpoint extends LoanWorkflowServiceGrpc.LoanWorkflowServiceImplBase {
    protected final LoanWorkflowApplication application;

    protected AbstractLoanWorkflowEndpoint(LoanWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a loan.
     */
    @Override
    public void startLoanWorkflow(StartLoanWorkflowRequest request, StreamObserver<StartLoanWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startLoanWorkflow(request));
    }

    /**
     * Advances a loan.
     */
    @Override
    public void advanceLoanWorkflow(AdvanceLoanWorkflowRequest request, StreamObserver<AdvanceLoanWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceLoanWorkflow(request));
    }

    /**
     * Cancels a loan.
     */
    @Override
    public void cancelLoanWorkflow(CancelLoanWorkflowRequest request, StreamObserver<CancelLoanWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelLoanWorkflow(request));
    }

    /**
     * Retrys a loan.
     */
    @Override
    public void retryLoanWorkflow(RetryLoanWorkflowRequest request, StreamObserver<RetryLoanWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryLoanWorkflow(request));
    }

    /**
     * Pauses a loan.
     */
    @Override
    public void pauseLoanWorkflow(PauseLoanWorkflowRequest request, StreamObserver<PauseLoanWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseLoanWorkflow(request));
    }

    /**
     * Returns a loan.
     */
    @Override
    public void inspectLoanWorkflow(InspectLoanWorkflowRequest request, StreamObserver<InspectLoanWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectLoanWorkflow(request));
    }

    /**
     * Signals a loan.
     */
    @Override
    public void signalLoanWorkflow(SignalLoanWorkflowRequest request, StreamObserver<SignalLoanWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalLoanWorkflow(request));
    }
}
