package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.AdvanceLoanApplicationWorkflowRequest;
import com.acme.lending.v1.AdvanceLoanApplicationWorkflowResponse;
import com.acme.lending.v1.CancelLoanApplicationWorkflowRequest;
import com.acme.lending.v1.CancelLoanApplicationWorkflowResponse;
import com.acme.lending.v1.InspectLoanApplicationWorkflowRequest;
import com.acme.lending.v1.InspectLoanApplicationWorkflowResponse;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanApplicationWorkflowRequest;
import com.acme.lending.v1.PauseLoanApplicationWorkflowResponse;
import com.acme.lending.v1.ResumeLoanApplicationWorkflowRequest;
import com.acme.lending.v1.ResumeLoanApplicationWorkflowResponse;
import com.acme.lending.v1.RetryLoanApplicationWorkflowRequest;
import com.acme.lending.v1.RetryLoanApplicationWorkflowResponse;
import com.acme.lending.v1.SignalLoanApplicationWorkflowRequest;
import com.acme.lending.v1.SignalLoanApplicationWorkflowResponse;
import com.acme.lending.v1.StartLoanApplicationWorkflowRequest;
import com.acme.lending.v1.StartLoanApplicationWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a loan application from start to finish.
 */
@GrpcService
public class LoanApplicationWorkflowEndpoint extends LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceImplBase {
    protected final LoanApplicationWorkflowApplication application;

    public LoanApplicationWorkflowEndpoint(LoanApplicationWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a loan application.
     */
    @Override
    public void startLoanApplicationWorkflow(StartLoanApplicationWorkflowRequest request, StreamObserver<StartLoanApplicationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startLoanApplicationWorkflow(request));
    }

    /**
     * Advances a loan application.
     */
    @Override
    public void advanceLoanApplicationWorkflow(AdvanceLoanApplicationWorkflowRequest request, StreamObserver<AdvanceLoanApplicationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceLoanApplicationWorkflow(request));
    }

    /**
     * Cancels a loan application.
     */
    @Override
    public void cancelLoanApplicationWorkflow(CancelLoanApplicationWorkflowRequest request, StreamObserver<CancelLoanApplicationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelLoanApplicationWorkflow(request));
    }

    /**
     * Retrys a loan application.
     */
    @Override
    public void retryLoanApplicationWorkflow(RetryLoanApplicationWorkflowRequest request, StreamObserver<RetryLoanApplicationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryLoanApplicationWorkflow(request));
    }

    /**
     * Pauses a loan application.
     */
    @Override
    public void pauseLoanApplicationWorkflow(PauseLoanApplicationWorkflowRequest request, StreamObserver<PauseLoanApplicationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseLoanApplicationWorkflow(request));
    }

    /**
     * Resumes a loan application.
     */
    @Override
    public void resumeLoanApplicationWorkflow(ResumeLoanApplicationWorkflowRequest request, StreamObserver<ResumeLoanApplicationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeLoanApplicationWorkflow(request));
    }

    /**
     * Returns a loan application.
     */
    @Override
    public void inspectLoanApplicationWorkflow(InspectLoanApplicationWorkflowRequest request, StreamObserver<InspectLoanApplicationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectLoanApplicationWorkflow(request));
    }

    /**
     * Signals a loan application.
     */
    @Override
    public void signalLoanApplicationWorkflow(SignalLoanApplicationWorkflowRequest request, StreamObserver<SignalLoanApplicationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalLoanApplicationWorkflow(request));
    }
}
