package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.AdvanceDeductionWorkflowRequest;
import com.acme.payroll.v1.AdvanceDeductionWorkflowResponse;
import com.acme.payroll.v1.CancelDeductionWorkflowRequest;
import com.acme.payroll.v1.CancelDeductionWorkflowResponse;
import com.acme.payroll.v1.DeductionWorkflowServiceGrpc;
import com.acme.payroll.v1.InspectDeductionWorkflowRequest;
import com.acme.payroll.v1.InspectDeductionWorkflowResponse;
import com.acme.payroll.v1.ListDeductionWorkflowsRequest;
import com.acme.payroll.v1.ListDeductionWorkflowsResponse;
import com.acme.payroll.v1.PauseDeductionWorkflowRequest;
import com.acme.payroll.v1.PauseDeductionWorkflowResponse;
import com.acme.payroll.v1.ResumeDeductionWorkflowRequest;
import com.acme.payroll.v1.ResumeDeductionWorkflowResponse;
import com.acme.payroll.v1.RetryDeductionWorkflowRequest;
import com.acme.payroll.v1.RetryDeductionWorkflowResponse;
import com.acme.payroll.v1.SignalDeductionWorkflowRequest;
import com.acme.payroll.v1.SignalDeductionWorkflowResponse;
import com.acme.payroll.v1.StartDeductionWorkflowRequest;
import com.acme.payroll.v1.StartDeductionWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a deduction from start to finish.
 */
@GrpcService
public class DeductionWorkflowEndpoint extends DeductionWorkflowServiceGrpc.DeductionWorkflowServiceImplBase {
    protected final DeductionWorkflowApplication application;

    public DeductionWorkflowEndpoint(DeductionWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a deduction.
     */
    @Override
    public void startDeductionWorkflow(StartDeductionWorkflowRequest request, StreamObserver<StartDeductionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startDeductionWorkflow(request));
    }

    /**
     * Advances a deduction.
     */
    @Override
    public void advanceDeductionWorkflow(AdvanceDeductionWorkflowRequest request, StreamObserver<AdvanceDeductionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceDeductionWorkflow(request));
    }

    /**
     * Cancels a deduction.
     */
    @Override
    public void cancelDeductionWorkflow(CancelDeductionWorkflowRequest request, StreamObserver<CancelDeductionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelDeductionWorkflow(request));
    }

    /**
     * Retrys a deduction.
     */
    @Override
    public void retryDeductionWorkflow(RetryDeductionWorkflowRequest request, StreamObserver<RetryDeductionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryDeductionWorkflow(request));
    }

    /**
     * Pauses a deduction.
     */
    @Override
    public void pauseDeductionWorkflow(PauseDeductionWorkflowRequest request, StreamObserver<PauseDeductionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseDeductionWorkflow(request));
    }

    /**
     * Resumes a deduction.
     */
    @Override
    public void resumeDeductionWorkflow(ResumeDeductionWorkflowRequest request, StreamObserver<ResumeDeductionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeDeductionWorkflow(request));
    }

    /**
     * Returns a deduction.
     */
    @Override
    public void inspectDeductionWorkflow(InspectDeductionWorkflowRequest request, StreamObserver<InspectDeductionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectDeductionWorkflow(request));
    }

    /**
     * Lists deductions a page at a time.
     */
    @Override
    public void listDeductionWorkflows(ListDeductionWorkflowsRequest request, StreamObserver<ListDeductionWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listDeductionWorkflows(request));
    }

    /**
     * Signals a deduction.
     */
    @Override
    public void signalDeductionWorkflow(SignalDeductionWorkflowRequest request, StreamObserver<SignalDeductionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalDeductionWorkflow(request));
    }
}
