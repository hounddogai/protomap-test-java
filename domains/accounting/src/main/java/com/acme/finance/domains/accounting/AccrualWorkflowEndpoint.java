package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AccrualWorkflowServiceGrpc;
import com.acme.accounting.v1.AdvanceAccrualWorkflowRequest;
import com.acme.accounting.v1.AdvanceAccrualWorkflowResponse;
import com.acme.accounting.v1.CancelAccrualWorkflowRequest;
import com.acme.accounting.v1.CancelAccrualWorkflowResponse;
import com.acme.accounting.v1.InspectAccrualWorkflowRequest;
import com.acme.accounting.v1.InspectAccrualWorkflowResponse;
import com.acme.accounting.v1.ListAccrualWorkflowsRequest;
import com.acme.accounting.v1.ListAccrualWorkflowsResponse;
import com.acme.accounting.v1.PauseAccrualWorkflowRequest;
import com.acme.accounting.v1.PauseAccrualWorkflowResponse;
import com.acme.accounting.v1.ResumeAccrualWorkflowRequest;
import com.acme.accounting.v1.ResumeAccrualWorkflowResponse;
import com.acme.accounting.v1.RetryAccrualWorkflowRequest;
import com.acme.accounting.v1.RetryAccrualWorkflowResponse;
import com.acme.accounting.v1.SignalAccrualWorkflowRequest;
import com.acme.accounting.v1.SignalAccrualWorkflowResponse;
import com.acme.accounting.v1.StartAccrualWorkflowRequest;
import com.acme.accounting.v1.StartAccrualWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes an accrual from start to finish.
 */
@GrpcService
public class AccrualWorkflowEndpoint extends AccrualWorkflowServiceGrpc.AccrualWorkflowServiceImplBase {
    protected final AccrualWorkflowApplication application;

    public AccrualWorkflowEndpoint(AccrualWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an accrual.
     */
    @Override
    public void startAccrualWorkflow(StartAccrualWorkflowRequest request, StreamObserver<StartAccrualWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startAccrualWorkflow(request));
    }

    /**
     * Advances an accrual.
     */
    @Override
    public void advanceAccrualWorkflow(AdvanceAccrualWorkflowRequest request, StreamObserver<AdvanceAccrualWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceAccrualWorkflow(request));
    }

    /**
     * Cancels an accrual.
     */
    @Override
    public void cancelAccrualWorkflow(CancelAccrualWorkflowRequest request, StreamObserver<CancelAccrualWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelAccrualWorkflow(request));
    }

    /**
     * Retrys an accrual.
     */
    @Override
    public void retryAccrualWorkflow(RetryAccrualWorkflowRequest request, StreamObserver<RetryAccrualWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryAccrualWorkflow(request));
    }

    /**
     * Pauses an accrual.
     */
    @Override
    public void pauseAccrualWorkflow(PauseAccrualWorkflowRequest request, StreamObserver<PauseAccrualWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseAccrualWorkflow(request));
    }

    /**
     * Resumes an accrual.
     */
    @Override
    public void resumeAccrualWorkflow(ResumeAccrualWorkflowRequest request, StreamObserver<ResumeAccrualWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeAccrualWorkflow(request));
    }

    /**
     * Returns an accrual.
     */
    @Override
    public void inspectAccrualWorkflow(InspectAccrualWorkflowRequest request, StreamObserver<InspectAccrualWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectAccrualWorkflow(request));
    }

    /**
     * Lists accruals a page at a time.
     */
    @Override
    public void listAccrualWorkflows(ListAccrualWorkflowsRequest request, StreamObserver<ListAccrualWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAccrualWorkflows(request));
    }

    /**
     * Signals an accrual.
     */
    @Override
    public void signalAccrualWorkflow(SignalAccrualWorkflowRequest request, StreamObserver<SignalAccrualWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalAccrualWorkflow(request));
    }
}
