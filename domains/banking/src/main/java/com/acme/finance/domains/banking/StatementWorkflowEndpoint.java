package com.acme.finance.domains.banking;

import com.acme.banking.v1.AdvanceStatementWorkflowRequest;
import com.acme.banking.v1.AdvanceStatementWorkflowResponse;
import com.acme.banking.v1.CancelStatementWorkflowRequest;
import com.acme.banking.v1.CancelStatementWorkflowResponse;
import com.acme.banking.v1.InspectStatementWorkflowRequest;
import com.acme.banking.v1.InspectStatementWorkflowResponse;
import com.acme.banking.v1.ListStatementWorkflowsRequest;
import com.acme.banking.v1.ListStatementWorkflowsResponse;
import com.acme.banking.v1.PauseStatementWorkflowRequest;
import com.acme.banking.v1.PauseStatementWorkflowResponse;
import com.acme.banking.v1.ResumeStatementWorkflowRequest;
import com.acme.banking.v1.ResumeStatementWorkflowResponse;
import com.acme.banking.v1.RetryStatementWorkflowRequest;
import com.acme.banking.v1.RetryStatementWorkflowResponse;
import com.acme.banking.v1.SignalStatementWorkflowRequest;
import com.acme.banking.v1.SignalStatementWorkflowResponse;
import com.acme.banking.v1.StartStatementWorkflowRequest;
import com.acme.banking.v1.StartStatementWorkflowResponse;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a statement from start to finish.
 */
@GrpcService
public class StatementWorkflowEndpoint extends StatementWorkflowServiceGrpc.StatementWorkflowServiceImplBase {
    protected final StatementWorkflowApplication application;

    public StatementWorkflowEndpoint(StatementWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a statement.
     */
    @Override
    public void startStatementWorkflow(StartStatementWorkflowRequest request, StreamObserver<StartStatementWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startStatementWorkflow(request));
    }

    /**
     * Advances a statement.
     */
    @Override
    public void advanceStatementWorkflow(AdvanceStatementWorkflowRequest request, StreamObserver<AdvanceStatementWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceStatementWorkflow(request));
    }

    /**
     * Cancels a statement.
     */
    @Override
    public void cancelStatementWorkflow(CancelStatementWorkflowRequest request, StreamObserver<CancelStatementWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelStatementWorkflow(request));
    }

    /**
     * Retrys a statement.
     */
    @Override
    public void retryStatementWorkflow(RetryStatementWorkflowRequest request, StreamObserver<RetryStatementWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryStatementWorkflow(request));
    }

    /**
     * Pauses a statement.
     */
    @Override
    public void pauseStatementWorkflow(PauseStatementWorkflowRequest request, StreamObserver<PauseStatementWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseStatementWorkflow(request));
    }

    /**
     * Resumes a statement.
     */
    @Override
    public void resumeStatementWorkflow(ResumeStatementWorkflowRequest request, StreamObserver<ResumeStatementWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeStatementWorkflow(request));
    }

    /**
     * Returns a statement.
     */
    @Override
    public void inspectStatementWorkflow(InspectStatementWorkflowRequest request, StreamObserver<InspectStatementWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectStatementWorkflow(request));
    }

    /**
     * Lists statements a page at a time.
     */
    @Override
    public void listStatementWorkflows(ListStatementWorkflowsRequest request, StreamObserver<ListStatementWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listStatementWorkflows(request));
    }

    /**
     * Signals a statement.
     */
    @Override
    public void signalStatementWorkflow(SignalStatementWorkflowRequest request, StreamObserver<SignalStatementWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalStatementWorkflow(request));
    }
}
