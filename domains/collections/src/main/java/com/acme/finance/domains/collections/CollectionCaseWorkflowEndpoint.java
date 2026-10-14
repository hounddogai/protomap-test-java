package com.acme.finance.domains.collections;

import com.acme.collections.v1.AdvanceCollectionCaseWorkflowRequest;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowResponse;
import com.acme.collections.v1.CancelCollectionCaseWorkflowRequest;
import com.acme.collections.v1.CancelCollectionCaseWorkflowResponse;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.InspectCollectionCaseWorkflowRequest;
import com.acme.collections.v1.InspectCollectionCaseWorkflowResponse;
import com.acme.collections.v1.ListCollectionCaseWorkflowsRequest;
import com.acme.collections.v1.ListCollectionCaseWorkflowsResponse;
import com.acme.collections.v1.PauseCollectionCaseWorkflowRequest;
import com.acme.collections.v1.PauseCollectionCaseWorkflowResponse;
import com.acme.collections.v1.ResumeCollectionCaseWorkflowRequest;
import com.acme.collections.v1.ResumeCollectionCaseWorkflowResponse;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.collections.v1.SignalCollectionCaseWorkflowRequest;
import com.acme.collections.v1.SignalCollectionCaseWorkflowResponse;
import com.acme.collections.v1.StartCollectionCaseWorkflowRequest;
import com.acme.collections.v1.StartCollectionCaseWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a collection case from start to finish.
 */
@GrpcService
public class CollectionCaseWorkflowEndpoint extends CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceImplBase {
    protected final CollectionCaseWorkflowApplication application;

    public CollectionCaseWorkflowEndpoint(CollectionCaseWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a collection case.
     */
    @Override
    public void startCollectionCaseWorkflow(StartCollectionCaseWorkflowRequest request, StreamObserver<StartCollectionCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startCollectionCaseWorkflow(request));
    }

    /**
     * Advances a collection case.
     */
    @Override
    public void advanceCollectionCaseWorkflow(AdvanceCollectionCaseWorkflowRequest request, StreamObserver<AdvanceCollectionCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceCollectionCaseWorkflow(request));
    }

    /**
     * Cancels a collection case.
     */
    @Override
    public void cancelCollectionCaseWorkflow(CancelCollectionCaseWorkflowRequest request, StreamObserver<CancelCollectionCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelCollectionCaseWorkflow(request));
    }

    /**
     * Retrys a collection case.
     */
    @Override
    public void retryCollectionCaseWorkflow(RetryCollectionCaseWorkflowRequest request, StreamObserver<RetryCollectionCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryCollectionCaseWorkflow(request));
    }

    /**
     * Pauses a collection case.
     */
    @Override
    public void pauseCollectionCaseWorkflow(PauseCollectionCaseWorkflowRequest request, StreamObserver<PauseCollectionCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseCollectionCaseWorkflow(request));
    }

    /**
     * Resumes a collection case.
     */
    @Override
    public void resumeCollectionCaseWorkflow(ResumeCollectionCaseWorkflowRequest request, StreamObserver<ResumeCollectionCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeCollectionCaseWorkflow(request));
    }

    /**
     * Returns a collection case.
     */
    @Override
    public void inspectCollectionCaseWorkflow(InspectCollectionCaseWorkflowRequest request, StreamObserver<InspectCollectionCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectCollectionCaseWorkflow(request));
    }

    /**
     * Lists collection cases a page at a time.
     */
    @Override
    public void listCollectionCaseWorkflows(ListCollectionCaseWorkflowsRequest request, StreamObserver<ListCollectionCaseWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listCollectionCaseWorkflows(request));
    }

    /**
     * Signals a collection case.
     */
    @Override
    public void signalCollectionCaseWorkflow(SignalCollectionCaseWorkflowRequest request, StreamObserver<SignalCollectionCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalCollectionCaseWorkflow(request));
    }
}
