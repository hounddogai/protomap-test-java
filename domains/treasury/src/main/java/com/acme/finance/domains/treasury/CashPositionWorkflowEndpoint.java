package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.AdvanceCashPositionWorkflowRequest;
import com.acme.treasury.v1.AdvanceCashPositionWorkflowResponse;
import com.acme.treasury.v1.CashPositionWorkflowServiceGrpc;
import com.acme.treasury.v1.InspectCashPositionWorkflowRequest;
import com.acme.treasury.v1.InspectCashPositionWorkflowResponse;
import com.acme.treasury.v1.ListCashPositionWorkflowsRequest;
import com.acme.treasury.v1.ListCashPositionWorkflowsResponse;
import com.acme.treasury.v1.ResumeCashPositionWorkflowRequest;
import com.acme.treasury.v1.ResumeCashPositionWorkflowResponse;
import com.acme.treasury.v1.RetryCashPositionWorkflowRequest;
import com.acme.treasury.v1.RetryCashPositionWorkflowResponse;
import com.acme.treasury.v1.SignalCashPositionWorkflowRequest;
import com.acme.treasury.v1.SignalCashPositionWorkflowResponse;
import com.acme.treasury.v1.StartCashPositionWorkflowRequest;
import com.acme.treasury.v1.StartCashPositionWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a cash position from start to finish.
 */
@GrpcService
public class CashPositionWorkflowEndpoint extends CashPositionWorkflowServiceGrpc.CashPositionWorkflowServiceImplBase {
    protected final CashPositionWorkflowApplication application;

    public CashPositionWorkflowEndpoint(CashPositionWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a cash position.
     */
    @Override
    public void startCashPositionWorkflow(StartCashPositionWorkflowRequest request, StreamObserver<StartCashPositionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startCashPositionWorkflow(request));
    }

    /**
     * Advances a cash position.
     */
    @Override
    public void advanceCashPositionWorkflow(AdvanceCashPositionWorkflowRequest request, StreamObserver<AdvanceCashPositionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceCashPositionWorkflow(request));
    }

    /**
     * Retrys a cash position.
     */
    @Override
    public void retryCashPositionWorkflow(RetryCashPositionWorkflowRequest request, StreamObserver<RetryCashPositionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryCashPositionWorkflow(request));
    }

    /**
     * Resumes a cash position.
     */
    @Override
    public void resumeCashPositionWorkflow(ResumeCashPositionWorkflowRequest request, StreamObserver<ResumeCashPositionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeCashPositionWorkflow(request));
    }

    /**
     * Returns a cash position.
     */
    @Override
    public void inspectCashPositionWorkflow(InspectCashPositionWorkflowRequest request, StreamObserver<InspectCashPositionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectCashPositionWorkflow(request));
    }

    /**
     * Lists cash positions a page at a time.
     */
    @Override
    public void listCashPositionWorkflows(ListCashPositionWorkflowsRequest request, StreamObserver<ListCashPositionWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listCashPositionWorkflows(request));
    }

    /**
     * Signals a cash position.
     */
    @Override
    public void signalCashPositionWorkflow(SignalCashPositionWorkflowRequest request, StreamObserver<SignalCashPositionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalCashPositionWorkflow(request));
    }
}
