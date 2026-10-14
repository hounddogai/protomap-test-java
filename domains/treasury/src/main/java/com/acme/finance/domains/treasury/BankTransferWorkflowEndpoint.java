package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.AdvanceBankTransferWorkflowRequest;
import com.acme.treasury.v1.AdvanceBankTransferWorkflowResponse;
import com.acme.treasury.v1.BankTransferWorkflowServiceGrpc;
import com.acme.treasury.v1.CancelBankTransferWorkflowRequest;
import com.acme.treasury.v1.CancelBankTransferWorkflowResponse;
import com.acme.treasury.v1.InspectBankTransferWorkflowRequest;
import com.acme.treasury.v1.InspectBankTransferWorkflowResponse;
import com.acme.treasury.v1.ListBankTransferWorkflowsRequest;
import com.acme.treasury.v1.ListBankTransferWorkflowsResponse;
import com.acme.treasury.v1.PauseBankTransferWorkflowRequest;
import com.acme.treasury.v1.PauseBankTransferWorkflowResponse;
import com.acme.treasury.v1.ResumeBankTransferWorkflowRequest;
import com.acme.treasury.v1.ResumeBankTransferWorkflowResponse;
import com.acme.treasury.v1.RetryBankTransferWorkflowRequest;
import com.acme.treasury.v1.RetryBankTransferWorkflowResponse;
import com.acme.treasury.v1.SignalBankTransferWorkflowRequest;
import com.acme.treasury.v1.SignalBankTransferWorkflowResponse;
import com.acme.treasury.v1.StartBankTransferWorkflowRequest;
import com.acme.treasury.v1.StartBankTransferWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a bank transfer from start to finish.
 */
@GrpcService
public class BankTransferWorkflowEndpoint extends BankTransferWorkflowServiceGrpc.BankTransferWorkflowServiceImplBase {
    protected final BankTransferWorkflowApplication application;

    public BankTransferWorkflowEndpoint(BankTransferWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a bank transfer.
     */
    @Override
    public void startBankTransferWorkflow(StartBankTransferWorkflowRequest request, StreamObserver<StartBankTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startBankTransferWorkflow(request));
    }

    /**
     * Advances a bank transfer.
     */
    @Override
    public void advanceBankTransferWorkflow(AdvanceBankTransferWorkflowRequest request, StreamObserver<AdvanceBankTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceBankTransferWorkflow(request));
    }

    /**
     * Cancels a bank transfer.
     */
    @Override
    public void cancelBankTransferWorkflow(CancelBankTransferWorkflowRequest request, StreamObserver<CancelBankTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelBankTransferWorkflow(request));
    }

    /**
     * Retrys a bank transfer.
     */
    @Override
    public void retryBankTransferWorkflow(RetryBankTransferWorkflowRequest request, StreamObserver<RetryBankTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryBankTransferWorkflow(request));
    }

    /**
     * Pauses a bank transfer.
     */
    @Override
    public void pauseBankTransferWorkflow(PauseBankTransferWorkflowRequest request, StreamObserver<PauseBankTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseBankTransferWorkflow(request));
    }

    /**
     * Resumes a bank transfer.
     */
    @Override
    public void resumeBankTransferWorkflow(ResumeBankTransferWorkflowRequest request, StreamObserver<ResumeBankTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeBankTransferWorkflow(request));
    }

    /**
     * Returns a bank transfer.
     */
    @Override
    public void inspectBankTransferWorkflow(InspectBankTransferWorkflowRequest request, StreamObserver<InspectBankTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectBankTransferWorkflow(request));
    }

    /**
     * Lists bank transfers a page at a time.
     */
    @Override
    public void listBankTransferWorkflows(ListBankTransferWorkflowsRequest request, StreamObserver<ListBankTransferWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listBankTransferWorkflows(request));
    }

    /**
     * Signals a bank transfer.
     */
    @Override
    public void signalBankTransferWorkflow(SignalBankTransferWorkflowRequest request, StreamObserver<SignalBankTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalBankTransferWorkflow(request));
    }
}
