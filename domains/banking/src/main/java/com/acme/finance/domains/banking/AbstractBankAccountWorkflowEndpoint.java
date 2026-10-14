package com.acme.finance.domains.banking;

import com.acme.banking.v1.AdvanceBankAccountWorkflowRequest;
import com.acme.banking.v1.AdvanceBankAccountWorkflowResponse;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.CancelBankAccountWorkflowRequest;
import com.acme.banking.v1.CancelBankAccountWorkflowResponse;
import com.acme.banking.v1.InspectBankAccountWorkflowRequest;
import com.acme.banking.v1.InspectBankAccountWorkflowResponse;
import com.acme.banking.v1.ListBankAccountWorkflowsRequest;
import com.acme.banking.v1.ListBankAccountWorkflowsResponse;
import com.acme.banking.v1.ResumeBankAccountWorkflowRequest;
import com.acme.banking.v1.ResumeBankAccountWorkflowResponse;
import com.acme.banking.v1.SignalBankAccountWorkflowRequest;
import com.acme.banking.v1.SignalBankAccountWorkflowResponse;
import com.acme.banking.v1.StartBankAccountWorkflowRequest;
import com.acme.banking.v1.StartBankAccountWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes a bank account from start to finish.
 */
public abstract class AbstractBankAccountWorkflowEndpoint extends BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceImplBase {
    protected final BankAccountWorkflowApplication application;

    protected AbstractBankAccountWorkflowEndpoint(BankAccountWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a bank account.
     */
    @Override
    public void startBankAccountWorkflow(StartBankAccountWorkflowRequest request, StreamObserver<StartBankAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startBankAccountWorkflow(request));
    }

    /**
     * Advances a bank account.
     */
    @Override
    public void advanceBankAccountWorkflow(AdvanceBankAccountWorkflowRequest request, StreamObserver<AdvanceBankAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceBankAccountWorkflow(request));
    }

    /**
     * Cancels a bank account.
     */
    @Override
    public void cancelBankAccountWorkflow(CancelBankAccountWorkflowRequest request, StreamObserver<CancelBankAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelBankAccountWorkflow(request));
    }

    /**
     * Resumes a bank account.
     */
    @Override
    public void resumeBankAccountWorkflow(ResumeBankAccountWorkflowRequest request, StreamObserver<ResumeBankAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeBankAccountWorkflow(request));
    }

    /**
     * Returns a bank account.
     */
    @Override
    public void inspectBankAccountWorkflow(InspectBankAccountWorkflowRequest request, StreamObserver<InspectBankAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectBankAccountWorkflow(request));
    }

    /**
     * Lists bank accounts a page at a time.
     */
    @Override
    public void listBankAccountWorkflows(ListBankAccountWorkflowsRequest request, StreamObserver<ListBankAccountWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listBankAccountWorkflows(request));
    }

    /**
     * Signals a bank account.
     */
    @Override
    public void signalBankAccountWorkflow(SignalBankAccountWorkflowRequest request, StreamObserver<SignalBankAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalBankAccountWorkflow(request));
    }
}
