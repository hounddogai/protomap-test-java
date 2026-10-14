package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.ActivateBankTransferRequest;
import com.acme.treasury.v1.ActivateBankTransferResponse;
import com.acme.treasury.v1.BankTransferLifecycleServiceGrpc;
import com.acme.treasury.v1.ExpireBankTransferRequest;
import com.acme.treasury.v1.ExpireBankTransferResponse;
import com.acme.treasury.v1.GetBankTransferLifecycleRequest;
import com.acme.treasury.v1.GetBankTransferLifecycleResponse;
import com.acme.treasury.v1.ReinstateBankTransferRequest;
import com.acme.treasury.v1.ReinstateBankTransferResponse;
import com.acme.treasury.v1.RenewBankTransferRequest;
import com.acme.treasury.v1.RenewBankTransferResponse;
import com.acme.treasury.v1.ScheduleBankTransferExpiryRequest;
import com.acme.treasury.v1.ScheduleBankTransferExpiryResponse;
import com.acme.treasury.v1.SuspendBankTransferRequest;
import com.acme.treasury.v1.SuspendBankTransferResponse;
import com.acme.treasury.v1.TerminateBankTransferRequest;
import com.acme.treasury.v1.TerminateBankTransferResponse;
import io.grpc.stub.StreamObserver;

/**
 * Moves a bank transfer between the states of its lifecycle.
 */
public abstract class AbstractBankTransferLifecycleEndpoint extends BankTransferLifecycleServiceGrpc.BankTransferLifecycleServiceImplBase {
    protected final BankTransferLifecycleApplication application;

    protected AbstractBankTransferLifecycleEndpoint(BankTransferLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a bank transfer.
     */
    @Override
    public void activateBankTransfer(ActivateBankTransferRequest request, StreamObserver<ActivateBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateBankTransfer(request));
    }

    /**
     * Suspends a bank transfer.
     */
    @Override
    public void suspendBankTransfer(SuspendBankTransferRequest request, StreamObserver<SuspendBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendBankTransfer(request));
    }

    /**
     * Reinstates a bank transfer.
     */
    @Override
    public void reinstateBankTransfer(ReinstateBankTransferRequest request, StreamObserver<ReinstateBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateBankTransfer(request));
    }

    /**
     * Terminates a bank transfer.
     */
    @Override
    public void terminateBankTransfer(TerminateBankTransferRequest request, StreamObserver<TerminateBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateBankTransfer(request));
    }

    /**
     * Renews a bank transfer.
     */
    @Override
    public void renewBankTransfer(RenewBankTransferRequest request, StreamObserver<RenewBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewBankTransfer(request));
    }

    /**
     * Expires a bank transfer.
     */
    @Override
    public void expireBankTransfer(ExpireBankTransferRequest request, StreamObserver<ExpireBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireBankTransfer(request));
    }

    /**
     * Returns a bank transfer.
     */
    @Override
    public void getBankTransferLifecycle(GetBankTransferLifecycleRequest request, StreamObserver<GetBankTransferLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBankTransferLifecycle(request));
    }

    /**
     * Schedules a bank transfer.
     */
    @Override
    public void scheduleBankTransferExpiry(ScheduleBankTransferExpiryRequest request, StreamObserver<ScheduleBankTransferExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleBankTransferExpiry(request));
    }
}
