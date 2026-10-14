package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.ActivateEInvoiceRequest;
import com.acme.invoicing.v1.ActivateEInvoiceResponse;
import com.acme.invoicing.v1.EInvoiceLifecycleServiceGrpc;
import com.acme.invoicing.v1.ExpireEInvoiceRequest;
import com.acme.invoicing.v1.ExpireEInvoiceResponse;
import com.acme.invoicing.v1.GetEInvoiceLifecycleRequest;
import com.acme.invoicing.v1.GetEInvoiceLifecycleResponse;
import com.acme.invoicing.v1.ReinstateEInvoiceRequest;
import com.acme.invoicing.v1.ReinstateEInvoiceResponse;
import com.acme.invoicing.v1.RenewEInvoiceRequest;
import com.acme.invoicing.v1.RenewEInvoiceResponse;
import com.acme.invoicing.v1.ScheduleEInvoiceExpiryRequest;
import com.acme.invoicing.v1.ScheduleEInvoiceExpiryResponse;
import com.acme.invoicing.v1.SuspendEInvoiceRequest;
import com.acme.invoicing.v1.SuspendEInvoiceResponse;
import com.acme.invoicing.v1.TerminateEInvoiceRequest;
import com.acme.invoicing.v1.TerminateEInvoiceResponse;
import io.grpc.stub.StreamObserver;

/**
 * Moves an e invoice between the states of its lifecycle.
 */
public abstract class AbstractEInvoiceLifecycleEndpoint extends EInvoiceLifecycleServiceGrpc.EInvoiceLifecycleServiceImplBase {
    protected final EInvoiceLifecycleApplication application;

    protected AbstractEInvoiceLifecycleEndpoint(EInvoiceLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates an e invoice.
     */
    @Override
    public void activateEInvoice(ActivateEInvoiceRequest request, StreamObserver<ActivateEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateEInvoice(request));
    }

    /**
     * Suspends an e invoice.
     */
    @Override
    public void suspendEInvoice(SuspendEInvoiceRequest request, StreamObserver<SuspendEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendEInvoice(request));
    }

    /**
     * Reinstates an e invoice.
     */
    @Override
    public void reinstateEInvoice(ReinstateEInvoiceRequest request, StreamObserver<ReinstateEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateEInvoice(request));
    }

    /**
     * Terminates an e invoice.
     */
    @Override
    public void terminateEInvoice(TerminateEInvoiceRequest request, StreamObserver<TerminateEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateEInvoice(request));
    }

    /**
     * Renews an e invoice.
     */
    @Override
    public void renewEInvoice(RenewEInvoiceRequest request, StreamObserver<RenewEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewEInvoice(request));
    }

    /**
     * Expires an e invoice.
     */
    @Override
    public void expireEInvoice(ExpireEInvoiceRequest request, StreamObserver<ExpireEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireEInvoice(request));
    }

    /**
     * Returns an e invoice.
     */
    @Override
    public void getEInvoiceLifecycle(GetEInvoiceLifecycleRequest request, StreamObserver<GetEInvoiceLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getEInvoiceLifecycle(request));
    }

    /**
     * Schedules an e invoice.
     */
    @Override
    public void scheduleEInvoiceExpiry(ScheduleEInvoiceExpiryRequest request, StreamObserver<ScheduleEInvoiceExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleEInvoiceExpiry(request));
    }
}
