package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.ActivateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ActivateInvoiceTemplateResponse;
import com.acme.invoicing.v1.ExpireInvoiceTemplateRequest;
import com.acme.invoicing.v1.ExpireInvoiceTemplateResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateLifecycleRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateLifecycleResponse;
import com.acme.invoicing.v1.InvoiceTemplateLifecycleServiceGrpc;
import com.acme.invoicing.v1.ReinstateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ReinstateInvoiceTemplateResponse;
import com.acme.invoicing.v1.RenewInvoiceTemplateRequest;
import com.acme.invoicing.v1.RenewInvoiceTemplateResponse;
import com.acme.invoicing.v1.ScheduleInvoiceTemplateExpiryRequest;
import com.acme.invoicing.v1.ScheduleInvoiceTemplateExpiryResponse;
import com.acme.invoicing.v1.SuspendInvoiceTemplateRequest;
import com.acme.invoicing.v1.SuspendInvoiceTemplateResponse;
import com.acme.invoicing.v1.TerminateInvoiceTemplateRequest;
import com.acme.invoicing.v1.TerminateInvoiceTemplateResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves an invoice template between the states of its lifecycle.
 */
@GrpcService
public class InvoiceTemplateLifecycleEndpoint extends InvoiceTemplateLifecycleServiceGrpc.InvoiceTemplateLifecycleServiceImplBase {
    protected final InvoiceTemplateLifecycleApplication application;

    public InvoiceTemplateLifecycleEndpoint(InvoiceTemplateLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates an invoice template.
     */
    @Override
    public void activateInvoiceTemplate(ActivateInvoiceTemplateRequest request, StreamObserver<ActivateInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateInvoiceTemplate(request));
    }

    /**
     * Suspends an invoice template.
     */
    @Override
    public void suspendInvoiceTemplate(SuspendInvoiceTemplateRequest request, StreamObserver<SuspendInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendInvoiceTemplate(request));
    }

    /**
     * Reinstates an invoice template.
     */
    @Override
    public void reinstateInvoiceTemplate(ReinstateInvoiceTemplateRequest request, StreamObserver<ReinstateInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateInvoiceTemplate(request));
    }

    /**
     * Terminates an invoice template.
     */
    @Override
    public void terminateInvoiceTemplate(TerminateInvoiceTemplateRequest request, StreamObserver<TerminateInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateInvoiceTemplate(request));
    }

    /**
     * Renews an invoice template.
     */
    @Override
    public void renewInvoiceTemplate(RenewInvoiceTemplateRequest request, StreamObserver<RenewInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewInvoiceTemplate(request));
    }

    /**
     * Expires an invoice template.
     */
    @Override
    public void expireInvoiceTemplate(ExpireInvoiceTemplateRequest request, StreamObserver<ExpireInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireInvoiceTemplate(request));
    }

    /**
     * Returns an invoice template.
     */
    @Override
    public void getInvoiceTemplateLifecycle(GetInvoiceTemplateLifecycleRequest request, StreamObserver<GetInvoiceTemplateLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getInvoiceTemplateLifecycle(request));
    }

    /**
     * Schedules an invoice template.
     */
    @Override
    public void scheduleInvoiceTemplateExpiry(ScheduleInvoiceTemplateExpiryRequest request, StreamObserver<ScheduleInvoiceTemplateExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleInvoiceTemplateExpiry(request));
    }
}
