package com.acme.finance.domains.billing;

import com.acme.billing.v1.ActivateCreditNoteRequest;
import com.acme.billing.v1.ActivateCreditNoteResponse;
import com.acme.billing.v1.CreditNoteLifecycleServiceGrpc;
import com.acme.billing.v1.ExpireCreditNoteRequest;
import com.acme.billing.v1.ExpireCreditNoteResponse;
import com.acme.billing.v1.GetCreditNoteLifecycleRequest;
import com.acme.billing.v1.GetCreditNoteLifecycleResponse;
import com.acme.billing.v1.ReinstateCreditNoteRequest;
import com.acme.billing.v1.ReinstateCreditNoteResponse;
import com.acme.billing.v1.RenewCreditNoteRequest;
import com.acme.billing.v1.RenewCreditNoteResponse;
import com.acme.billing.v1.ScheduleCreditNoteExpiryRequest;
import com.acme.billing.v1.ScheduleCreditNoteExpiryResponse;
import com.acme.billing.v1.SuspendCreditNoteRequest;
import com.acme.billing.v1.SuspendCreditNoteResponse;
import com.acme.billing.v1.TerminateCreditNoteRequest;
import com.acme.billing.v1.TerminateCreditNoteResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Moves a credit note between the states of its lifecycle.
 */
public abstract class AbstractCreditNoteLifecycleEndpoint extends CreditNoteLifecycleServiceGrpc.CreditNoteLifecycleServiceImplBase {
    protected final CreditNoteLifecycleApplication application;

    protected AbstractCreditNoteLifecycleEndpoint(CreditNoteLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a credit note.
     */
    @Override
    public void activateCreditNote(ActivateCreditNoteRequest request, StreamObserver<ActivateCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateCreditNote(request));
    }

    /**
     * Suspends a credit note.
     */
    @Override
    public void suspendCreditNote(SuspendCreditNoteRequest request, StreamObserver<SuspendCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendCreditNote(request));
    }

    /**
     * Reinstates a credit note.
     */
    @Override
    public void reinstateCreditNote(ReinstateCreditNoteRequest request, StreamObserver<ReinstateCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateCreditNote(request));
    }

    /**
     * Terminates a credit note.
     */
    @Override
    public void terminateCreditNote(TerminateCreditNoteRequest request, StreamObserver<TerminateCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateCreditNote(request));
    }

    /**
     * Renews a credit note.
     */
    @Override
    public void renewCreditNote(RenewCreditNoteRequest request, StreamObserver<RenewCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewCreditNote(request));
    }

    /**
     * Expires a credit note.
     */
    @Override
    public void expireCreditNote(ExpireCreditNoteRequest request, StreamObserver<ExpireCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireCreditNote(request));
    }

    /**
     * Returns a credit note.
     */
    @Override
    public void getCreditNoteLifecycle(GetCreditNoteLifecycleRequest request, StreamObserver<GetCreditNoteLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCreditNoteLifecycle(request));
    }

    /**
     * Schedules a credit note.
     */
    @Override
    public void scheduleCreditNoteExpiry(ScheduleCreditNoteExpiryRequest request, StreamObserver<ScheduleCreditNoteExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleCreditNoteExpiry(request));
    }
}
