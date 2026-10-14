package com.acme.finance.domains.billing;

import com.acme.billing.v1.ActivatePaymentTermRequest;
import com.acme.billing.v1.ActivatePaymentTermResponse;
import com.acme.billing.v1.ExpirePaymentTermRequest;
import com.acme.billing.v1.ExpirePaymentTermResponse;
import com.acme.billing.v1.GetPaymentTermLifecycleRequest;
import com.acme.billing.v1.GetPaymentTermLifecycleResponse;
import com.acme.billing.v1.PaymentTermLifecycleServiceGrpc;
import com.acme.billing.v1.ReinstatePaymentTermRequest;
import com.acme.billing.v1.ReinstatePaymentTermResponse;
import com.acme.billing.v1.RenewPaymentTermRequest;
import com.acme.billing.v1.RenewPaymentTermResponse;
import com.acme.billing.v1.SchedulePaymentTermExpiryRequest;
import com.acme.billing.v1.SchedulePaymentTermExpiryResponse;
import com.acme.billing.v1.SuspendPaymentTermRequest;
import com.acme.billing.v1.SuspendPaymentTermResponse;
import com.acme.billing.v1.TerminatePaymentTermRequest;
import com.acme.billing.v1.TerminatePaymentTermResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a payment term between the states of its lifecycle.
 */
@GrpcService
public class PaymentTermLifecycleEndpoint extends PaymentTermLifecycleServiceGrpc.PaymentTermLifecycleServiceImplBase {
    protected final PaymentTermLifecycleApplication application;

    public PaymentTermLifecycleEndpoint(PaymentTermLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a payment term.
     */
    @Override
    public void activatePaymentTerm(ActivatePaymentTermRequest request, StreamObserver<ActivatePaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.activatePaymentTerm(request));
    }

    /**
     * Suspends a payment term.
     */
    @Override
    public void suspendPaymentTerm(SuspendPaymentTermRequest request, StreamObserver<SuspendPaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendPaymentTerm(request));
    }

    /**
     * Reinstates a payment term.
     */
    @Override
    public void reinstatePaymentTerm(ReinstatePaymentTermRequest request, StreamObserver<ReinstatePaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstatePaymentTerm(request));
    }

    /**
     * Terminates a payment term.
     */
    @Override
    public void terminatePaymentTerm(TerminatePaymentTermRequest request, StreamObserver<TerminatePaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminatePaymentTerm(request));
    }

    /**
     * Renews a payment term.
     */
    @Override
    public void renewPaymentTerm(RenewPaymentTermRequest request, StreamObserver<RenewPaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewPaymentTerm(request));
    }

    /**
     * Expires a payment term.
     */
    @Override
    public void expirePaymentTerm(ExpirePaymentTermRequest request, StreamObserver<ExpirePaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.expirePaymentTerm(request));
    }

    /**
     * Returns a payment term.
     */
    @Override
    public void getPaymentTermLifecycle(GetPaymentTermLifecycleRequest request, StreamObserver<GetPaymentTermLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPaymentTermLifecycle(request));
    }

    /**
     * Schedules a payment term.
     */
    @Override
    public void schedulePaymentTermExpiry(SchedulePaymentTermExpiryRequest request, StreamObserver<SchedulePaymentTermExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.schedulePaymentTermExpiry(request));
    }
}
