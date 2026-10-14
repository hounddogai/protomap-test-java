package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.ActivateRepaymentRequest;
import com.acme.lending.v1.ActivateRepaymentResponse;
import com.acme.lending.v1.ExpireRepaymentRequest;
import com.acme.lending.v1.ExpireRepaymentResponse;
import com.acme.lending.v1.GetRepaymentLifecycleRequest;
import com.acme.lending.v1.GetRepaymentLifecycleResponse;
import com.acme.lending.v1.RenewRepaymentRequest;
import com.acme.lending.v1.RenewRepaymentResponse;
import com.acme.lending.v1.RepaymentLifecycleServiceGrpc;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.lending.v1.SuspendRepaymentRequest;
import com.acme.lending.v1.SuspendRepaymentResponse;
import com.acme.lending.v1.TerminateRepaymentRequest;
import com.acme.lending.v1.TerminateRepaymentResponse;
import io.grpc.stub.StreamObserver;

/**
 * Moves a repayment between the states of its lifecycle.
 */
public abstract class AbstractRepaymentLifecycleEndpoint extends RepaymentLifecycleServiceGrpc.RepaymentLifecycleServiceImplBase {
    protected final RepaymentLifecycleApplication application;

    protected AbstractRepaymentLifecycleEndpoint(RepaymentLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a repayment.
     */
    @Override
    public void activateRepayment(ActivateRepaymentRequest request, StreamObserver<ActivateRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateRepayment(request));
    }

    /**
     * Suspends a repayment.
     */
    @Override
    public void suspendRepayment(SuspendRepaymentRequest request, StreamObserver<SuspendRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendRepayment(request));
    }

    /**
     * Terminates a repayment.
     */
    @Override
    public void terminateRepayment(TerminateRepaymentRequest request, StreamObserver<TerminateRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateRepayment(request));
    }

    /**
     * Renews a repayment.
     */
    @Override
    public void renewRepayment(RenewRepaymentRequest request, StreamObserver<RenewRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewRepayment(request));
    }

    /**
     * Expires a repayment.
     */
    @Override
    public void expireRepayment(ExpireRepaymentRequest request, StreamObserver<ExpireRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireRepayment(request));
    }

    /**
     * Returns a repayment.
     */
    @Override
    public void getRepaymentLifecycle(GetRepaymentLifecycleRequest request, StreamObserver<GetRepaymentLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRepaymentLifecycle(request));
    }

    /**
     * Schedules a repayment.
     */
    @Override
    public void scheduleRepaymentExpiry(ScheduleRepaymentExpiryRequest request, StreamObserver<ScheduleRepaymentExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleRepaymentExpiry(request));
    }
}
