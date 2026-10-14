package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ActivateVarianceRequest;
import com.acme.budgeting.v1.ActivateVarianceResponse;
import com.acme.budgeting.v1.ExpireVarianceRequest;
import com.acme.budgeting.v1.ExpireVarianceResponse;
import com.acme.budgeting.v1.GetVarianceLifecycleRequest;
import com.acme.budgeting.v1.GetVarianceLifecycleResponse;
import com.acme.budgeting.v1.RenewVarianceRequest;
import com.acme.budgeting.v1.RenewVarianceResponse;
import com.acme.budgeting.v1.ScheduleVarianceExpiryRequest;
import com.acme.budgeting.v1.ScheduleVarianceExpiryResponse;
import com.acme.budgeting.v1.SuspendVarianceRequest;
import com.acme.budgeting.v1.SuspendVarianceResponse;
import com.acme.budgeting.v1.TerminateVarianceRequest;
import com.acme.budgeting.v1.TerminateVarianceResponse;
import com.acme.budgeting.v1.VarianceLifecycleServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Moves a variance between the states of its lifecycle.
 */
public abstract class AbstractVarianceLifecycleEndpoint extends VarianceLifecycleServiceGrpc.VarianceLifecycleServiceImplBase {
    protected final VarianceLifecycleApplication application;

    protected AbstractVarianceLifecycleEndpoint(VarianceLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a variance.
     */
    @Override
    public void activateVariance(ActivateVarianceRequest request, StreamObserver<ActivateVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateVariance(request));
    }

    /**
     * Suspends a variance.
     */
    @Override
    public void suspendVariance(SuspendVarianceRequest request, StreamObserver<SuspendVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendVariance(request));
    }

    /**
     * Terminates a variance.
     */
    @Override
    public void terminateVariance(TerminateVarianceRequest request, StreamObserver<TerminateVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateVariance(request));
    }

    /**
     * Renews a variance.
     */
    @Override
    public void renewVariance(RenewVarianceRequest request, StreamObserver<RenewVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewVariance(request));
    }

    /**
     * Expires a variance.
     */
    @Override
    public void expireVariance(ExpireVarianceRequest request, StreamObserver<ExpireVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireVariance(request));
    }

    /**
     * Returns a variance.
     */
    @Override
    public void getVarianceLifecycle(GetVarianceLifecycleRequest request, StreamObserver<GetVarianceLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getVarianceLifecycle(request));
    }

    /**
     * Schedules a variance.
     */
    @Override
    public void scheduleVarianceExpiry(ScheduleVarianceExpiryRequest request, StreamObserver<ScheduleVarianceExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleVarianceExpiry(request));
    }
}
