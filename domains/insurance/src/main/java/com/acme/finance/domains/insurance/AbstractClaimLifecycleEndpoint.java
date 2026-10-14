package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ActivateClaimRequest;
import com.acme.insurance.v1.ActivateClaimResponse;
import com.acme.insurance.v1.ClaimLifecycleServiceGrpc;
import com.acme.insurance.v1.ExpireClaimRequest;
import com.acme.insurance.v1.ExpireClaimResponse;
import com.acme.insurance.v1.ReinstateClaimRequest;
import com.acme.insurance.v1.ReinstateClaimResponse;
import com.acme.insurance.v1.RenewClaimRequest;
import com.acme.insurance.v1.RenewClaimResponse;
import com.acme.insurance.v1.ScheduleClaimExpiryRequest;
import com.acme.insurance.v1.ScheduleClaimExpiryResponse;
import com.acme.insurance.v1.SuspendClaimRequest;
import com.acme.insurance.v1.SuspendClaimResponse;
import com.acme.insurance.v1.TerminateClaimRequest;
import com.acme.insurance.v1.TerminateClaimResponse;
import io.grpc.stub.StreamObserver;

/**
 * Moves a claim between the states of its lifecycle.
 */
public abstract class AbstractClaimLifecycleEndpoint extends ClaimLifecycleServiceGrpc.ClaimLifecycleServiceImplBase {
    protected final ClaimLifecycleApplication application;

    protected AbstractClaimLifecycleEndpoint(ClaimLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a claim.
     */
    @Override
    public void activateClaim(ActivateClaimRequest request, StreamObserver<ActivateClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateClaim(request));
    }

    /**
     * Suspends a claim.
     */
    @Override
    public void suspendClaim(SuspendClaimRequest request, StreamObserver<SuspendClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendClaim(request));
    }

    /**
     * Reinstates a claim.
     */
    @Override
    public void reinstateClaim(ReinstateClaimRequest request, StreamObserver<ReinstateClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateClaim(request));
    }

    /**
     * Terminates a claim.
     */
    @Override
    public void terminateClaim(TerminateClaimRequest request, StreamObserver<TerminateClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateClaim(request));
    }

    /**
     * Renews a claim.
     */
    @Override
    public void renewClaim(RenewClaimRequest request, StreamObserver<RenewClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewClaim(request));
    }

    /**
     * Expires a claim.
     */
    @Override
    public void expireClaim(ExpireClaimRequest request, StreamObserver<ExpireClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireClaim(request));
    }

    /**
     * Schedules a claim.
     */
    @Override
    public void scheduleClaimExpiry(ScheduleClaimExpiryRequest request, StreamObserver<ScheduleClaimExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleClaimExpiry(request));
    }
}
