package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ActivateCoverageRequest;
import com.acme.insurance.v1.ActivateCoverageResponse;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.ExpireCoverageRequest;
import com.acme.insurance.v1.ExpireCoverageResponse;
import com.acme.insurance.v1.GetCoverageLifecycleRequest;
import com.acme.insurance.v1.GetCoverageLifecycleResponse;
import com.acme.insurance.v1.ReinstateCoverageRequest;
import com.acme.insurance.v1.ReinstateCoverageResponse;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.insurance.v1.SuspendCoverageRequest;
import com.acme.insurance.v1.SuspendCoverageResponse;
import com.acme.insurance.v1.TerminateCoverageRequest;
import com.acme.insurance.v1.TerminateCoverageResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a coverage between the states of its lifecycle.
 */
@GrpcService
public class CoverageLifecycleEndpoint extends CoverageLifecycleServiceGrpc.CoverageLifecycleServiceImplBase {
    protected final CoverageLifecycleApplication application;

    public CoverageLifecycleEndpoint(CoverageLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a coverage.
     */
    @Override
    public void activateCoverage(ActivateCoverageRequest request, StreamObserver<ActivateCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateCoverage(request));
    }

    /**
     * Suspends a coverage.
     */
    @Override
    public void suspendCoverage(SuspendCoverageRequest request, StreamObserver<SuspendCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendCoverage(request));
    }

    /**
     * Reinstates a coverage.
     */
    @Override
    public void reinstateCoverage(ReinstateCoverageRequest request, StreamObserver<ReinstateCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateCoverage(request));
    }

    /**
     * Terminates a coverage.
     */
    @Override
    public void terminateCoverage(TerminateCoverageRequest request, StreamObserver<TerminateCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateCoverage(request));
    }

    /**
     * Expires a coverage.
     */
    @Override
    public void expireCoverage(ExpireCoverageRequest request, StreamObserver<ExpireCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireCoverage(request));
    }

    /**
     * Returns a coverage.
     */
    @Override
    public void getCoverageLifecycle(GetCoverageLifecycleRequest request, StreamObserver<GetCoverageLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCoverageLifecycle(request));
    }

    /**
     * Schedules a coverage.
     */
    @Override
    public void scheduleCoverageExpiry(ScheduleCoverageExpiryRequest request, StreamObserver<ScheduleCoverageExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleCoverageExpiry(request));
    }
}
