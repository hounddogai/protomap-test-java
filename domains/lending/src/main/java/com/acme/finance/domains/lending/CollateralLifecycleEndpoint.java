package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.ActivateCollateralRequest;
import com.acme.lending.v1.ActivateCollateralResponse;
import com.acme.lending.v1.CollateralLifecycleServiceGrpc;
import com.acme.lending.v1.GetCollateralLifecycleRequest;
import com.acme.lending.v1.GetCollateralLifecycleResponse;
import com.acme.lending.v1.ReinstateCollateralRequest;
import com.acme.lending.v1.ReinstateCollateralResponse;
import com.acme.lending.v1.RenewCollateralRequest;
import com.acme.lending.v1.RenewCollateralResponse;
import com.acme.lending.v1.SuspendCollateralRequest;
import com.acme.lending.v1.SuspendCollateralResponse;
import com.acme.lending.v1.TerminateCollateralRequest;
import com.acme.lending.v1.TerminateCollateralResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a collateral between the states of its lifecycle.
 */
@GrpcService
public class CollateralLifecycleEndpoint extends CollateralLifecycleServiceGrpc.CollateralLifecycleServiceImplBase {
    protected final CollateralLifecycleApplication application;

    public CollateralLifecycleEndpoint(CollateralLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a collateral.
     */
    @Override
    public void activateCollateral(ActivateCollateralRequest request, StreamObserver<ActivateCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateCollateral(request));
    }

    /**
     * Suspends a collateral.
     */
    @Override
    public void suspendCollateral(SuspendCollateralRequest request, StreamObserver<SuspendCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendCollateral(request));
    }

    /**
     * Reinstates a collateral.
     */
    @Override
    public void reinstateCollateral(ReinstateCollateralRequest request, StreamObserver<ReinstateCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateCollateral(request));
    }

    /**
     * Terminates a collateral.
     */
    @Override
    public void terminateCollateral(TerminateCollateralRequest request, StreamObserver<TerminateCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateCollateral(request));
    }

    /**
     * Renews a collateral.
     */
    @Override
    public void renewCollateral(RenewCollateralRequest request, StreamObserver<RenewCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewCollateral(request));
    }

    /**
     * Returns a collateral.
     */
    @Override
    public void getCollateralLifecycle(GetCollateralLifecycleRequest request, StreamObserver<GetCollateralLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCollateralLifecycle(request));
    }
}
