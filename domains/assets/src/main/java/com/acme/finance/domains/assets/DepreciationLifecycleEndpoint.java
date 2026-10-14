package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateDepreciationRequest;
import com.acme.assets.v1.ActivateDepreciationResponse;
import com.acme.assets.v1.DepreciationLifecycleServiceGrpc;
import com.acme.assets.v1.ExpireDepreciationRequest;
import com.acme.assets.v1.ExpireDepreciationResponse;
import com.acme.assets.v1.GetDepreciationLifecycleRequest;
import com.acme.assets.v1.GetDepreciationLifecycleResponse;
import com.acme.assets.v1.ReinstateDepreciationRequest;
import com.acme.assets.v1.ReinstateDepreciationResponse;
import com.acme.assets.v1.RenewDepreciationRequest;
import com.acme.assets.v1.RenewDepreciationResponse;
import com.acme.assets.v1.SuspendDepreciationRequest;
import com.acme.assets.v1.SuspendDepreciationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a depreciation between the states of its lifecycle.
 */
@GrpcService
public class DepreciationLifecycleEndpoint extends DepreciationLifecycleServiceGrpc.DepreciationLifecycleServiceImplBase {
    protected final DepreciationLifecycleApplication application;

    public DepreciationLifecycleEndpoint(DepreciationLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a depreciation.
     */
    @Override
    public void activateDepreciation(ActivateDepreciationRequest request, StreamObserver<ActivateDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateDepreciation(request));
    }

    /**
     * Suspends a depreciation.
     */
    @Override
    public void suspendDepreciation(SuspendDepreciationRequest request, StreamObserver<SuspendDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendDepreciation(request));
    }

    /**
     * Reinstates a depreciation.
     */
    @Override
    public void reinstateDepreciation(ReinstateDepreciationRequest request, StreamObserver<ReinstateDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateDepreciation(request));
    }

    /**
     * Renews a depreciation.
     */
    @Override
    public void renewDepreciation(RenewDepreciationRequest request, StreamObserver<RenewDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewDepreciation(request));
    }

    /**
     * Expires a depreciation.
     */
    @Override
    public void expireDepreciation(ExpireDepreciationRequest request, StreamObserver<ExpireDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireDepreciation(request));
    }

    /**
     * Returns a depreciation.
     */
    @Override
    public void getDepreciationLifecycle(GetDepreciationLifecycleRequest request, StreamObserver<GetDepreciationLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDepreciationLifecycle(request));
    }
}
