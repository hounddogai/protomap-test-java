package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateFixedAssetRequest;
import com.acme.assets.v1.ActivateFixedAssetResponse;
import com.acme.assets.v1.ExpireFixedAssetRequest;
import com.acme.assets.v1.ExpireFixedAssetResponse;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.GetFixedAssetLifecycleRequest;
import com.acme.assets.v1.GetFixedAssetLifecycleResponse;
import com.acme.assets.v1.ReinstateFixedAssetRequest;
import com.acme.assets.v1.ReinstateFixedAssetResponse;
import com.acme.assets.v1.RenewFixedAssetRequest;
import com.acme.assets.v1.RenewFixedAssetResponse;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.assets.v1.SuspendFixedAssetRequest;
import com.acme.assets.v1.SuspendFixedAssetResponse;
import com.acme.assets.v1.TerminateFixedAssetRequest;
import com.acme.assets.v1.TerminateFixedAssetResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a fixed asset between the states of its lifecycle.
 */
@GrpcService
public class FixedAssetLifecycleEndpoint extends FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceImplBase {
    protected final FixedAssetLifecycleApplication application;

    public FixedAssetLifecycleEndpoint(FixedAssetLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a fixed asset.
     */
    @Override
    public void activateFixedAsset(ActivateFixedAssetRequest request, StreamObserver<ActivateFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateFixedAsset(request));
    }

    /**
     * Suspends a fixed asset.
     */
    @Override
    public void suspendFixedAsset(SuspendFixedAssetRequest request, StreamObserver<SuspendFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendFixedAsset(request));
    }

    /**
     * Reinstates a fixed asset.
     */
    @Override
    public void reinstateFixedAsset(ReinstateFixedAssetRequest request, StreamObserver<ReinstateFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateFixedAsset(request));
    }

    /**
     * Terminates a fixed asset.
     */
    @Override
    public void terminateFixedAsset(TerminateFixedAssetRequest request, StreamObserver<TerminateFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateFixedAsset(request));
    }

    /**
     * Renews a fixed asset.
     */
    @Override
    public void renewFixedAsset(RenewFixedAssetRequest request, StreamObserver<RenewFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewFixedAsset(request));
    }

    /**
     * Expires a fixed asset.
     */
    @Override
    public void expireFixedAsset(ExpireFixedAssetRequest request, StreamObserver<ExpireFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireFixedAsset(request));
    }

    /**
     * Returns a fixed asset.
     */
    @Override
    public void getFixedAssetLifecycle(GetFixedAssetLifecycleRequest request, StreamObserver<GetFixedAssetLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFixedAssetLifecycle(request));
    }

    /**
     * Schedules a fixed asset.
     */
    @Override
    public void scheduleFixedAssetExpiry(ScheduleFixedAssetExpiryRequest request, StreamObserver<ScheduleFixedAssetExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleFixedAssetExpiry(request));
    }
}
