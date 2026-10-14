package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateAssetTransferRequest;
import com.acme.assets.v1.ActivateAssetTransferResponse;
import com.acme.assets.v1.AssetTransferLifecycleServiceGrpc;
import com.acme.assets.v1.ExpireAssetTransferRequest;
import com.acme.assets.v1.ExpireAssetTransferResponse;
import com.acme.assets.v1.GetAssetTransferLifecycleRequest;
import com.acme.assets.v1.GetAssetTransferLifecycleResponse;
import com.acme.assets.v1.ReinstateAssetTransferRequest;
import com.acme.assets.v1.ReinstateAssetTransferResponse;
import com.acme.assets.v1.RenewAssetTransferRequest;
import com.acme.assets.v1.RenewAssetTransferResponse;
import com.acme.assets.v1.ScheduleAssetTransferExpiryRequest;
import com.acme.assets.v1.ScheduleAssetTransferExpiryResponse;
import com.acme.assets.v1.SuspendAssetTransferRequest;
import com.acme.assets.v1.SuspendAssetTransferResponse;
import com.acme.assets.v1.TerminateAssetTransferRequest;
import com.acme.assets.v1.TerminateAssetTransferResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves an asset transfer between the states of its lifecycle.
 */
@GrpcService
public class AssetTransferLifecycleEndpoint extends AssetTransferLifecycleServiceGrpc.AssetTransferLifecycleServiceImplBase {
    protected final AssetTransferLifecycleApplication application;

    public AssetTransferLifecycleEndpoint(AssetTransferLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates an asset transfer.
     */
    @Override
    public void activateAssetTransfer(ActivateAssetTransferRequest request, StreamObserver<ActivateAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateAssetTransfer(request));
    }

    /**
     * Suspends an asset transfer.
     */
    @Override
    public void suspendAssetTransfer(SuspendAssetTransferRequest request, StreamObserver<SuspendAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendAssetTransfer(request));
    }

    /**
     * Reinstates an asset transfer.
     */
    @Override
    public void reinstateAssetTransfer(ReinstateAssetTransferRequest request, StreamObserver<ReinstateAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateAssetTransfer(request));
    }

    /**
     * Terminates an asset transfer.
     */
    @Override
    public void terminateAssetTransfer(TerminateAssetTransferRequest request, StreamObserver<TerminateAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateAssetTransfer(request));
    }

    /**
     * Renews an asset transfer.
     */
    @Override
    public void renewAssetTransfer(RenewAssetTransferRequest request, StreamObserver<RenewAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewAssetTransfer(request));
    }

    /**
     * Expires an asset transfer.
     */
    @Override
    public void expireAssetTransfer(ExpireAssetTransferRequest request, StreamObserver<ExpireAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireAssetTransfer(request));
    }

    /**
     * Returns an asset transfer.
     */
    @Override
    public void getAssetTransferLifecycle(GetAssetTransferLifecycleRequest request, StreamObserver<GetAssetTransferLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAssetTransferLifecycle(request));
    }

    /**
     * Schedules an asset transfer.
     */
    @Override
    public void scheduleAssetTransferExpiry(ScheduleAssetTransferExpiryRequest request, StreamObserver<ScheduleAssetTransferExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleAssetTransferExpiry(request));
    }
}
