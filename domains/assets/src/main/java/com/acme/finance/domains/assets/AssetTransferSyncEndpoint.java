package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.GetAssetTransferSyncStateRequest;
import com.acme.assets.v1.GetAssetTransferSyncStateResponse;
import com.acme.assets.v1.ListAssetTransferConflictsRequest;
import com.acme.assets.v1.ListAssetTransferConflictsResponse;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.assets.v1.PushAssetTransferRequest;
import com.acme.assets.v1.PushAssetTransferResponse;
import com.acme.assets.v1.ReconcileAssetTransfersRequest;
import com.acme.assets.v1.ReconcileAssetTransfersResponse;
import com.acme.assets.v1.SyncAssetTransfersRequest;
import com.acme.assets.v1.SyncAssetTransfersResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps asset transfers in step with the systems of record in other regions.
 */
@GrpcService
public class AssetTransferSyncEndpoint extends AssetTransferSyncServiceGrpc.AssetTransferSyncServiceImplBase {
    protected final AssetTransferSyncApplication application;

    public AssetTransferSyncEndpoint(AssetTransferSyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs an asset transfer.
     */
    @Override
    public void syncAssetTransfers(SyncAssetTransfersRequest request, StreamObserver<SyncAssetTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncAssetTransfers(request));
    }

    /**
     * Pushs an asset transfer.
     */
    @Override
    public void pushAssetTransfer(PushAssetTransferRequest request, StreamObserver<PushAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushAssetTransfer(request));
    }

    /**
     * Pulls an asset transfer.
     */
    @Override
    public void pullAssetTransfers(PullAssetTransfersRequest request, StreamObserver<PullAssetTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.pullAssetTransfers(request));
    }

    /**
     * Reconciles an asset transfer.
     */
    @Override
    public void reconcileAssetTransfers(ReconcileAssetTransfersRequest request, StreamObserver<ReconcileAssetTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.reconcileAssetTransfers(request));
    }

    /**
     * Returns an asset transfer.
     */
    @Override
    public void getAssetTransferSyncState(GetAssetTransferSyncStateRequest request, StreamObserver<GetAssetTransferSyncStateResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAssetTransferSyncState(request));
    }

    /**
     * Lists asset transfers a page at a time.
     */
    @Override
    public void listAssetTransferConflicts(ListAssetTransferConflictsRequest request, StreamObserver<ListAssetTransferConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAssetTransferConflicts(request));
    }
}
