package com.acme.finance.domains.assets;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.BatchGetFixedAssetsRequest;
import com.acme.assets.v1.BatchGetFixedAssetsResponse;
import com.acme.assets.v1.CreateFixedAssetRequest;
import com.acme.assets.v1.CreateFixedAssetResponse;
import com.acme.assets.v1.DeleteFixedAssetRequest;
import com.acme.assets.v1.DeleteFixedAssetResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.GetFixedAssetRequest;
import com.acme.assets.v1.GetFixedAssetResponse;
import com.acme.assets.v1.ListFixedAssetsRequest;
import com.acme.assets.v1.ListFixedAssetsResponse;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.assets.v1.WatchFixedAssetsRequest;
import com.acme.assets.v1.WatchFixedAssetsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages fixed assets: creates, reads, changes, and lists them.
 */
@GrpcService
public class FixedAssetEndpoint extends FixedAssetServiceGrpc.FixedAssetServiceImplBase {
    protected final FixedAssetApplication application;

    public FixedAssetEndpoint(FixedAssetApplication application) {
        this.application = application;
    }

    /**
     * Creates a fixed asset.
     */
    @Override
    public void createFixedAsset(CreateFixedAssetRequest request, StreamObserver<CreateFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.createFixedAsset(request));
    }

    /**
     * Returns a fixed asset.
     */
    @Override
    public void getFixedAsset(GetFixedAssetRequest request, StreamObserver<GetFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFixedAsset(request));
    }

    /**
     * Deletes a fixed asset.
     */
    @Override
    public void deleteFixedAsset(DeleteFixedAssetRequest request, StreamObserver<DeleteFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteFixedAsset(request));
    }

    /**
     * Lists fixed assets a page at a time.
     */
    @Override
    public void listFixedAssets(ListFixedAssetsRequest request, StreamObserver<ListFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listFixedAssets(request));
    }

    /**
     * Returns several fixed assets at once.
     */
    @Override
    public void batchGetFixedAssets(BatchGetFixedAssetsRequest request, StreamObserver<BatchGetFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetFixedAssets(request));
    }

    /**
     * Lists fixed assets a page at a time.
     */
    @Override
    public void searchFixedAssets(SearchFixedAssetsRequest request, StreamObserver<SearchFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchFixedAssets(request));
    }

    /**
     * Streams the changes of fixed assets as they happen.
     */
    @Override
    public void watchFixedAssets(WatchFixedAssetsRequest request, StreamObserver<WatchFixedAssetsResponse> observer) {
        try {
            var snapshot = application.watchFixedAssets(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of fixed assets and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateFixedAssetsRequest> batchCreateFixedAssets(StreamObserver<BatchCreateFixedAssetsResponse> observer) {
        return new StreamObserver<BatchCreateFixedAssetsRequest>() {
            private boolean finished;
            private final BatchCreateFixedAssetsResponse.Builder summary = BatchCreateFixedAssetsResponse.newBuilder();

            @Override
            public void onNext(BatchCreateFixedAssetsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateFixedAssets(request);
                    summary.mergeFrom(response);
                } catch (RuntimeException failure) {
                    finished = true;
                    observer.onError(RpcSupport.status(failure));
                }
            }

            @Override
            public void onError(Throwable failure) {
                if (!finished) {
                    finished = true;
                    observer.onError(RpcSupport.status(failure));
                }
            }

            @Override
            public void onCompleted() {
                if (finished) { return; }
                finished = true;
                observer.onNext(summary.build());
                observer.onCompleted();
            }
        };
    }
}
