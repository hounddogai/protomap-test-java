package com.acme.finance.domains.assets;

import com.acme.assets.v1.BrowseFixedAssetsRequest;
import com.acme.assets.v1.BrowseFixedAssetsResponse;
import com.acme.assets.v1.CountFixedAssetsRequest;
import com.acme.assets.v1.CountFixedAssetsResponse;
import com.acme.assets.v1.FindFixedAssetsRequest;
import com.acme.assets.v1.FindFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetQueryServiceGrpc;
import com.acme.assets.v1.LookupFixedAssetRequest;
import com.acme.assets.v1.LookupFixedAssetResponse;
import com.acme.assets.v1.ResolveFixedAssetsRequest;
import com.acme.assets.v1.ResolveFixedAssetsResponse;
import com.acme.assets.v1.SummarizeFixedAssetsRequest;
import com.acme.assets.v1.SummarizeFixedAssetsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about fixed assets, for screens and reports.
 */
@GrpcService
public class FixedAssetQueryEndpoint extends FixedAssetQueryServiceGrpc.FixedAssetQueryServiceImplBase {
    protected final FixedAssetQueryApplication application;

    public FixedAssetQueryEndpoint(FixedAssetQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a fixed asset.
     */
    @Override
    public void lookupFixedAsset(LookupFixedAssetRequest request, StreamObserver<LookupFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupFixedAsset(request));
    }

    /**
     * Lists fixed assets a page at a time.
     */
    @Override
    public void browseFixedAssets(BrowseFixedAssetsRequest request, StreamObserver<BrowseFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseFixedAssets(request));
    }

    /**
     * Lists fixed assets a page at a time.
     */
    @Override
    public void findFixedAssets(FindFixedAssetsRequest request, StreamObserver<FindFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findFixedAssets(request));
    }

    /**
     * Counts fixed assets by status.
     */
    @Override
    public void countFixedAssets(CountFixedAssetsRequest request, StreamObserver<CountFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countFixedAssets(request));
    }

    /**
     * Returns several fixed assets at once.
     */
    @Override
    public void resolveFixedAssets(ResolveFixedAssetsRequest request, StreamObserver<ResolveFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveFixedAssets(request));
    }

    /**
     * Summarizes a fixed asset.
     */
    @Override
    public void summarizeFixedAssets(SummarizeFixedAssetsRequest request, StreamObserver<SummarizeFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeFixedAssets(request));
    }
}
