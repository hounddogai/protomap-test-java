package com.acme.finance.domains.assets;

import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.GetFixedAssetRatesRequest;
import com.acme.assets.v1.GetFixedAssetRatesResponse;
import com.acme.assets.v1.QuoteFixedAssetRequest;
import com.acme.assets.v1.QuoteFixedAssetResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a fixed asset, such as totals, taxes, and estimates.
 */
@GrpcService
public class FixedAssetCalculationEndpoint extends FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceImplBase {
    protected final FixedAssetCalculationApplication application;

    public FixedAssetCalculationEndpoint(FixedAssetCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a fixed asset.
     */
    @Override
    public void calculateFixedAsset(CalculateFixedAssetRequest request, StreamObserver<CalculateFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateFixedAsset(request));
    }

    /**
     * Estimates a fixed asset.
     */
    @Override
    public void estimateFixedAsset(EstimateFixedAssetRequest request, StreamObserver<EstimateFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateFixedAsset(request));
    }

    /**
     * Quotes a fixed asset.
     */
    @Override
    public void quoteFixedAsset(QuoteFixedAssetRequest request, StreamObserver<QuoteFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteFixedAsset(request));
    }

    /**
     * Recalculates a fixed asset.
     */
    @Override
    public void recalculateFixedAssets(RecalculateFixedAssetsRequest request, StreamObserver<RecalculateFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateFixedAssets(request));
    }

    /**
     * Explains a fixed asset.
     */
    @Override
    public void explainFixedAssetCalculation(ExplainFixedAssetCalculationRequest request, StreamObserver<ExplainFixedAssetCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainFixedAssetCalculation(request));
    }

    /**
     * Returns a fixed asset.
     */
    @Override
    public void getFixedAssetRates(GetFixedAssetRatesRequest request, StreamObserver<GetFixedAssetRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFixedAssetRates(request));
    }
}
