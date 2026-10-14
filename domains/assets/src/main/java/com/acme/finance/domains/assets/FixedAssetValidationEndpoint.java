package com.acme.finance.domains.assets;

import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetFixedAssetRulesRequest;
import com.acme.assets.v1.GetFixedAssetRulesResponse;
import com.acme.assets.v1.LintFixedAssetsRequest;
import com.acme.assets.v1.LintFixedAssetsResponse;
import com.acme.assets.v1.PreviewFixedAssetRequest;
import com.acme.assets.v1.PreviewFixedAssetResponse;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks fixed assets against the rules before anything acts on them.
 */
@GrpcService
public class FixedAssetValidationEndpoint extends FixedAssetValidationServiceGrpc.FixedAssetValidationServiceImplBase {
    protected final FixedAssetValidationApplication application;

    public FixedAssetValidationEndpoint(FixedAssetValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a fixed asset.
     */
    @Override
    public void validateFixedAsset(ValidateFixedAssetRequest request, StreamObserver<ValidateFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateFixedAsset(request));
    }

    /**
     * Previews a fixed asset.
     */
    @Override
    public void previewFixedAsset(PreviewFixedAssetRequest request, StreamObserver<PreviewFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewFixedAsset(request));
    }

    /**
     * Checks a fixed asset.
     */
    @Override
    public void checkFixedAssetEligibility(CheckFixedAssetEligibilityRequest request, StreamObserver<CheckFixedAssetEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkFixedAssetEligibility(request));
    }

    /**
     * Scores a fixed asset.
     */
    @Override
    public void scoreFixedAsset(ScoreFixedAssetRequest request, StreamObserver<ScoreFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreFixedAsset(request));
    }

    /**
     * Lints a fixed asset.
     */
    @Override
    public void lintFixedAssets(LintFixedAssetsRequest request, StreamObserver<LintFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintFixedAssets(request));
    }

    /**
     * Returns a fixed asset.
     */
    @Override
    public void getFixedAssetRules(GetFixedAssetRulesRequest request, StreamObserver<GetFixedAssetRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFixedAssetRules(request));
    }
}
