package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.CheckAssetTransferEligibilityRequest;
import com.acme.assets.v1.CheckAssetTransferEligibilityResponse;
import com.acme.assets.v1.ExplainAssetTransferDecisionRequest;
import com.acme.assets.v1.ExplainAssetTransferDecisionResponse;
import com.acme.assets.v1.GetAssetTransferRulesRequest;
import com.acme.assets.v1.GetAssetTransferRulesResponse;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.assets.v1.NormalizeAssetTransferRequest;
import com.acme.assets.v1.NormalizeAssetTransferResponse;
import com.acme.assets.v1.PreviewAssetTransferRequest;
import com.acme.assets.v1.PreviewAssetTransferResponse;
import com.acme.assets.v1.ValidateAssetTransferRequest;
import com.acme.assets.v1.ValidateAssetTransferResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks asset transfers against the rules before anything acts on them.
 */
@GrpcService
public class AssetTransferValidationEndpoint extends AssetTransferValidationServiceGrpc.AssetTransferValidationServiceImplBase {
    protected final AssetTransferValidationApplication application;

    public AssetTransferValidationEndpoint(AssetTransferValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an asset transfer.
     */
    @Override
    public void validateAssetTransfer(ValidateAssetTransferRequest request, StreamObserver<ValidateAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateAssetTransfer(request));
    }

    /**
     * Previews an asset transfer.
     */
    @Override
    public void previewAssetTransfer(PreviewAssetTransferRequest request, StreamObserver<PreviewAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewAssetTransfer(request));
    }

    /**
     * Checks an asset transfer.
     */
    @Override
    public void checkAssetTransferEligibility(CheckAssetTransferEligibilityRequest request, StreamObserver<CheckAssetTransferEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkAssetTransferEligibility(request));
    }

    /**
     * Normalizes an asset transfer.
     */
    @Override
    public void normalizeAssetTransfer(NormalizeAssetTransferRequest request, StreamObserver<NormalizeAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeAssetTransfer(request));
    }

    /**
     * Explains an asset transfer.
     */
    @Override
    public void explainAssetTransferDecision(ExplainAssetTransferDecisionRequest request, StreamObserver<ExplainAssetTransferDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainAssetTransferDecision(request));
    }

    /**
     * Lints an asset transfer.
     */
    @Override
    public void lintAssetTransfers(LintAssetTransfersRequest request, StreamObserver<LintAssetTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintAssetTransfers(request));
    }

    /**
     * Returns an asset transfer.
     */
    @Override
    public void getAssetTransferRules(GetAssetTransferRulesRequest request, StreamObserver<GetAssetTransferRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAssetTransferRules(request));
    }
}
