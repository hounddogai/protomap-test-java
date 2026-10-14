package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityRequest;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityResponse;
import com.acme.procurement.v1.GetGoodsReceiptRulesRequest;
import com.acme.procurement.v1.GetGoodsReceiptRulesResponse;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.LintGoodsReceiptsRequest;
import com.acme.procurement.v1.LintGoodsReceiptsResponse;
import com.acme.procurement.v1.NormalizeGoodsReceiptRequest;
import com.acme.procurement.v1.NormalizeGoodsReceiptResponse;
import com.acme.procurement.v1.PreviewGoodsReceiptRequest;
import com.acme.procurement.v1.PreviewGoodsReceiptResponse;
import com.acme.procurement.v1.ScoreGoodsReceiptRequest;
import com.acme.procurement.v1.ScoreGoodsReceiptResponse;
import com.acme.procurement.v1.ValidateGoodsReceiptRequest;
import com.acme.procurement.v1.ValidateGoodsReceiptResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks goods receipts against the rules before anything acts on them.
 */
@GrpcService
public class GoodsReceiptValidationEndpoint extends GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceImplBase {
    protected final GoodsReceiptValidationApplication application;

    public GoodsReceiptValidationEndpoint(GoodsReceiptValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a goods receipt.
     */
    @Override
    public void validateGoodsReceipt(ValidateGoodsReceiptRequest request, StreamObserver<ValidateGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateGoodsReceipt(request));
    }

    /**
     * Previews a goods receipt.
     */
    @Override
    public void previewGoodsReceipt(PreviewGoodsReceiptRequest request, StreamObserver<PreviewGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewGoodsReceipt(request));
    }

    /**
     * Checks a goods receipt.
     */
    @Override
    public void checkGoodsReceiptEligibility(CheckGoodsReceiptEligibilityRequest request, StreamObserver<CheckGoodsReceiptEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkGoodsReceiptEligibility(request));
    }

    /**
     * Normalizes a goods receipt.
     */
    @Override
    public void normalizeGoodsReceipt(NormalizeGoodsReceiptRequest request, StreamObserver<NormalizeGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeGoodsReceipt(request));
    }

    /**
     * Scores a goods receipt.
     */
    @Override
    public void scoreGoodsReceipt(ScoreGoodsReceiptRequest request, StreamObserver<ScoreGoodsReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreGoodsReceipt(request));
    }

    /**
     * Lints a goods receipt.
     */
    @Override
    public void lintGoodsReceipts(LintGoodsReceiptsRequest request, StreamObserver<LintGoodsReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintGoodsReceipts(request));
    }

    /**
     * Returns a goods receipt.
     */
    @Override
    public void getGoodsReceiptRules(GetGoodsReceiptRulesRequest request, StreamObserver<GetGoodsReceiptRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getGoodsReceiptRules(request));
    }
}
