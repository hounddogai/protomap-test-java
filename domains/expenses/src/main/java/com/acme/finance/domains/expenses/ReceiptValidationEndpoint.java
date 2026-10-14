package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.ExplainReceiptDecisionRequest;
import com.acme.expenses.v1.ExplainReceiptDecisionResponse;
import com.acme.expenses.v1.GetReceiptRulesRequest;
import com.acme.expenses.v1.GetReceiptRulesResponse;
import com.acme.expenses.v1.LintReceiptsRequest;
import com.acme.expenses.v1.LintReceiptsResponse;
import com.acme.expenses.v1.NormalizeReceiptRequest;
import com.acme.expenses.v1.NormalizeReceiptResponse;
import com.acme.expenses.v1.PreviewReceiptRequest;
import com.acme.expenses.v1.PreviewReceiptResponse;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ScoreReceiptRequest;
import com.acme.expenses.v1.ScoreReceiptResponse;
import com.acme.expenses.v1.ValidateReceiptRequest;
import com.acme.expenses.v1.ValidateReceiptResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks receipts against the rules before anything acts on them.
 */
@GrpcService
public class ReceiptValidationEndpoint extends ReceiptValidationServiceGrpc.ReceiptValidationServiceImplBase {
    protected final ReceiptValidationApplication application;

    public ReceiptValidationEndpoint(ReceiptValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a receipt.
     */
    @Override
    public void validateReceipt(ValidateReceiptRequest request, StreamObserver<ValidateReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateReceipt(request));
    }

    /**
     * Previews a receipt.
     */
    @Override
    public void previewReceipt(PreviewReceiptRequest request, StreamObserver<PreviewReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewReceipt(request));
    }

    /**
     * Checks a receipt.
     */
    @Override
    public void checkReceiptEligibility(CheckReceiptEligibilityRequest request, StreamObserver<CheckReceiptEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkReceiptEligibility(request));
    }

    /**
     * Normalizes a receipt.
     */
    @Override
    public void normalizeReceipt(NormalizeReceiptRequest request, StreamObserver<NormalizeReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeReceipt(request));
    }

    /**
     * Explains a receipt.
     */
    @Override
    public void explainReceiptDecision(ExplainReceiptDecisionRequest request, StreamObserver<ExplainReceiptDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainReceiptDecision(request));
    }

    /**
     * Scores a receipt.
     */
    @Override
    public void scoreReceipt(ScoreReceiptRequest request, StreamObserver<ScoreReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreReceipt(request));
    }

    /**
     * Lints a receipt.
     */
    @Override
    public void lintReceipts(LintReceiptsRequest request, StreamObserver<LintReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintReceipts(request));
    }

    /**
     * Returns a receipt.
     */
    @Override
    public void getReceiptRules(GetReceiptRulesRequest request, StreamObserver<GetReceiptRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getReceiptRules(request));
    }
}
