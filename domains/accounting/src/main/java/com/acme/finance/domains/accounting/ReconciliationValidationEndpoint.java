package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.CheckReconciliationEligibilityRequest;
import com.acme.accounting.v1.CheckReconciliationEligibilityResponse;
import com.acme.accounting.v1.ExplainReconciliationDecisionRequest;
import com.acme.accounting.v1.ExplainReconciliationDecisionResponse;
import com.acme.accounting.v1.GetReconciliationRulesRequest;
import com.acme.accounting.v1.GetReconciliationRulesResponse;
import com.acme.accounting.v1.LintReconciliationsRequest;
import com.acme.accounting.v1.LintReconciliationsResponse;
import com.acme.accounting.v1.NormalizeReconciliationRequest;
import com.acme.accounting.v1.NormalizeReconciliationResponse;
import com.acme.accounting.v1.PreviewReconciliationRequest;
import com.acme.accounting.v1.PreviewReconciliationResponse;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.ScoreReconciliationRequest;
import com.acme.accounting.v1.ScoreReconciliationResponse;
import com.acme.accounting.v1.ValidateReconciliationRequest;
import com.acme.accounting.v1.ValidateReconciliationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks reconciliations against the rules before anything acts on them.
 */
@GrpcService
public class ReconciliationValidationEndpoint extends ReconciliationValidationServiceGrpc.ReconciliationValidationServiceImplBase {
    protected final ReconciliationValidationApplication application;

    public ReconciliationValidationEndpoint(ReconciliationValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a reconciliation.
     */
    @Override
    public void validateReconciliation(ValidateReconciliationRequest request, StreamObserver<ValidateReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateReconciliation(request));
    }

    /**
     * Previews a reconciliation.
     */
    @Override
    public void previewReconciliation(PreviewReconciliationRequest request, StreamObserver<PreviewReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewReconciliation(request));
    }

    /**
     * Checks a reconciliation.
     */
    @Override
    public void checkReconciliationEligibility(CheckReconciliationEligibilityRequest request, StreamObserver<CheckReconciliationEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkReconciliationEligibility(request));
    }

    /**
     * Normalizes a reconciliation.
     */
    @Override
    public void normalizeReconciliation(NormalizeReconciliationRequest request, StreamObserver<NormalizeReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeReconciliation(request));
    }

    /**
     * Explains a reconciliation.
     */
    @Override
    public void explainReconciliationDecision(ExplainReconciliationDecisionRequest request, StreamObserver<ExplainReconciliationDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainReconciliationDecision(request));
    }

    /**
     * Scores a reconciliation.
     */
    @Override
    public void scoreReconciliation(ScoreReconciliationRequest request, StreamObserver<ScoreReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreReconciliation(request));
    }

    /**
     * Lints a reconciliation.
     */
    @Override
    public void lintReconciliations(LintReconciliationsRequest request, StreamObserver<LintReconciliationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintReconciliations(request));
    }

    /**
     * Returns a reconciliation.
     */
    @Override
    public void getReconciliationRules(GetReconciliationRulesRequest request, StreamObserver<GetReconciliationRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getReconciliationRules(request));
    }
}
