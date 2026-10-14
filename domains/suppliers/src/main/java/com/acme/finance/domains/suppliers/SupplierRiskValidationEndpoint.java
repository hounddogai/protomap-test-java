package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionResponse;
import com.acme.suppliers.v1.GetSupplierRiskRulesRequest;
import com.acme.suppliers.v1.GetSupplierRiskRulesResponse;
import com.acme.suppliers.v1.LintSupplierRisksRequest;
import com.acme.suppliers.v1.LintSupplierRisksResponse;
import com.acme.suppliers.v1.NormalizeSupplierRiskRequest;
import com.acme.suppliers.v1.NormalizeSupplierRiskResponse;
import com.acme.suppliers.v1.PreviewSupplierRiskRequest;
import com.acme.suppliers.v1.PreviewSupplierRiskResponse;
import com.acme.suppliers.v1.ScoreSupplierRiskRequest;
import com.acme.suppliers.v1.ScoreSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRiskRequest;
import com.acme.suppliers.v1.ValidateSupplierRiskResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks supplier risks against the rules before anything acts on them.
 */
@GrpcService
public class SupplierRiskValidationEndpoint extends SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceImplBase {
    protected final SupplierRiskValidationApplication application;

    public SupplierRiskValidationEndpoint(SupplierRiskValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a supplier risk.
     */
    @Override
    public void validateSupplierRisk(ValidateSupplierRiskRequest request, StreamObserver<ValidateSupplierRiskResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateSupplierRisk(request));
    }

    /**
     * Previews a supplier risk.
     */
    @Override
    public void previewSupplierRisk(PreviewSupplierRiskRequest request, StreamObserver<PreviewSupplierRiskResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewSupplierRisk(request));
    }

    /**
     * Checks a supplier risk.
     */
    @Override
    public void checkSupplierRiskEligibility(CheckSupplierRiskEligibilityRequest request, StreamObserver<CheckSupplierRiskEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkSupplierRiskEligibility(request));
    }

    /**
     * Normalizes a supplier risk.
     */
    @Override
    public void normalizeSupplierRisk(NormalizeSupplierRiskRequest request, StreamObserver<NormalizeSupplierRiskResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeSupplierRisk(request));
    }

    /**
     * Explains a supplier risk.
     */
    @Override
    public void explainSupplierRiskDecision(ExplainSupplierRiskDecisionRequest request, StreamObserver<ExplainSupplierRiskDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainSupplierRiskDecision(request));
    }

    /**
     * Scores a supplier risk.
     */
    @Override
    public void scoreSupplierRisk(ScoreSupplierRiskRequest request, StreamObserver<ScoreSupplierRiskResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreSupplierRisk(request));
    }

    /**
     * Lints a supplier risk.
     */
    @Override
    public void lintSupplierRisks(LintSupplierRisksRequest request, StreamObserver<LintSupplierRisksResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintSupplierRisks(request));
    }

    /**
     * Returns a supplier risk.
     */
    @Override
    public void getSupplierRiskRules(GetSupplierRiskRulesRequest request, StreamObserver<GetSupplierRiskRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getSupplierRiskRules(request));
    }
}
