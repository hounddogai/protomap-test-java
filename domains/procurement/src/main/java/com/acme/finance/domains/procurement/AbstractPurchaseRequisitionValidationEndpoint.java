package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRulesRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionRulesResponse;
import com.acme.procurement.v1.LintPurchaseRequisitionsRequest;
import com.acme.procurement.v1.LintPurchaseRequisitionsResponse;
import com.acme.procurement.v1.NormalizePurchaseRequisitionRequest;
import com.acme.procurement.v1.NormalizePurchaseRequisitionResponse;
import com.acme.procurement.v1.PreviewPurchaseRequisitionRequest;
import com.acme.procurement.v1.PreviewPurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.ScorePurchaseRequisitionRequest;
import com.acme.procurement.v1.ScorePurchaseRequisitionResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import io.grpc.stub.StreamObserver;

/**
 * Checks purchase requisitions against the rules before anything acts on them.
 */
public abstract class AbstractPurchaseRequisitionValidationEndpoint extends PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceImplBase {
    protected final PurchaseRequisitionValidationApplication application;

    protected AbstractPurchaseRequisitionValidationEndpoint(PurchaseRequisitionValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a purchase requisition.
     */
    @Override
    public void validatePurchaseRequisition(ValidatePurchaseRequisitionRequest request, StreamObserver<ValidatePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.validatePurchaseRequisition(request));
    }

    /**
     * Previews a purchase requisition.
     */
    @Override
    public void previewPurchaseRequisition(PreviewPurchaseRequisitionRequest request, StreamObserver<PreviewPurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewPurchaseRequisition(request));
    }

    /**
     * Checks a purchase requisition.
     */
    @Override
    public void checkPurchaseRequisitionEligibility(CheckPurchaseRequisitionEligibilityRequest request, StreamObserver<CheckPurchaseRequisitionEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkPurchaseRequisitionEligibility(request));
    }

    /**
     * Normalizes a purchase requisition.
     */
    @Override
    public void normalizePurchaseRequisition(NormalizePurchaseRequisitionRequest request, StreamObserver<NormalizePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizePurchaseRequisition(request));
    }

    /**
     * Explains a purchase requisition.
     */
    @Override
    public void explainPurchaseRequisitionDecision(ExplainPurchaseRequisitionDecisionRequest request, StreamObserver<ExplainPurchaseRequisitionDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPurchaseRequisitionDecision(request));
    }

    /**
     * Scores a purchase requisition.
     */
    @Override
    public void scorePurchaseRequisition(ScorePurchaseRequisitionRequest request, StreamObserver<ScorePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.scorePurchaseRequisition(request));
    }

    /**
     * Lints a purchase requisition.
     */
    @Override
    public void lintPurchaseRequisitions(LintPurchaseRequisitionsRequest request, StreamObserver<LintPurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintPurchaseRequisitions(request));
    }

    /**
     * Returns a purchase requisition.
     */
    @Override
    public void getPurchaseRequisitionRules(GetPurchaseRequisitionRulesRequest request, StreamObserver<GetPurchaseRequisitionRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPurchaseRequisitionRules(request));
    }
}
