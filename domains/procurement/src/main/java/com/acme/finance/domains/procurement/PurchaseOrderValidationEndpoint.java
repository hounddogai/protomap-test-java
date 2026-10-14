package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.CheckPurchaseOrderEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseOrderEligibilityResponse;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionResponse;
import com.acme.procurement.v1.GetPurchaseOrderRulesRequest;
import com.acme.procurement.v1.GetPurchaseOrderRulesResponse;
import com.acme.procurement.v1.LintPurchaseOrdersRequest;
import com.acme.procurement.v1.LintPurchaseOrdersResponse;
import com.acme.procurement.v1.NormalizePurchaseOrderRequest;
import com.acme.procurement.v1.NormalizePurchaseOrderResponse;
import com.acme.procurement.v1.PreviewPurchaseOrderRequest;
import com.acme.procurement.v1.PreviewPurchaseOrderResponse;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.ScorePurchaseOrderRequest;
import com.acme.procurement.v1.ScorePurchaseOrderResponse;
import com.acme.procurement.v1.ValidatePurchaseOrderRequest;
import com.acme.procurement.v1.ValidatePurchaseOrderResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks purchase orders against the rules before anything acts on them.
 */
@GrpcService
public class PurchaseOrderValidationEndpoint extends PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceImplBase {
    protected final PurchaseOrderValidationApplication application;

    public PurchaseOrderValidationEndpoint(PurchaseOrderValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a purchase order.
     */
    @Override
    public void validatePurchaseOrder(ValidatePurchaseOrderRequest request, StreamObserver<ValidatePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.validatePurchaseOrder(request));
    }

    /**
     * Previews a purchase order.
     */
    @Override
    public void previewPurchaseOrder(PreviewPurchaseOrderRequest request, StreamObserver<PreviewPurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewPurchaseOrder(request));
    }

    /**
     * Checks a purchase order.
     */
    @Override
    public void checkPurchaseOrderEligibility(CheckPurchaseOrderEligibilityRequest request, StreamObserver<CheckPurchaseOrderEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkPurchaseOrderEligibility(request));
    }

    /**
     * Normalizes a purchase order.
     */
    @Override
    public void normalizePurchaseOrder(NormalizePurchaseOrderRequest request, StreamObserver<NormalizePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizePurchaseOrder(request));
    }

    /**
     * Explains a purchase order.
     */
    @Override
    public void explainPurchaseOrderDecision(ExplainPurchaseOrderDecisionRequest request, StreamObserver<ExplainPurchaseOrderDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPurchaseOrderDecision(request));
    }

    /**
     * Scores a purchase order.
     */
    @Override
    public void scorePurchaseOrder(ScorePurchaseOrderRequest request, StreamObserver<ScorePurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.scorePurchaseOrder(request));
    }

    /**
     * Lints a purchase order.
     */
    @Override
    public void lintPurchaseOrders(LintPurchaseOrdersRequest request, StreamObserver<LintPurchaseOrdersResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintPurchaseOrders(request));
    }

    /**
     * Returns a purchase order.
     */
    @Override
    public void getPurchaseOrderRules(GetPurchaseOrderRulesRequest request, StreamObserver<GetPurchaseOrderRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPurchaseOrderRules(request));
    }
}
