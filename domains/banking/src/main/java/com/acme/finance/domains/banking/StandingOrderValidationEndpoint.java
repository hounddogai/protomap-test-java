package com.acme.finance.domains.banking;

import com.acme.banking.v1.CheckStandingOrderEligibilityRequest;
import com.acme.banking.v1.CheckStandingOrderEligibilityResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.GetStandingOrderRulesRequest;
import com.acme.banking.v1.GetStandingOrderRulesResponse;
import com.acme.banking.v1.LintStandingOrdersRequest;
import com.acme.banking.v1.LintStandingOrdersResponse;
import com.acme.banking.v1.NormalizeStandingOrderRequest;
import com.acme.banking.v1.NormalizeStandingOrderResponse;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.ScoreStandingOrderRequest;
import com.acme.banking.v1.ScoreStandingOrderResponse;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks standing orders against the rules before anything acts on them.
 */
@GrpcService
public class StandingOrderValidationEndpoint extends StandingOrderValidationServiceGrpc.StandingOrderValidationServiceImplBase {
    protected final StandingOrderValidationApplication application;

    public StandingOrderValidationEndpoint(StandingOrderValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a standing order.
     */
    @Override
    public void validateStandingOrder(ValidateStandingOrderRequest request, StreamObserver<ValidateStandingOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateStandingOrder(request));
    }

    /**
     * Previews a standing order.
     */
    @Override
    public void previewStandingOrder(PreviewStandingOrderRequest request, StreamObserver<PreviewStandingOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewStandingOrder(request));
    }

    /**
     * Checks a standing order.
     */
    @Override
    public void checkStandingOrderEligibility(CheckStandingOrderEligibilityRequest request, StreamObserver<CheckStandingOrderEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkStandingOrderEligibility(request));
    }

    /**
     * Normalizes a standing order.
     */
    @Override
    public void normalizeStandingOrder(NormalizeStandingOrderRequest request, StreamObserver<NormalizeStandingOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeStandingOrder(request));
    }

    /**
     * Explains a standing order.
     */
    @Override
    public void explainStandingOrderDecision(ExplainStandingOrderDecisionRequest request, StreamObserver<ExplainStandingOrderDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainStandingOrderDecision(request));
    }

    /**
     * Scores a standing order.
     */
    @Override
    public void scoreStandingOrder(ScoreStandingOrderRequest request, StreamObserver<ScoreStandingOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreStandingOrder(request));
    }

    /**
     * Lints a standing order.
     */
    @Override
    public void lintStandingOrders(LintStandingOrdersRequest request, StreamObserver<LintStandingOrdersResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintStandingOrders(request));
    }

    /**
     * Returns a standing order.
     */
    @Override
    public void getStandingOrderRules(GetStandingOrderRulesRequest request, StreamObserver<GetStandingOrderRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getStandingOrderRules(request));
    }
}
