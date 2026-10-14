package com.acme.finance.domains.billing;

import com.acme.billing.v1.CheckPaymentTermEligibilityRequest;
import com.acme.billing.v1.CheckPaymentTermEligibilityResponse;
import com.acme.billing.v1.ExplainPaymentTermDecisionRequest;
import com.acme.billing.v1.ExplainPaymentTermDecisionResponse;
import com.acme.billing.v1.GetPaymentTermRulesRequest;
import com.acme.billing.v1.GetPaymentTermRulesResponse;
import com.acme.billing.v1.LintPaymentTermsRequest;
import com.acme.billing.v1.LintPaymentTermsResponse;
import com.acme.billing.v1.PaymentTermValidationServiceGrpc;
import com.acme.billing.v1.PreviewPaymentTermRequest;
import com.acme.billing.v1.PreviewPaymentTermResponse;
import com.acme.billing.v1.ScorePaymentTermRequest;
import com.acme.billing.v1.ScorePaymentTermResponse;
import com.acme.billing.v1.ValidatePaymentTermRequest;
import com.acme.billing.v1.ValidatePaymentTermResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks payment terms against the rules before anything acts on them.
 */
@GrpcService
public class PaymentTermValidationEndpoint extends PaymentTermValidationServiceGrpc.PaymentTermValidationServiceImplBase {
    protected final PaymentTermValidationApplication application;

    public PaymentTermValidationEndpoint(PaymentTermValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a payment term.
     */
    @Override
    public void validatePaymentTerm(ValidatePaymentTermRequest request, StreamObserver<ValidatePaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.validatePaymentTerm(request));
    }

    /**
     * Previews a payment term.
     */
    @Override
    public void previewPaymentTerm(PreviewPaymentTermRequest request, StreamObserver<PreviewPaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewPaymentTerm(request));
    }

    /**
     * Checks a payment term.
     */
    @Override
    public void checkPaymentTermEligibility(CheckPaymentTermEligibilityRequest request, StreamObserver<CheckPaymentTermEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkPaymentTermEligibility(request));
    }

    /**
     * Explains a payment term.
     */
    @Override
    public void explainPaymentTermDecision(ExplainPaymentTermDecisionRequest request, StreamObserver<ExplainPaymentTermDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPaymentTermDecision(request));
    }

    /**
     * Scores a payment term.
     */
    @Override
    public void scorePaymentTerm(ScorePaymentTermRequest request, StreamObserver<ScorePaymentTermResponse> observer) {
        RpcSupport.unary(observer, () -> application.scorePaymentTerm(request));
    }

    /**
     * Lints a payment term.
     */
    @Override
    public void lintPaymentTerms(LintPaymentTermsRequest request, StreamObserver<LintPaymentTermsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintPaymentTerms(request));
    }

    /**
     * Returns a payment term.
     */
    @Override
    public void getPaymentTermRules(GetPaymentTermRulesRequest request, StreamObserver<GetPaymentTermRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPaymentTermRules(request));
    }
}
