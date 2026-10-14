package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.CheckRepaymentEligibilityRequest;
import com.acme.lending.v1.CheckRepaymentEligibilityResponse;
import com.acme.lending.v1.ExplainRepaymentDecisionRequest;
import com.acme.lending.v1.ExplainRepaymentDecisionResponse;
import com.acme.lending.v1.GetRepaymentRulesRequest;
import com.acme.lending.v1.GetRepaymentRulesResponse;
import com.acme.lending.v1.LintRepaymentsRequest;
import com.acme.lending.v1.LintRepaymentsResponse;
import com.acme.lending.v1.NormalizeRepaymentRequest;
import com.acme.lending.v1.NormalizeRepaymentResponse;
import com.acme.lending.v1.PreviewRepaymentRequest;
import com.acme.lending.v1.PreviewRepaymentResponse;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.ScoreRepaymentRequest;
import com.acme.lending.v1.ScoreRepaymentResponse;
import com.acme.lending.v1.ValidateRepaymentRequest;
import com.acme.lending.v1.ValidateRepaymentResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks repayments against the rules before anything acts on them.
 */
@GrpcService
public class RepaymentValidationEndpoint extends RepaymentValidationServiceGrpc.RepaymentValidationServiceImplBase {
    protected final RepaymentValidationApplication application;

    public RepaymentValidationEndpoint(RepaymentValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a repayment.
     */
    @Override
    public void validateRepayment(ValidateRepaymentRequest request, StreamObserver<ValidateRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateRepayment(request));
    }

    /**
     * Previews a repayment.
     */
    @Override
    public void previewRepayment(PreviewRepaymentRequest request, StreamObserver<PreviewRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewRepayment(request));
    }

    /**
     * Checks a repayment.
     */
    @Override
    public void checkRepaymentEligibility(CheckRepaymentEligibilityRequest request, StreamObserver<CheckRepaymentEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkRepaymentEligibility(request));
    }

    /**
     * Normalizes a repayment.
     */
    @Override
    public void normalizeRepayment(NormalizeRepaymentRequest request, StreamObserver<NormalizeRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeRepayment(request));
    }

    /**
     * Explains a repayment.
     */
    @Override
    public void explainRepaymentDecision(ExplainRepaymentDecisionRequest request, StreamObserver<ExplainRepaymentDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainRepaymentDecision(request));
    }

    /**
     * Scores a repayment.
     */
    @Override
    public void scoreRepayment(ScoreRepaymentRequest request, StreamObserver<ScoreRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreRepayment(request));
    }

    /**
     * Lints a repayment.
     */
    @Override
    public void lintRepayments(LintRepaymentsRequest request, StreamObserver<LintRepaymentsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintRepayments(request));
    }

    /**
     * Returns a repayment.
     */
    @Override
    public void getRepaymentRules(GetRepaymentRulesRequest request, StreamObserver<GetRepaymentRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRepaymentRules(request));
    }
}
