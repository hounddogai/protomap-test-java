package com.acme.finance.domains.billing;

import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CheckBillingAccountEligibilityRequest;
import com.acme.billing.v1.CheckBillingAccountEligibilityResponse;
import com.acme.billing.v1.ExplainBillingAccountDecisionRequest;
import com.acme.billing.v1.ExplainBillingAccountDecisionResponse;
import com.acme.billing.v1.GetBillingAccountRulesRequest;
import com.acme.billing.v1.GetBillingAccountRulesResponse;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.NormalizeBillingAccountRequest;
import com.acme.billing.v1.NormalizeBillingAccountResponse;
import com.acme.billing.v1.PreviewBillingAccountRequest;
import com.acme.billing.v1.PreviewBillingAccountResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.ValidateBillingAccountRequest;
import com.acme.billing.v1.ValidateBillingAccountResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks billing accounts against the rules before anything acts on them.
 */
@GrpcService
public class BillingAccountValidationEndpoint extends BillingAccountValidationServiceGrpc.BillingAccountValidationServiceImplBase {
    protected final BillingAccountValidationApplication application;

    public BillingAccountValidationEndpoint(BillingAccountValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a billing account.
     */
    @Override
    public void validateBillingAccount(ValidateBillingAccountRequest request, StreamObserver<ValidateBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateBillingAccount(request));
    }

    /**
     * Previews a billing account.
     */
    @Override
    public void previewBillingAccount(PreviewBillingAccountRequest request, StreamObserver<PreviewBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewBillingAccount(request));
    }

    /**
     * Checks a billing account.
     */
    @Override
    public void checkBillingAccountEligibility(CheckBillingAccountEligibilityRequest request, StreamObserver<CheckBillingAccountEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkBillingAccountEligibility(request));
    }

    /**
     * Normalizes a billing account.
     */
    @Override
    public void normalizeBillingAccount(NormalizeBillingAccountRequest request, StreamObserver<NormalizeBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeBillingAccount(request));
    }

    /**
     * Explains a billing account.
     */
    @Override
    public void explainBillingAccountDecision(ExplainBillingAccountDecisionRequest request, StreamObserver<ExplainBillingAccountDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainBillingAccountDecision(request));
    }

    /**
     * Scores a billing account.
     */
    @Override
    public void scoreBillingAccount(ScoreBillingAccountRequest request, StreamObserver<ScoreBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreBillingAccount(request));
    }

    /**
     * Lints a billing account.
     */
    @Override
    public void lintBillingAccounts(LintBillingAccountsRequest request, StreamObserver<LintBillingAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintBillingAccounts(request));
    }

    /**
     * Returns a billing account.
     */
    @Override
    public void getBillingAccountRules(GetBillingAccountRulesRequest request, StreamObserver<GetBillingAccountRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBillingAccountRules(request));
    }
}
