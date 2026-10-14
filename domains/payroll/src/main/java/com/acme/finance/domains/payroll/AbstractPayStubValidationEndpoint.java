package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.CheckPayStubEligibilityRequest;
import com.acme.payroll.v1.CheckPayStubEligibilityResponse;
import com.acme.payroll.v1.ExplainPayStubDecisionRequest;
import com.acme.payroll.v1.ExplainPayStubDecisionResponse;
import com.acme.payroll.v1.GetPayStubRulesRequest;
import com.acme.payroll.v1.GetPayStubRulesResponse;
import com.acme.payroll.v1.LintPayStubsRequest;
import com.acme.payroll.v1.LintPayStubsResponse;
import com.acme.payroll.v1.NormalizePayStubRequest;
import com.acme.payroll.v1.NormalizePayStubResponse;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PreviewPayStubRequest;
import com.acme.payroll.v1.PreviewPayStubResponse;
import com.acme.payroll.v1.ScorePayStubRequest;
import com.acme.payroll.v1.ScorePayStubResponse;
import com.acme.payroll.v1.ValidatePayStubRequest;
import com.acme.payroll.v1.ValidatePayStubResponse;
import io.grpc.stub.StreamObserver;

/**
 * Checks pay stubs against the rules before anything acts on them.
 */
public abstract class AbstractPayStubValidationEndpoint extends PayStubValidationServiceGrpc.PayStubValidationServiceImplBase {
    protected final PayStubValidationApplication application;

    protected AbstractPayStubValidationEndpoint(PayStubValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a pay stub.
     */
    @Override
    public void validatePayStub(ValidatePayStubRequest request, StreamObserver<ValidatePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.validatePayStub(request));
    }

    /**
     * Previews a pay stub.
     */
    @Override
    public void previewPayStub(PreviewPayStubRequest request, StreamObserver<PreviewPayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewPayStub(request));
    }

    /**
     * Checks a pay stub.
     */
    @Override
    public void checkPayStubEligibility(CheckPayStubEligibilityRequest request, StreamObserver<CheckPayStubEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkPayStubEligibility(request));
    }

    /**
     * Normalizes a pay stub.
     */
    @Override
    public void normalizePayStub(NormalizePayStubRequest request, StreamObserver<NormalizePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizePayStub(request));
    }

    /**
     * Explains a pay stub.
     */
    @Override
    public void explainPayStubDecision(ExplainPayStubDecisionRequest request, StreamObserver<ExplainPayStubDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPayStubDecision(request));
    }

    /**
     * Scores a pay stub.
     */
    @Override
    public void scorePayStub(ScorePayStubRequest request, StreamObserver<ScorePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.scorePayStub(request));
    }

    /**
     * Lints a pay stub.
     */
    @Override
    public void lintPayStubs(LintPayStubsRequest request, StreamObserver<LintPayStubsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintPayStubs(request));
    }

    /**
     * Returns a pay stub.
     */
    @Override
    public void getPayStubRules(GetPayStubRulesRequest request, StreamObserver<GetPayStubRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPayStubRules(request));
    }
}
