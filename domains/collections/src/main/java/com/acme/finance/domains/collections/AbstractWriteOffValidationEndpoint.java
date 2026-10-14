package com.acme.finance.domains.collections;

import com.acme.collections.v1.CheckWriteOffEligibilityRequest;
import com.acme.collections.v1.CheckWriteOffEligibilityResponse;
import com.acme.collections.v1.ExplainWriteOffDecisionRequest;
import com.acme.collections.v1.ExplainWriteOffDecisionResponse;
import com.acme.collections.v1.GetWriteOffRulesRequest;
import com.acme.collections.v1.GetWriteOffRulesResponse;
import com.acme.collections.v1.LintWriteOffsRequest;
import com.acme.collections.v1.LintWriteOffsResponse;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.ScoreWriteOffRequest;
import com.acme.collections.v1.ScoreWriteOffResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Checks write offs against the rules before anything acts on them.
 */
public abstract class AbstractWriteOffValidationEndpoint extends WriteOffValidationServiceGrpc.WriteOffValidationServiceImplBase {
    protected final WriteOffValidationApplication application;

    protected AbstractWriteOffValidationEndpoint(WriteOffValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a write off.
     */
    @Override
    public void validateWriteOff(ValidateWriteOffRequest request, StreamObserver<ValidateWriteOffResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateWriteOff(request));
    }

    /**
     * Previews a write off.
     */
    @Override
    public void previewWriteOff(PreviewWriteOffRequest request, StreamObserver<PreviewWriteOffResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewWriteOff(request));
    }

    /**
     * Checks a write off.
     */
    @Override
    public void checkWriteOffEligibility(CheckWriteOffEligibilityRequest request, StreamObserver<CheckWriteOffEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkWriteOffEligibility(request));
    }

    /**
     * Normalizes a write off.
     */
    @Override
    public void normalizeWriteOff(NormalizeWriteOffRequest request, StreamObserver<NormalizeWriteOffResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeWriteOff(request));
    }

    /**
     * Explains a write off.
     */
    @Override
    public void explainWriteOffDecision(ExplainWriteOffDecisionRequest request, StreamObserver<ExplainWriteOffDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainWriteOffDecision(request));
    }

    /**
     * Scores a write off.
     */
    @Override
    public void scoreWriteOff(ScoreWriteOffRequest request, StreamObserver<ScoreWriteOffResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreWriteOff(request));
    }

    /**
     * Lints a write off.
     */
    @Override
    public void lintWriteOffs(LintWriteOffsRequest request, StreamObserver<LintWriteOffsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintWriteOffs(request));
    }

    /**
     * Returns a write off.
     */
    @Override
    public void getWriteOffRules(GetWriteOffRulesRequest request, StreamObserver<GetWriteOffRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getWriteOffRules(request));
    }
}
