package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.CheckStressTestEligibilityRequest;
import com.acme.risk.v1.CheckStressTestEligibilityResponse;
import com.acme.risk.v1.ExplainStressTestDecisionRequest;
import com.acme.risk.v1.ExplainStressTestDecisionResponse;
import com.acme.risk.v1.LintStressTestsRequest;
import com.acme.risk.v1.LintStressTestsResponse;
import com.acme.risk.v1.NormalizeStressTestRequest;
import com.acme.risk.v1.NormalizeStressTestResponse;
import com.acme.risk.v1.PreviewStressTestRequest;
import com.acme.risk.v1.PreviewStressTestResponse;
import com.acme.risk.v1.ScoreStressTestRequest;
import com.acme.risk.v1.ScoreStressTestResponse;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.risk.v1.ValidateStressTestRequest;
import com.acme.risk.v1.ValidateStressTestResponse;
import io.grpc.stub.StreamObserver;

/**
 * Checks stress tests against the rules before anything acts on them.
 */
public abstract class AbstractStressTestValidationEndpoint extends StressTestValidationServiceGrpc.StressTestValidationServiceImplBase {
    protected final StressTestValidationApplication application;

    protected AbstractStressTestValidationEndpoint(StressTestValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a stress test.
     */
    @Override
    public void validateStressTest(ValidateStressTestRequest request, StreamObserver<ValidateStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateStressTest(request));
    }

    /**
     * Previews a stress test.
     */
    @Override
    public void previewStressTest(PreviewStressTestRequest request, StreamObserver<PreviewStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewStressTest(request));
    }

    /**
     * Checks a stress test.
     */
    @Override
    public void checkStressTestEligibility(CheckStressTestEligibilityRequest request, StreamObserver<CheckStressTestEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkStressTestEligibility(request));
    }

    /**
     * Normalizes a stress test.
     */
    @Override
    public void normalizeStressTest(NormalizeStressTestRequest request, StreamObserver<NormalizeStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeStressTest(request));
    }

    /**
     * Explains a stress test.
     */
    @Override
    public void explainStressTestDecision(ExplainStressTestDecisionRequest request, StreamObserver<ExplainStressTestDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainStressTestDecision(request));
    }

    /**
     * Scores a stress test.
     */
    @Override
    public void scoreStressTest(ScoreStressTestRequest request, StreamObserver<ScoreStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreStressTest(request));
    }

    /**
     * Lints a stress test.
     */
    @Override
    public void lintStressTests(LintStressTestsRequest request, StreamObserver<LintStressTestsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintStressTests(request));
    }
}
