package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CheckUnderwritingEligibilityRequest;
import com.acme.insurance.v1.CheckUnderwritingEligibilityResponse;
import com.acme.insurance.v1.ExplainUnderwritingDecisionRequest;
import com.acme.insurance.v1.ExplainUnderwritingDecisionResponse;
import com.acme.insurance.v1.GetUnderwritingRulesRequest;
import com.acme.insurance.v1.GetUnderwritingRulesResponse;
import com.acme.insurance.v1.NormalizeUnderwritingRequest;
import com.acme.insurance.v1.NormalizeUnderwritingResponse;
import com.acme.insurance.v1.PreviewUnderwritingRequest;
import com.acme.insurance.v1.PreviewUnderwritingResponse;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingRequest;
import com.acme.insurance.v1.ValidateUnderwritingResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks underwritings against the rules before anything acts on them.
 */
@GrpcService
public class UnderwritingValidationEndpoint extends UnderwritingValidationServiceGrpc.UnderwritingValidationServiceImplBase {
    protected final UnderwritingValidationApplication application;

    public UnderwritingValidationEndpoint(UnderwritingValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an underwriting.
     */
    @Override
    public void validateUnderwriting(ValidateUnderwritingRequest request, StreamObserver<ValidateUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateUnderwriting(request));
    }

    /**
     * Previews an underwriting.
     */
    @Override
    public void previewUnderwriting(PreviewUnderwritingRequest request, StreamObserver<PreviewUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewUnderwriting(request));
    }

    /**
     * Checks an underwriting.
     */
    @Override
    public void checkUnderwritingEligibility(CheckUnderwritingEligibilityRequest request, StreamObserver<CheckUnderwritingEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkUnderwritingEligibility(request));
    }

    /**
     * Normalizes an underwriting.
     */
    @Override
    public void normalizeUnderwriting(NormalizeUnderwritingRequest request, StreamObserver<NormalizeUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeUnderwriting(request));
    }

    /**
     * Explains an underwriting.
     */
    @Override
    public void explainUnderwritingDecision(ExplainUnderwritingDecisionRequest request, StreamObserver<ExplainUnderwritingDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainUnderwritingDecision(request));
    }

    /**
     * Scores an underwriting.
     */
    @Override
    public void scoreUnderwriting(ScoreUnderwritingRequest request, StreamObserver<ScoreUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreUnderwriting(request));
    }

    /**
     * Returns an underwriting.
     */
    @Override
    public void getUnderwritingRules(GetUnderwritingRulesRequest request, StreamObserver<GetUnderwritingRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getUnderwritingRules(request));
    }
}
