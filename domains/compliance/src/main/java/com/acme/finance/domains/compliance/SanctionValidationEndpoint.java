package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.CheckSanctionEligibilityRequest;
import com.acme.compliance.v1.CheckSanctionEligibilityResponse;
import com.acme.compliance.v1.ExplainSanctionDecisionRequest;
import com.acme.compliance.v1.ExplainSanctionDecisionResponse;
import com.acme.compliance.v1.GetSanctionRulesRequest;
import com.acme.compliance.v1.GetSanctionRulesResponse;
import com.acme.compliance.v1.NormalizeSanctionRequest;
import com.acme.compliance.v1.NormalizeSanctionResponse;
import com.acme.compliance.v1.PreviewSanctionRequest;
import com.acme.compliance.v1.PreviewSanctionResponse;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.ScoreSanctionRequest;
import com.acme.compliance.v1.ScoreSanctionResponse;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks sanctions against the rules before anything acts on them.
 */
@GrpcService
public class SanctionValidationEndpoint extends SanctionValidationServiceGrpc.SanctionValidationServiceImplBase {
    protected final SanctionValidationApplication application;

    public SanctionValidationEndpoint(SanctionValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a sanction.
     */
    @Override
    public void validateSanction(ValidateSanctionRequest request, StreamObserver<ValidateSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateSanction(request));
    }

    /**
     * Previews a sanction.
     */
    @Override
    public void previewSanction(PreviewSanctionRequest request, StreamObserver<PreviewSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewSanction(request));
    }

    /**
     * Checks a sanction.
     */
    @Override
    public void checkSanctionEligibility(CheckSanctionEligibilityRequest request, StreamObserver<CheckSanctionEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkSanctionEligibility(request));
    }

    /**
     * Normalizes a sanction.
     */
    @Override
    public void normalizeSanction(NormalizeSanctionRequest request, StreamObserver<NormalizeSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeSanction(request));
    }

    /**
     * Explains a sanction.
     */
    @Override
    public void explainSanctionDecision(ExplainSanctionDecisionRequest request, StreamObserver<ExplainSanctionDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainSanctionDecision(request));
    }

    /**
     * Scores a sanction.
     */
    @Override
    public void scoreSanction(ScoreSanctionRequest request, StreamObserver<ScoreSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreSanction(request));
    }

    /**
     * Returns a sanction.
     */
    @Override
    public void getSanctionRules(GetSanctionRulesRequest request, StreamObserver<GetSanctionRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getSanctionRules(request));
    }
}
