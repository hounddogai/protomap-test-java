package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.CheckRfqEligibilityRequest;
import com.acme.procurement.v1.CheckRfqEligibilityResponse;
import com.acme.procurement.v1.ExplainRfqDecisionRequest;
import com.acme.procurement.v1.ExplainRfqDecisionResponse;
import com.acme.procurement.v1.GetRfqRulesRequest;
import com.acme.procurement.v1.GetRfqRulesResponse;
import com.acme.procurement.v1.LintRfqsRequest;
import com.acme.procurement.v1.LintRfqsResponse;
import com.acme.procurement.v1.NormalizeRfqRequest;
import com.acme.procurement.v1.NormalizeRfqResponse;
import com.acme.procurement.v1.PreviewRfqRequest;
import com.acme.procurement.v1.PreviewRfqResponse;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.ScoreRfqRequest;
import com.acme.procurement.v1.ScoreRfqResponse;
import com.acme.procurement.v1.ValidateRfqRequest;
import com.acme.procurement.v1.ValidateRfqResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks rfqs against the rules before anything acts on them.
 */
@GrpcService
public class RfqValidationEndpoint extends RfqValidationServiceGrpc.RfqValidationServiceImplBase {
    protected final RfqValidationApplication application;

    public RfqValidationEndpoint(RfqValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a rfq.
     */
    @Override
    public void validateRfq(ValidateRfqRequest request, StreamObserver<ValidateRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateRfq(request));
    }

    /**
     * Previews a rfq.
     */
    @Override
    public void previewRfq(PreviewRfqRequest request, StreamObserver<PreviewRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewRfq(request));
    }

    /**
     * Checks a rfq.
     */
    @Override
    public void checkRfqEligibility(CheckRfqEligibilityRequest request, StreamObserver<CheckRfqEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkRfqEligibility(request));
    }

    /**
     * Normalizes a rfq.
     */
    @Override
    public void normalizeRfq(NormalizeRfqRequest request, StreamObserver<NormalizeRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeRfq(request));
    }

    /**
     * Explains a rfq.
     */
    @Override
    public void explainRfqDecision(ExplainRfqDecisionRequest request, StreamObserver<ExplainRfqDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainRfqDecision(request));
    }

    /**
     * Scores a rfq.
     */
    @Override
    public void scoreRfq(ScoreRfqRequest request, StreamObserver<ScoreRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreRfq(request));
    }

    /**
     * Lints a rfq.
     */
    @Override
    public void lintRfqs(LintRfqsRequest request, StreamObserver<LintRfqsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintRfqs(request));
    }

    /**
     * Returns a rfq.
     */
    @Override
    public void getRfqRules(GetRfqRulesRequest request, StreamObserver<GetRfqRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRfqRules(request));
    }
}
