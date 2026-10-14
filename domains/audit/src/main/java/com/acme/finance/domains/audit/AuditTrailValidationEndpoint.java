package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.audit.v1.ExplainAuditTrailDecisionRequest;
import com.acme.audit.v1.ExplainAuditTrailDecisionResponse;
import com.acme.audit.v1.GetAuditTrailRulesRequest;
import com.acme.audit.v1.GetAuditTrailRulesResponse;
import com.acme.audit.v1.LintAuditTrailsRequest;
import com.acme.audit.v1.LintAuditTrailsResponse;
import com.acme.audit.v1.NormalizeAuditTrailRequest;
import com.acme.audit.v1.NormalizeAuditTrailResponse;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.audit.v1.ScoreAuditTrailRequest;
import com.acme.audit.v1.ScoreAuditTrailResponse;
import com.acme.audit.v1.ValidateAuditTrailRequest;
import com.acme.audit.v1.ValidateAuditTrailResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks audit trails against the rules before anything acts on them.
 */
@GrpcService
public class AuditTrailValidationEndpoint extends AuditTrailValidationServiceGrpc.AuditTrailValidationServiceImplBase {
    protected final AuditTrailValidationApplication application;

    public AuditTrailValidationEndpoint(AuditTrailValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an audit trail.
     */
    @Override
    public void validateAuditTrail(ValidateAuditTrailRequest request, StreamObserver<ValidateAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateAuditTrail(request));
    }

    /**
     * Previews an audit trail.
     */
    @Override
    public void previewAuditTrail(PreviewAuditTrailRequest request, StreamObserver<PreviewAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewAuditTrail(request));
    }

    /**
     * Checks an audit trail.
     */
    @Override
    public void checkAuditTrailEligibility(CheckAuditTrailEligibilityRequest request, StreamObserver<CheckAuditTrailEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkAuditTrailEligibility(request));
    }

    /**
     * Normalizes an audit trail.
     */
    @Override
    public void normalizeAuditTrail(NormalizeAuditTrailRequest request, StreamObserver<NormalizeAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeAuditTrail(request));
    }

    /**
     * Explains an audit trail.
     */
    @Override
    public void explainAuditTrailDecision(ExplainAuditTrailDecisionRequest request, StreamObserver<ExplainAuditTrailDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainAuditTrailDecision(request));
    }

    /**
     * Scores an audit trail.
     */
    @Override
    public void scoreAuditTrail(ScoreAuditTrailRequest request, StreamObserver<ScoreAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreAuditTrail(request));
    }

    /**
     * Lints an audit trail.
     */
    @Override
    public void lintAuditTrails(LintAuditTrailsRequest request, StreamObserver<LintAuditTrailsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintAuditTrails(request));
    }

    /**
     * Returns an audit trail.
     */
    @Override
    public void getAuditTrailRules(GetAuditTrailRulesRequest request, StreamObserver<GetAuditTrailRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAuditTrailRules(request));
    }
}
