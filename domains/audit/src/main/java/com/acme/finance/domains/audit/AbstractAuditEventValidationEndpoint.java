package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditEventEligibilityRequest;
import com.acme.audit.v1.CheckAuditEventEligibilityResponse;
import com.acme.audit.v1.ExplainAuditEventDecisionRequest;
import com.acme.audit.v1.ExplainAuditEventDecisionResponse;
import com.acme.audit.v1.GetAuditEventRulesRequest;
import com.acme.audit.v1.GetAuditEventRulesResponse;
import com.acme.audit.v1.LintAuditEventsRequest;
import com.acme.audit.v1.LintAuditEventsResponse;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.PreviewAuditEventRequest;
import com.acme.audit.v1.PreviewAuditEventResponse;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Checks audit events against the rules before anything acts on them.
 */
public abstract class AbstractAuditEventValidationEndpoint extends AuditEventValidationServiceGrpc.AuditEventValidationServiceImplBase {
    protected final AuditEventValidationApplication application;

    protected AbstractAuditEventValidationEndpoint(AuditEventValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an audit event.
     */
    @Override
    public void validateAuditEvent(ValidateAuditEventRequest request, StreamObserver<ValidateAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateAuditEvent(request));
    }

    /**
     * Previews an audit event.
     */
    @Override
    public void previewAuditEvent(PreviewAuditEventRequest request, StreamObserver<PreviewAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewAuditEvent(request));
    }

    /**
     * Checks an audit event.
     */
    @Override
    public void checkAuditEventEligibility(CheckAuditEventEligibilityRequest request, StreamObserver<CheckAuditEventEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkAuditEventEligibility(request));
    }

    /**
     * Normalizes an audit event.
     */
    @Override
    public void normalizeAuditEvent(NormalizeAuditEventRequest request, StreamObserver<NormalizeAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeAuditEvent(request));
    }

    /**
     * Explains an audit event.
     */
    @Override
    public void explainAuditEventDecision(ExplainAuditEventDecisionRequest request, StreamObserver<ExplainAuditEventDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainAuditEventDecision(request));
    }

    /**
     * Scores an audit event.
     */
    @Override
    public void scoreAuditEvent(ScoreAuditEventRequest request, StreamObserver<ScoreAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreAuditEvent(request));
    }

    /**
     * Lints an audit event.
     */
    @Override
    public void lintAuditEvents(LintAuditEventsRequest request, StreamObserver<LintAuditEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintAuditEvents(request));
    }

    /**
     * Returns an audit event.
     */
    @Override
    public void getAuditEventRules(GetAuditEventRulesRequest request, StreamObserver<GetAuditEventRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAuditEventRules(request));
    }
}
