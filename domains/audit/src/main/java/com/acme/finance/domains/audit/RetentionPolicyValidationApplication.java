package com.acme.finance.domains.audit;

import com.acme.audit.v1.CheckRetentionPolicyEligibilityRequest;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityResponse;
import com.acme.audit.v1.ExplainRetentionPolicyDecisionRequest;
import com.acme.audit.v1.ExplainRetentionPolicyDecisionResponse;
import com.acme.audit.v1.GetRetentionPolicyRulesRequest;
import com.acme.audit.v1.GetRetentionPolicyRulesResponse;
import com.acme.audit.v1.LintRetentionPoliciesRequest;
import com.acme.audit.v1.LintRetentionPoliciesResponse;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.PreviewRetentionPolicyRequest;
import com.acme.audit.v1.PreviewRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.ValidateRetentionPolicyRequest;
import com.acme.audit.v1.ValidateRetentionPolicyResponse;
import com.acme.finance.platform.RpcSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks retention policies against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RetentionPolicyValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(RetentionPolicyValidationApplication.class);

    private final AuditRepository repository;

    public RetentionPolicyValidationApplication(
            AuditRepository repository) {
        this.repository = repository;
    }

    /**
     * Validates a retention policy.
     */
    public ValidateRetentionPolicyResponse validateRetentionPolicy(ValidateRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateRetentionPolicy", request.getRetentionPolicy().getRetentionPolicyId());
        return ValidateRetentionPolicyResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setName(request.getRetentionPolicy().getName())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Previews a retention policy.
     */
    public PreviewRetentionPolicyResponse previewRetentionPolicy(PreviewRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewRetentionPolicy", request.getRetentionPolicy().getRetentionPolicyId());
        return PreviewRetentionPolicyResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .setStatus(request.getRetentionPolicy().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a retention policy.
     */
    public CheckRetentionPolicyEligibilityResponse checkRetentionPolicyEligibility(CheckRetentionPolicyEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckRetentionPolicyEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckRetentionPolicyEligibility", request.getRetentionPolicy().getRetentionPolicyId());
        return CheckRetentionPolicyEligibilityResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setPriority(request.getRetentionPolicy().getPriority())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Normalizes a retention policy.
     */
    public NormalizeRetentionPolicyResponse normalizeRetentionPolicy(NormalizeRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeRetentionPolicy", request.getRetentionPolicy().getRetentionPolicyId());
        return NormalizeRetentionPolicyResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setName(request.getRetentionPolicy().getName())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Explains a retention policy.
     */
    public ExplainRetentionPolicyDecisionResponse explainRetentionPolicyDecision(ExplainRetentionPolicyDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainRetentionPolicyDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainRetentionPolicyDecision", request.getRetentionPolicy().getRetentionPolicyId());
        return ExplainRetentionPolicyDecisionResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .setStatus(request.getRetentionPolicy().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a retention policy.
     */
    public ScoreRetentionPolicyResponse scoreRetentionPolicy(ScoreRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreRetentionPolicy", request.getRetentionPolicy().getRetentionPolicyId());
        return ScoreRetentionPolicyResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .putAllParameters(request.getRetentionPolicy().getParametersMap())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Lints a retention policy.
     */
    public LintRetentionPoliciesResponse lintRetentionPolicies(LintRetentionPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintRetentionPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintRetentionPolicies", request.getRetentionPolicy().getRetentionPolicyId());
        return LintRetentionPoliciesResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .putAllParameters(request.getRetentionPolicy().getParametersMap())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Returns a retention policy.
     */
    public GetRetentionPolicyRulesResponse getRetentionPolicyRules(GetRetentionPolicyRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRetentionPolicyRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "GetRetentionPolicyRules");
        return GetRetentionPolicyRulesResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }
}
