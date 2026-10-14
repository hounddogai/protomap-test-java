package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrail;
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
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.ScoreAuditTrailRequest;
import com.acme.audit.v1.ScoreAuditTrailResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.ValidateAuditTrailRequest;
import com.acme.audit.v1.ValidateAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.ListDocumentsRequest;
import com.acme.documents.v1.ListDocumentsResponse;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks audit trails against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AuditTrailValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditTrailValidationApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final AuditRepository repository;

    public AuditTrailValidationApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            AuditRepository repository) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.repository = repository;
    }

    /**
     * Validates an audit trail.
     */
    public ValidateAuditTrailResponse validateAuditTrail(ValidateAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());

        var outbound0 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAuditTrail().getAudit().getCreatedAt())
                                .setCreatedBy(request.getAuditTrail().getAudit().getCreatedBy())
                                .setEtag(request.getAuditTrail().getAudit().getEtag())
                                .setUpdatedAt(request.getAuditTrail().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input0 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateAuditTrail", request.getAuditTrail().getAuditTrailId());
        return ValidateAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setNumber(request.getAuditTrail().getNumber())
                        .build())
                .build();
    }

    /**
     * Previews an audit trail.
     */
    public PreviewAuditTrailResponse previewAuditTrail(PreviewAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewAuditTrail", request.getAuditTrail().getAuditTrailId());
        return PreviewAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setNumber(request.getAuditTrail().getNumber())
                        .build())
                .build();
    }

    /**
     * Checks an audit trail.
     */
    public CheckAuditTrailEligibilityResponse checkAuditTrailEligibility(CheckAuditTrailEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckAuditTrailEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckAuditTrailEligibility", request.getAuditTrail().getAuditTrailId());
        return CheckAuditTrailEligibilityResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setTotal(request.getAuditTrail().getTotal())
                        .build())
                .build();
    }

    /**
     * Normalizes an audit trail.
     */
    public NormalizeAuditTrailResponse normalizeAuditTrail(NormalizeAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeAuditTrail", request.getAuditTrail().getAuditTrailId());
        return NormalizeAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setIssuedAt(request.getAuditTrail().getIssuedAt())
                        .build())
                .build();
    }

    /**
     * Explains an audit trail.
     */
    public ExplainAuditTrailDecisionResponse explainAuditTrailDecision(ExplainAuditTrailDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainAuditTrailDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainAuditTrailDecision", request.getAuditTrail().getAuditTrailId());
        return ExplainAuditTrailDecisionResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setDueAt(request.getAuditTrail().getDueAt())
                        .build())
                .build();
    }

    /**
     * Scores an audit trail.
     */
    public ScoreAuditTrailResponse scoreAuditTrail(ScoreAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());

        var outbound0 = ListDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAuditTrail().getAudit().getUpdatedAt())
                        .setRegion(request.getAuditTrail().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<ListDocumentsRequest, ListDocumentsResponse> port0 = RpcPort.adapt(documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listDocuments);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreAuditTrail", request.getAuditTrail().getAuditTrailId());
        return ScoreAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setNumber(request.getAuditTrail().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lints an audit trail.
     */
    public LintAuditTrailsResponse lintAuditTrails(LintAuditTrailsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintAuditTrails")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintAuditTrails", request.getAuditTrail().getAuditTrailId());
        return LintAuditTrailsResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setBillingAddress(request.getAuditTrail().getBillingAddress())
                        .build())
                .build();
    }

    /**
     * Returns an audit trail.
     */
    public GetAuditTrailRulesResponse getAuditTrailRules(GetAuditTrailRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAuditTrailRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "GetAuditTrailRules");
        return GetAuditTrailRulesResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .build();
    }
}
