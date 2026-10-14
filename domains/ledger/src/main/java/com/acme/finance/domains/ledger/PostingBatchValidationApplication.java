package com.acme.finance.domains.ledger;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityRequest;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodAdminServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.GetPostingBatchRulesRequest;
import com.acme.ledger.v1.GetPostingBatchRulesResponse;
import com.acme.ledger.v1.LintPostingBatchesRequest;
import com.acme.ledger.v1.LintPostingBatchesResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PreviewPostingBatchRequest;
import com.acme.ledger.v1.PreviewPostingBatchResponse;
import com.acme.ledger.v1.RepairFiscalPeriodRequest;
import com.acme.ledger.v1.RepairFiscalPeriodResponse;
import com.acme.ledger.v1.ScorePostingBatchRequest;
import com.acme.ledger.v1.ScorePostingBatchResponse;
import com.acme.ledger.v1.ValidatePostingBatchRequest;
import com.acme.ledger.v1.ValidatePostingBatchResponse;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks posting batches against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PostingBatchValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(PostingBatchValidationApplication.class);

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final FiscalPeriodAdminServiceGrpc.FiscalPeriodAdminServiceBlockingStub ledgerFiscalPeriodAdminServiceBlocking;

    private final LedgerRepository repository;

    public PostingBatchValidationApplication(
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            FiscalPeriodAdminServiceGrpc.FiscalPeriodAdminServiceBlockingStub ledgerFiscalPeriodAdminServiceBlocking,
            LedgerRepository repository) {
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.ledgerFiscalPeriodAdminServiceBlocking = ledgerFiscalPeriodAdminServiceBlocking;
        this.repository = repository;
    }

    /**
     * Validates a posting batch.
     */
    public ValidatePostingBatchResponse validatePostingBatch(ValidatePostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidatePostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePostingBatch(request.getContext().getTenantId(), request.getPostingBatch());
        log.atDebug().addKeyValue("auditValue", request.getPostingBatch().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidatePostingBatch", request.getPostingBatch().getPostingBatchId());
        return ValidatePostingBatchResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .setTotal(request.getPostingBatch().getTotal())
                        .build())
                .build();
    }

    /**
     * Previews a posting batch.
     */
    public PreviewPostingBatchResponse previewPostingBatch(PreviewPostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewPostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePostingBatch(request.getContext().getTenantId(), request.getPostingBatch());
        log.atDebug().addKeyValue("auditValue", request.getPostingBatch().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewPostingBatch", request.getPostingBatch().getPostingBatchId());
        return PreviewPostingBatchResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .setStatus(request.getPostingBatch().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a posting batch.
     */
    public CheckPostingBatchEligibilityResponse checkPostingBatchEligibility(CheckPostingBatchEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckPostingBatchEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePostingBatch(request.getContext().getTenantId(), request.getPostingBatch());

        var outbound0 = RepairFiscalPeriodRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RepairFiscalPeriodRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .addParticipants(FiscalPeriodParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getContact().getName())
                                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> ledgerFiscalPeriodAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).repairFiscalPeriod(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPostingBatch().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckPostingBatchEligibility", request.getPostingBatch().getPostingBatchId());
        return CheckPostingBatchEligibilityResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFiscalPeriod().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getFiscalPeriod().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(request.getPostingBatch().getCounterparty())
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Scores a posting batch.
     */
    public ScorePostingBatchResponse scorePostingBatch(ScorePostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScorePostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePostingBatch(request.getContext().getTenantId(), request.getPostingBatch());

        var outbound0 = CheckAccessPolicyEligibilityRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPostingBatch().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getPostingBatch().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            CheckAccessPolicyEligibilityResponse fetch(CheckAccessPolicyEligibilityRequest input) {
                return authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAccessPolicyEligibility(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPostingBatch().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScorePostingBatch", request.getPostingBatch().getPostingBatchId());
        return ScorePostingBatchResponse.newBuilder()
                .setBreakdown(ScorePostingBatchResponse.Breakdown.newBuilder()
                        .setTotal(request.getPostingBatch().getTotal())
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints a posting batch.
     */
    public LintPostingBatchesResponse lintPostingBatches(LintPostingBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintPostingBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePostingBatch(request.getContext().getTenantId(), request.getPostingBatch());
        log.atDebug().addKeyValue("auditValue", request.getPostingBatch().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintPostingBatches", request.getPostingBatch().getPostingBatchId());
        return LintPostingBatchesResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setDueAt(request.getPostingBatch().getDueAt())
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Returns a posting batch.
     */
    public GetPostingBatchRulesResponse getPostingBatchRules(GetPostingBatchRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPostingBatchRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "GetPostingBatchRules");
        return GetPostingBatchRulesResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }
}
