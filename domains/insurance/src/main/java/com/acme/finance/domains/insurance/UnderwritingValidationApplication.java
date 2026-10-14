package com.acme.finance.domains.insurance;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.BrowseUsersRequest;
import com.acme.identity.v1.BrowseUsersResponse;
import com.acme.identity.v1.ExportUsersRequest;
import com.acme.identity.v1.ExportUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckClaimEligibilityRequest;
import com.acme.insurance.v1.CheckClaimEligibilityResponse;
import com.acme.insurance.v1.CheckUnderwritingEligibilityRequest;
import com.acme.insurance.v1.CheckUnderwritingEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.ClaimStatusChange;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.EstimateUnderwritingRequest;
import com.acme.insurance.v1.EstimateUnderwritingResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.ExplainUnderwritingDecisionRequest;
import com.acme.insurance.v1.ExplainUnderwritingDecisionResponse;
import com.acme.insurance.v1.GetUnderwritingRatesRequest;
import com.acme.insurance.v1.GetUnderwritingRatesResponse;
import com.acme.insurance.v1.GetUnderwritingRulesRequest;
import com.acme.insurance.v1.GetUnderwritingRulesResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.NormalizeUnderwritingRequest;
import com.acme.insurance.v1.NormalizeUnderwritingResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PreviewUnderwritingRequest;
import com.acme.insurance.v1.PreviewUnderwritingResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.ScoreClaimRequest;
import com.acme.insurance.v1.ScoreClaimResponse;
import com.acme.insurance.v1.ScoreCoverageRequest;
import com.acme.insurance.v1.ScoreCoverageResponse;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.UnderwritingChannelOrigin;
import com.acme.insurance.v1.UnderwritingSla;
import com.acme.insurance.v1.ValidateCoverageRequest;
import com.acme.insurance.v1.ValidateCoverageResponse;
import com.acme.insurance.v1.ValidateUnderwritingRequest;
import com.acme.insurance.v1.ValidateUnderwritingResponse;
import com.acme.labeling.v1.LabelSetRef;
import com.acme.labeling.v1.LabelingTask;
import com.acme.labeling.v1.LabelingTaskCalculationServiceGrpc;
import com.acme.labeling.v1.RecalculateLabelingTasksRequest;
import com.acme.labeling.v1.RecalculateLabelingTasksResponse;
import com.acme.metrics.v1.CheckMetricSeriesEligibilityRequest;
import com.acme.metrics.v1.CheckMetricSeriesEligibilityResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.notifications.v1.LintNotificationsRequest;
import com.acme.notifications.v1.LintNotificationsResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Checks underwritings against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class UnderwritingValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(UnderwritingValidationApplication.class);

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final LabelingTaskCalculationServiceGrpc.LabelingTaskCalculationServiceBlockingStub labelingLabelingTaskCalculationServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    public UnderwritingValidationApplication(
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            LabelingTaskCalculationServiceGrpc.LabelingTaskCalculationServiceBlockingStub labelingLabelingTaskCalculationServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.labelingLabelingTaskCalculationServiceBlocking = labelingLabelingTaskCalculationServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates an underwriting.
     */
    public ValidateUnderwritingResponse validateUnderwriting(ValidateUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = RpcSupport.unpack(Any.pack(RecalculateLabelingTasksRequest.newBuilder()
                .setContext(request.getContext())
                .setLabelingTask(LabelingTask.newBuilder()
                        .setCustomer(LabelSetRef.newBuilder()
                                .setDisplayName(request.getUnderwriting().getCustomer().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build()), RecalculateLabelingTasksRequest.class);
        var reply0 = labelingLabelingTaskCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLabelingTasks(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(LintNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getUnderwriting().getAssignee().getEmail())
                                .setUserId(reply0.getLabelingTask().getAssignee().getUserId())
                                .build())
                        .build())
                .build()), LintNotificationsRequest.class);
        var reply1 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintNotifications(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateUnderwriting", request.getUnderwriting().getUnderwritingId());
        return ValidateUnderwritingResponse.newBuilder()
                .setOutcome(ValidateUnderwritingResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getLabelingTask().getRequester().getEmail())
                                .build())
                        .setSubject(request.getUnderwriting().getSubject())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Previews an underwriting.
     */
    public PreviewUnderwritingResponse previewUnderwriting(PreviewUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = ExportUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getUnderwriting().getSla().getDueAt())
                        .putAllLabels(request.getUnderwriting().getCustomFieldsMap())
                        .setOwnerId(request.getUnderwriting().getAssignee().getUserId())
                        .build())
                .build();
        var stream0 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportUsers(outbound0);
        var reply0 = ExportUsersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RpcSupport.unpack(Any.pack(NormalizeCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getUnderwriting().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build()), NormalizeCoverageRequest.class);
        var reply1 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCoverage(outbound1);

        var outbound2 = ValidateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getUnderwriting().getAudit().getCreatedAt())
                                .setUpdatedAt(reply1.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .setBlock(CoverageBlockAction.newBuilder()
                                .setReason(reply1.getCoverage().getBlock().getReason())
                                .build())
                        .build())
                .build();
        var reply2 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCoverage(outbound2);

        var futures3 = request.getUnderwriting().getHistoryList().stream()
                .map(element -> CheckClaimEligibilityRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .addHistory(ClaimStatusChange.newBuilder()
                                .setChangedAt(element.getChangedAt())
                                .setChangedBy(element.getChangedBy())
                                .setFromStatus(element.getFromStatus())
                                .setToStatus(element.getToStatus())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkClaimEligibility(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? CheckClaimEligibilityResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewUnderwriting", request.getUnderwriting().getUnderwritingId());
        return PreviewUnderwritingResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getCoverage().getAudit().getEtag())
                                .setUpdatedAt(reply1.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .setSubject(request.getUnderwriting().getSubject())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Checks an underwriting.
     */
    public CheckUnderwritingEligibilityResponse checkUnderwritingEligibility(CheckUnderwritingEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckUnderwritingEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = EstimateUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setChannel(UnderwritingChannelOrigin.newBuilder()
                                .setAddress(switch (request.getUnderwriting().getOriginCase()) {
                                    case CHANNEL -> request.getUnderwriting().getChannel().getAddress();
                                    default -> "";
                                })
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getUnderwriting().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateUnderwriting(outbound0);

        var outbound1 = BrowseUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getUnderwriting().getSla().getDueAt())
                        .putAllLabels(reply0.getUnderwriting().getCustomFieldsMap())
                        .setOwnerId(reply0.getUnderwriting().getSla().getPolicyId())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseUsers(outbound1));

        var outbound2 = (ValidateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getUnderwriting().getAudit().getEtag())
                                .setUpdatedAt(request.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setDetails(ValidateCoverageRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .setPreferredChannel(request.getUnderwriting().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCoverage(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckUnderwritingEligibility", request.getUnderwriting().getUnderwritingId());
        return CheckUnderwritingEligibilityResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getUnderwriting().getCustomer().getDisplayName())
                                .setEmail(reply0.getUnderwriting().getRequester().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .setPriority(request.getUnderwriting().getPriority())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Normalizes an underwriting.
     */
    public NormalizeUnderwritingResponse normalizeUnderwriting(NormalizeUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = RpcSupport.unpack(Any.pack(ScoreCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getUnderwriting().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getUnderwriting().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build()), ScoreCoverageRequest.class);
        var reply0 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCoverage(outbound0);

        var outbound1 = GetUnderwritingRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwritingId(request.getUnderwritingId())
                .build();
        final class Stage1Adapter {
            GetUnderwritingRatesResponse fetch(GetUnderwritingRatesRequest input) {
                return insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getUnderwritingRates(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeUnderwriting", request.getUnderwriting().getUnderwritingId());
        return NormalizeUnderwritingResponse.newBuilder()
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getCoverage().getOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .setPriority(request.getUnderwriting().getPriority())
                        .setSla(UnderwritingSla.newBuilder()
                                .setBreached(reply1.getUnderwriting().getSla().getBreached())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Explains an underwriting.
     */
    public ExplainUnderwritingDecisionResponse explainUnderwritingDecision(ExplainUnderwritingDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainUnderwritingDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = ExplainPolicyCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPolicyCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getUnderwriting().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPolicyCalculation(value));

        var results1 = new ArrayList<RecalculateClaimsResponse>();
        for (var element : request.getUnderwriting().getCommentsList()) {
            RecalculateClaimsRequest itemRequest = RecalculateClaimsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .addComments(ClaimComment.newBuilder()
                                .setBody(element.getBody())
                                .setCommentId(element.getCommentId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results1.add(insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateClaims(itemRequest));
        }
        var reply1 = results1.isEmpty() ? RecalculateClaimsResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainUnderwritingDecision", request.getUnderwriting().getUnderwritingId());
        return ExplainUnderwritingDecisionResponse.newBuilder()
                .setOutcome(ExplainUnderwritingDecisionResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setStatus(request.getUnderwriting().getStatus())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Scores an underwriting.
     */
    public ScoreUnderwritingResponse scoreUnderwriting(ScoreUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = CheckMetricSeriesEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getUnderwriting().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<CheckMetricSeriesEligibilityRequest, CheckMetricSeriesEligibilityResponse>() {
            @Override
            public CheckMetricSeriesEligibilityResponse apply(CheckMetricSeriesEligibilityRequest input) {
                return metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkMetricSeriesEligibility(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreUnderwriting", request.getUnderwriting().getUnderwritingId());
        return ScoreUnderwritingResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setStatus(request.getUnderwriting().getStatus())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Returns an underwriting.
     */
    public GetUnderwritingRulesResponse getUnderwritingRules(GetUnderwritingRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetUnderwritingRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "GetUnderwritingRules");

        var outbound0 = CheckClaimEligibilityRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkClaimEligibility(outbound0);

        var outbound1 = ScoreClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getClaim().getAssignee().getDisplayName())
                                .setUserId(reply0.getClaim().getAssignee().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply0.getClaim().getRequester().getPhone())
                                .build())
                        .setSla(ClaimSla.newBuilder()
                                .setPausedFor(reply0.getClaim().getSla().getPausedFor())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreClaim(outbound1);
        return GetUnderwritingRulesResponse.newBuilder()
                .setUnderwriting(Underwriting.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getClaim().getCustomer().getEmail())
                                .setName(reply0.getClaim().getRequester().getName())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }
}
