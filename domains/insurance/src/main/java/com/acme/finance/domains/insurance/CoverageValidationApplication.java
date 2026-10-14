package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailLineTax;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.concessions.v1.ConcessionStand;
import com.acme.concessions.v1.ConcessionStandCalculationServiceGrpc;
import com.acme.concessions.v1.RecalculateConcessionStandsRequest;
import com.acme.concessions.v1.RecalculateConcessionStandsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.CloneUserRequest;
import com.acme.identity.v1.CloneUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.CheckCoverageEligibilityRequest;
import com.acme.insurance.v1.CheckCoverageEligibilityResponse;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimChannelOrigin;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.CompareClaimOptionsRequest;
import com.acme.insurance.v1.CompareClaimOptionsResponse;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.EstimateCoverageRequest;
import com.acme.insurance.v1.EstimateCoverageResponse;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.GetCoverageRulesRequest;
import com.acme.insurance.v1.GetCoverageRulesResponse;
import com.acme.insurance.v1.NormalizeClaimRequest;
import com.acme.insurance.v1.NormalizeClaimResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.insurance.v1.ScoreCoverageRequest;
import com.acme.insurance.v1.ScoreCoverageResponse;
import com.acme.insurance.v1.ValidateCoverageRequest;
import com.acme.insurance.v1.ValidateCoverageResponse;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks coverages against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class CoverageValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(CoverageValidationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final ConcessionStandCalculationServiceGrpc.ConcessionStandCalculationServiceBlockingStub concessionsConcessionStandCalculationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceFutureStub insuranceCoverageCalculationServiceFuture;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final CoverageValidationRequestMapper mapper;

    private final InsuranceRepository repository;

    public CoverageValidationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            ConcessionStandCalculationServiceGrpc.ConcessionStandCalculationServiceBlockingStub concessionsConcessionStandCalculationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceFutureStub insuranceCoverageCalculationServiceFuture,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            CoverageValidationRequestMapper mapper,
            InsuranceRepository repository) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.concessionsConcessionStandCalculationServiceBlocking = concessionsConcessionStandCalculationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.insuranceCoverageCalculationServiceFuture = insuranceCoverageCalculationServiceFuture;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    /**
     * Validates a coverage.
     */
    public ValidateCoverageResponse validateCoverage(ValidateCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = (NormalizeClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getCoverage().getOwner().getDisplayName())
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeClaim(outbound0);

        var outbound1 = (CheckPolicyEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCoverage().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPolicyEligibility(outbound1);

        var outbound2 = BeanCopies.copy(CloneUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getClaim().getAudit().getCreatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(reply0.getClaim().getAssignee().getDisplayName())
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .build(), CloneUserRequest.class);
        var reply2 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneUser(outbound2);

        var outbound3 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input3 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCoverage().getAudit().getCreatedAt())
                                .setCreatedBy(request.getCoverage().getAudit().getCreatedBy())
                                .setEtag(request.getCoverage().getAudit().getEtag())
                                .setUpdatedAt(request.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateCoverage", request.getCoverage().getCoverageId());
        return ValidateCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .putAllParameters(request.getCoverage().getParametersMap())
                        .build())
                .setOutcome(ValidateCoverageResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews a coverage.
     */
    public PreviewCoverageResponse previewCoverage(PreviewCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = mapper.previewCoverageStage0(request);
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePolicyOptions(outbound0);

        var outbound1 = NormalizePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizePolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getPolicy().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<NormalizePolicyRequest, NormalizePolicyResponse>() {
            @Override
            public NormalizePolicyResponse apply(NormalizePolicyRequest input) {
                return insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePolicy(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = CloneLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPolicy().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setBank(LedgerAccountBankFunding.newBuilder()
                                .setBic(switch (reply1.getPolicy().getFundingCase()) {
                                    case BANK -> reply1.getPolicy().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLedgerAccount(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewCoverage", request.getCoverageId());
        return PreviewCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPolicy().getAudit().getCreatedBy())
                                .setEtag(reply2.getLedgerAccount().getAudit().getEtag())
                                .build())
                        .setCoverageId(reply1.getPolicy().getHolder().getCoverageId())
                        .setDescription(request.getCoverage().getDescription())
                        .setEffective(request.getCoverage().getEffective())
                        .build())
                .setOutcome(PreviewCoverageResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a coverage.
     */
    public CheckCoverageEligibilityResponse checkCoverageEligibility(CheckCoverageEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckCoverageEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = ComparePolicyOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCoverage().getAudit().getCreatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .addLimits(PolicyLimit.newBuilder()
                                .setAmount(request.getCoverage().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.insurance.v1.PolicyCalculationService/ComparePolicyOptions", outbound0);
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePolicyOptions(command0.payload());

        var outbound1 = AnnotateAuditTrailVersionRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .addLines(AuditTrailLine.newBuilder()
                                .setTax(AuditTrailLineTax.newBuilder()
                                        .setAmount(request.getCoverage().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(AnnotateAuditTrailVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        final class Stage1Adapter {
            AnnotateAuditTrailVersionResponse fetch(AnnotateAuditTrailVersionRequest input) {
                return auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateAuditTrailVersion(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckCoverageEligibility", request.getCoverage().getCoverageId());
        return CheckCoverageEligibilityResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getPolicy().getAudit().getCreatedBy())
                                .setEtag(reply1.getAuditTrail().getAudit().getEtag())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setDescription(request.getCoverage().getDescription())
                        .build())
                .setOutcome(CheckCoverageEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Normalizes a coverage.
     */
    public NormalizeCoverageResponse normalizeCoverage(NormalizeCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var builder0 = CompareClaimOptionsRequest.newBuilder();
        builder0.setClaim(Claim.newBuilder()
                .setCustomer(CustomerRef.newBuilder()
                        .setDisplayName(request.getCoverage().getOwner().getDisplayName())
                        .build())
                .setRequester(ContactInfo.newBuilder()
                        .setName(request.getDetails().getContact().getName())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareClaimOptions(outbound0);

        var outbound1 = NormalizePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizePolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getClaim().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePolicy(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = QuoteClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getCoverage().getOwner().getDisplayName())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getClaim().getCustomer().getDisplayName())
                                .setEmail(reply0.getClaim().getRequester().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getClaim().getAssignee().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteClaim(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeCoverage", request.getCoverage().getCoverageId());
        return NormalizeCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setDescription(request.getCoverage().getDescription())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getClaim().getCustomer().getDisplayName())
                                .setUserId(reply0.getClaim().getAssignee().getUserId())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a coverage.
     */
    public ExplainCoverageDecisionResponse explainCoverageDecision(ExplainCoverageDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainCoverageDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = QuoteClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getCoverage().getOwner().getDisplayName())
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteClaim(outbound0);

        var outbound1 = CompareClaimOptionsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getCoverage().getOwner().getUserId())
                                .build())
                        .setChannel(ClaimChannelOrigin.newBuilder()
                                .setAddress(reply0.getClaim().getChannel().getAddress())
                                .build())
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(reply0.getClaim().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter1 = new Function<CompareClaimOptionsRequest, CompareClaimOptionsResponse>() {
            @Override
            public CompareClaimOptionsResponse apply(CompareClaimOptionsRequest input) {
                return insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareClaimOptions(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainCoverageDecision", request.getCoverage().getCoverageId());
        return ExplainCoverageDecisionResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getClaim().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setStatus(request.getCoverage().getStatus())
                        .build())
                .setOutcome(ExplainCoverageDecisionResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a coverage.
     */
    public ScoreCoverageResponse scoreCoverage(ScoreCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = RevertJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertJournalEntryRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCoverage().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.ledger.v1.JournalEntryHistoryService");
        var serviceName0 = "acme.ledger.v1" + "." + "JournalEntryHistoryService";
        var methodName0 = "Rev" + "ertJournalEntry";
        var descriptor0 = MethodDescriptor.<RevertJournalEntryRequest, RevertJournalEntryResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(RevertJournalEntryRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RevertJournalEntryResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = NormalizePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<NormalizePolicyRequest, NormalizePolicyResponse>() {
            @Override
            public NormalizePolicyResponse apply(NormalizePolicyRequest input) {
                return insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePolicy(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = mapper.scoreCoverageStage2(request, reply1);
        var reply2 = concessionsConcessionStandCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateConcessionStands(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreCoverage", request.getCoverage().getCoverageId());
        return ScoreCoverageResponse.newBuilder()
                .setBreakdown(ScoreCoverageResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getJournalEntry().getTotal())
                        .build())
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getPolicy().getContact().getEmail())
                                .build())
                        .setStatus(request.getCoverage().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a coverage.
     */
    public GetCoverageRulesResponse getCoverageRules(GetCoverageRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCoverageRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "GetCoverageRules");

        var outbound0 = EstimateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = insuranceCoverageCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCoverage(outbound0);
        var reply0 = RpcSupport.await(pending0);
        return GetCoverageRulesResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setName(reply0.getCoverage().getName())
                        .setPriority(reply0.getCoverage().getPriority())
                        .build())
                .build();
    }
}
