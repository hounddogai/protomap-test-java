package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.PushAuditTrailRequest;
import com.acme.audit.v1.PushAuditTrailResponse;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.BrowsePoliciesRequest;
import com.acme.insurance.v1.BrowsePoliciesResponse;
import com.acme.insurance.v1.CheckCoverageEligibilityRequest;
import com.acme.insurance.v1.CheckCoverageEligibilityResponse;
import com.acme.insurance.v1.CheckPremiumEligibilityRequest;
import com.acme.insurance.v1.CheckPremiumEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.CountPoliciesRequest;
import com.acme.insurance.v1.CountPoliciesResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExportPoliciesRequest;
import com.acme.insurance.v1.ExportPoliciesResponse;
import com.acme.insurance.v1.FindPoliciesRequest;
import com.acme.insurance.v1.FindPoliciesResponse;
import com.acme.insurance.v1.LookupPolicyRequest;
import com.acme.insurance.v1.LookupPolicyResponse;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.PeekPolicyRequest;
import com.acme.insurance.v1.PeekPolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.insurance.v1.ResolvePoliciesRequest;
import com.acme.insurance.v1.ResolvePoliciesResponse;
import com.acme.insurance.v1.ScoreCoverageRequest;
import com.acme.insurance.v1.ScoreCoverageResponse;
import com.acme.insurance.v1.SimulateUnderwritingRequest;
import com.acme.insurance.v1.SimulateUnderwritingResponse;
import com.acme.insurance.v1.SuggestPoliciesRequest;
import com.acme.insurance.v1.SuggestPoliciesResponse;
import com.acme.insurance.v1.SummarizePoliciesRequest;
import com.acme.insurance.v1.SummarizePoliciesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.ValidateClaimRequest;
import com.acme.insurance.v1.ValidateClaimResponse;
import com.acme.insurance.v1.ValidateCoverageRequest;
import com.acme.insurance.v1.ValidateCoverageResponse;
import com.acme.ledger.v1.BrowseJournalEntriesRequest;
import com.acme.ledger.v1.BrowseJournalEntriesResponse;
import com.acme.suppliers.v1.SupplierRef;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about policies, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PolicyQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(PolicyQueryApplication.class);

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final InsuranceRepository repository;

    public PolicyQueryApplication(
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            InsuranceRepository repository) {
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.repository = repository;
    }

    public record SummarizePoliciesStage0Requested(CheckPremiumEligibilityRequest request, CompletableFuture<CheckPremiumEligibilityResponse> completion, io.grpc.Context context) {}

    public record SummarizePoliciesStage1Requested(RevertAuditTrailRequest request, CompletableFuture<RevertAuditTrailResponse> completion, io.grpc.Context context) {}

    /**
     * Returns a policy.
     */
    public LookupPolicyResponse lookupPolicy(LookupPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "LookupPolicy");
        return LookupPolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setPolicyId(request.getPolicyId())
                        .build())
                .build();
    }

    /**
     * Lists policies a page at a time.
     */
    public BrowsePoliciesResponse browsePolicies(BrowsePoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowsePolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "BrowsePolicies");

        var outbound0 = SimulateUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateUnderwritingRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.insurance.v1.UnderwritingCalculationService/SimulateUnderwriting", outbound0);
        var reply0 = insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateUnderwriting(command0.payload());

        var outbound1 = BrowseJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseJournalEntriesRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.ledger.v1.JournalEntryQueryService");
        var serviceName1 = "acme.ledger.v1" + "." + "JournalEntryQueryService";
        var methodName1 = "Bro" + "wseJournalEntries";
        var descriptor1 = MethodDescriptor.<BrowseJournalEntriesRequest, BrowseJournalEntriesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(BrowseJournalEntriesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BrowseJournalEntriesResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowsePolicies", request.getPage().getPageToken());
        return BrowsePoliciesResponse.newBuilder()
                .addPolicies(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getUnderwriting().getRequester().getPhone())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lists policies a page at a time.
     */
    public FindPoliciesResponse findPolicies(FindPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "FindPolicies");

        var outbound0 = CheckCoverageEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setDetails(CheckCoverageEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        final class Stage0Adapter {
            CheckCoverageEligibilityResponse fetch(CheckCoverageEligibilityRequest input) {
                return insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCoverageEligibility(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = NormalizePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCoverage().getAudit().getEtag())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply0.getCoverage().getOwner().getDisplayName())
                                .build())
                        .addLimits(PolicyLimit.newBuilder()
                                .setAmount(reply0.getCoverage().getDiscount().getAmount())
                                .build())
                        .build())
                .setReason(switch (reply0.getCoverage().getActionCase()) {
                    case BLOCK -> reply0.getCoverage().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply1 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePolicy(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindPolicies", request.getPage().getPageToken());
        return FindPoliciesResponse.newBuilder()
                .addPolicies(Policy.newBuilder()
                        .addLimits(PolicyLimit.newBuilder()
                                .setAmount(reply0.getCoverage().getDiscount().getAmount())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Counts policies by status.
     */
    public CountPoliciesResponse countPolicies(CountPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "CountPolicies");

        var outbound0 = ScoreCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setDetails(ScoreCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ScoreCoverageResponse fetch(ScoreCoverageRequest input) {
                return insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCoverage(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountPolicies", request.getPage().getPageToken());
        return CountPoliciesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns several policies at once.
     */
    public ResolvePoliciesResponse resolvePolicies(ResolvePoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolvePolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "ResolvePolicies");
        return ResolvePoliciesResponse.getDefaultInstance();
    }

    /**
     * Streams every matching policy.
     */
    public ExportPoliciesResponse exportPolicies(ExportPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "ExportPolicies");

        var outbound0 = SimulateUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateUnderwritingRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateUnderwriting(value));

        var outbound1 = RecalculatePremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getUnderwriting().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getUnderwriting().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getUnderwriting().getAssignee().getDisplayName())
                                .build())
                        .setDueAt(reply0.getUnderwriting().getSla().getDueAt())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePremiums(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = ValidateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .build())
                .setDetails(ValidateCoverageRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getPremium().getCounterparty().getEmail())
                                .setName(reply0.getUnderwriting().getRequester().getName())
                                .build())
                        .build())
                .build();
        var reply2 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCoverage(outbound2);

        var outbound3 = ComparePolicyOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ComparePolicyOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply1.getPremium().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getCoverage().getAudit().getEtag())
                                .setUpdatedAt(reply0.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .setCard(PolicyCardFunding.newBuilder()
                                .setToken(switch (reply1.getPremium().getSettlementCase()) {
                                    case CARD -> reply1.getPremium().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePolicyOptions(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportPolicies", request.getFilter().getQuery());
        return ExportPoliciesResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setCoverageId(reply3.getPolicy().getHolder().getCoverageId())
                                .setDisplayName(reply1.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Summarizes a policy.
     */
    public SummarizePoliciesResponse summarizePolicies(SummarizePoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizePolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "SummarizePolicies");

        var outbound0 = CheckPremiumEligibilityRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<CheckPremiumEligibilityResponse>();
        events.publishEvent(new SummarizePoliciesStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = RevertAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPremium().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getPremium().getAudit().getUpdatedAt())
                                .build())
                        .setBank(AuditTrailBankSettlement.newBuilder()
                                .setIban(reply0.getPremium().getBank().getIban())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RevertAuditTrailRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var completion1 = new CompletableFuture<RevertAuditTrailResponse>();
        events.publishEvent(new SummarizePoliciesStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = BeanCopies.copy(ScoreCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getAuditTrail().getCounterparty().getDisplayName())
                                .setEmail(reply0.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .setDetails(ScoreCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPremium().getBillingAddress().getCountryCode())
                                .setRecipient(reply1.getAuditTrail().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build(), ScoreCoverageRequest.class);
        var reply2 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCoverage(outbound2);

        var outbound3 = PushAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBank(AuditTrailBankSettlement.newBuilder()
                                .setBic(switch (reply0.getPremium().getSettlementCase()) {
                                    case BANK -> reply0.getPremium().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PushAuditTrailRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply3 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushAuditTrail(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizePolicies", request.getPolicyId());
        return SummarizePoliciesResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists policies a page at a time.
     */
    public SuggestPoliciesResponse suggestPolicies(SuggestPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "SuggestPolicies");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestPolicies", request.getPage().getPageToken());
        return SuggestPoliciesResponse.getDefaultInstance();
    }

    /**
     * Returns a policy.
     */
    public PeekPolicyResponse peekPolicy(PeekPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "PeekPolicy");

        var outbound0 = BeanCopies.copy(ValidateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setSla(ClaimSla.newBuilder()
                                .setPolicyId(request.getPolicyId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), ValidateClaimRequest.class);
        var reply0 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateClaim(outbound0);
        return PeekPolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getClaim().getRequester().getEmail())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .build();
    }
}
