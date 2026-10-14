package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ExportAuditTrailsRequest;
import com.acme.audit.v1.ExportAuditTrailsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.ExportHouseholdsRequest;
import com.acme.customers.v1.ExportHouseholdsResponse;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivateCoverageRequest;
import com.acme.insurance.v1.ActivateCoverageResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.ExpireCoverageRequest;
import com.acme.insurance.v1.ExpireCoverageResponse;
import com.acme.insurance.v1.GenerateCoverageReportRequest;
import com.acme.insurance.v1.GenerateCoverageReportResponse;
import com.acme.insurance.v1.GetCoverageLifecycleRequest;
import com.acme.insurance.v1.GetCoverageLifecycleResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.PushUnderwritingRequest;
import com.acme.insurance.v1.PushUnderwritingResponse;
import com.acme.insurance.v1.ReinstateCoverageRequest;
import com.acme.insurance.v1.ReinstateCoverageResponse;
import com.acme.insurance.v1.ResolvePoliciesRequest;
import com.acme.insurance.v1.ResolvePoliciesResponse;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.insurance.v1.SummarizePoliciesRequest;
import com.acme.insurance.v1.SummarizePoliciesResponse;
import com.acme.insurance.v1.SuspendCoverageRequest;
import com.acme.insurance.v1.SuspendCoverageResponse;
import com.acme.insurance.v1.TerminateCoverageRequest;
import com.acme.insurance.v1.TerminateCoverageResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.FindJournalEntriesRequest;
import com.acme.ledger.v1.FindJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.lending.v1.CheckCollateralEligibilityRequest;
import com.acme.lending.v1.CheckCollateralEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Moves a coverage between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class CoverageLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(CoverageLifecycleApplication.class);

    private final AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public CoverageLifecycleApplication(
            AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking,
            PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.auditAuditTrailQueryServiceBlocking = auditAuditTrailQueryServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.insuranceCoverageReportingServiceBlocking = insuranceCoverageReportingServiceBlocking;
        this.insurancePolicyQueryServiceBlocking = insurancePolicyQueryServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingImportServiceBlocking = insuranceUnderwritingImportServiceBlocking;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    /**
     * Activates a coverage.
     */
    public ActivateCoverageResponse activateCoverage(ActivateCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "ActivateCoverage");

        var outbound0 = FindHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findHouseholds(outbound0), rpcExecutor);

        var outbound1 = PushPremiumRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(PushPremiumRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushPremium(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ValidateSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPremium().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getPremium().getBillingAddress().getCountryCode())
                                .addAllLines(reply1.getPremium().getBillingAddress().getLinesList())
                                .build())
                        .setCard(SupplierRatingCardSettlement.newBuilder()
                                .setLast4(reply1.getPremium().getCard().getLast4())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSupplierRating(value)).orElseThrow();

        var outbound3 = CheckCollateralEligibilityRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPremium().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getPremium().getCounterparty().getDisplayName())
                                .setEmail(reply1.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckCollateralEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCollateralEligibility(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateCoverage", request.getDetails().getReasonCode());
        return ActivateCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .build())
                .setOutcome(ActivateCoverageResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Suspends a coverage.
     */
    public SuspendCoverageResponse suspendCoverage(SuspendCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "SuspendCoverage");

        var outbound0 = GenerateCoverageReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setCoverageId(request.getCoverageId())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insuranceCoverageReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateCoverageReport(outbound0), rpcExecutor);

        var outbound1 = CalculateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateComplianceCase(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = (ExplainHouseholdDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(reply1.getComplianceCase().getAudit().getEtag())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply1.getComplianceCase().getAssignee().getDisplayName())
                                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                                .setUserId(reply1.getComplianceCase().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainHouseholdDecision(outbound2);

        var outbound3 = BeanCopies.copy(FindJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getComplianceCase().getAudit().getCreatedAt())
                        .setOwnerId(reply1.getComplianceCase().getAssignee().getUserId())
                        .setRegion(reply2.getHousehold().getLocation().getRegion())
                        .build())
                .build(), FindJournalEntriesRequest.class);
        var reply3 = ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findJournalEntries(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendCoverage", request.getDetails().getReasonCode());
        return SuspendCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setName(reply2.getHousehold().getName())
                        .build())
                .setOutcome(SuspendCoverageResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a coverage.
     */
    public ReinstateCoverageResponse reinstateCoverage(ReinstateCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "ReinstateCoverage");

        var outbound0 = ResolvePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePolicies(outbound0), rpcExecutor);

        var outbound1 = AnnotateJournalEntryVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateJournalEntryVersionRequest.Details.newBuilder()
                        .setAuthorEmail(request.getDetails().getNewOwner().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateJournalEntryVersion(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateCoverage", request.getDetails().getReasonCode());
        return ReinstateCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getJournalEntry().getAudit().getUpdatedAt())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .build())
                .setOutcome(ReinstateCoverageResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Terminates a coverage.
     */
    public TerminateCoverageResponse terminateCoverage(TerminateCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "TerminateCoverage");

        var outbound0 = BeanCopies.copy(PushUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushUnderwritingRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build(), PushUnderwritingRequest.class);
        var reply0 = insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushUnderwriting(outbound0);

        var outbound1 = ExportHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getUnderwriting().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getUnderwriting().getAssignee().getUserId())
                        .build())
                .build();
        var stream1 = customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportHouseholds(outbound1);
        var reply1 = ExportHouseholdsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = ExplainComplianceCaseCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainComplianceCaseCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.compliance.v1.ComplianceCaseCalculationService");
        var selected2 = ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.explainComplianceCaseCalculation(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateCoverage", request.getDetails().getReasonCode());
        return TerminateCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getUnderwriting().getCustomer().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setOutcome(TerminateCoverageResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Expires a coverage.
     */
    public ExpireCoverageResponse expireCoverage(ExpireCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "ExpireCoverage");

        var outbound0 = ListMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listMetricSerieses(value));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireCoverage", request.getDetails().getReasonCode());
        return ExpireCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .build())
                .setOutcome(ExpireCoverageResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a coverage.
     */
    public GetCoverageLifecycleResponse getCoverageLifecycle(GetCoverageLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCoverageLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "GetCoverageLifecycle");

        var outbound0 = SummarizePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizePoliciesRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.PolicyQueryService");
        var selected0 = PolicyQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.summarizePolicies(outbound0);
        return GetCoverageLifecycleResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Schedules a coverage.
     */
    public ScheduleCoverageExpiryResponse scheduleCoverageExpiry(ScheduleCoverageExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleCoverageExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = JsonCodec.roundTrip(CreateSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .build())
                        .setName(request.getDetails().getNotify().getName())
                        .build())
                .build(), CreateSupplierRequest.class);
        var reply0 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createSupplier(outbound0);

        var outbound1 = ExportAuditTrailsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportAuditTrailsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCoverage().getAudit().getCreatedAt())
                        .putAllLabels(request.getCoverage().getParametersMap())
                        .setOwnerId(request.getCoverage().getOwner().getUserId())
                        .setRegion(reply0.getSupplier().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var stream1 = auditAuditTrailQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportAuditTrails(outbound1);
        var reply1 = ExportAuditTrailsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = ValidateUnderwritingImportRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCoverage().getAudit().getCreatedBy())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(request.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getSupplier().getRelationshipManager().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> insuranceUnderwritingImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUnderwritingImport(value));
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleCoverageExpiry", request.getCoverage().getCoverageId());
        return ScheduleCoverageExpiryResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getUnderwriting().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getSupplier().getAudit().getCreatedBy())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .putAllParameters(request.getDetails().getParametersMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
