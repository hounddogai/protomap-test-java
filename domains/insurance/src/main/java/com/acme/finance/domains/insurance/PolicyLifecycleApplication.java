package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.ValidateChartOfAccountsRequest;
import com.acme.accounting.v1.ValidateChartOfAccountsResponse;
import com.acme.analytics.v1.AnalyticsEvent;
import com.acme.analytics.v1.AnalyticsEventServiceGrpc;
import com.acme.analytics.v1.CreateAnalyticsEventRequest;
import com.acme.analytics.v1.CreateAnalyticsEventResponse;
import com.acme.analytics.v1.FunnelRef;
import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.ListComplianceCasesRequest;
import com.acme.compliance.v1.ListComplianceCasesResponse;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.LintDocumentsRequest;
import com.acme.documents.v1.LintDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.giftcards.v1.CreateGiftCardRequest;
import com.acme.giftcards.v1.CreateGiftCardResponse;
import com.acme.giftcards.v1.GiftCard;
import com.acme.giftcards.v1.GiftCardServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivatePolicyRequest;
import com.acme.insurance.v1.ActivatePolicyResponse;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageRouteAction;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExpirePolicyRequest;
import com.acme.insurance.v1.ExpirePolicyResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.ExplainUnderwritingDecisionRequest;
import com.acme.insurance.v1.ExplainUnderwritingDecisionResponse;
import com.acme.insurance.v1.GetCoverageRulesRequest;
import com.acme.insurance.v1.GetCoverageRulesResponse;
import com.acme.insurance.v1.GetPolicyLifecycleRequest;
import com.acme.insurance.v1.GetPolicyLifecycleResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.ReinstatePolicyRequest;
import com.acme.insurance.v1.ReinstatePolicyResponse;
import com.acme.insurance.v1.RenewPolicyRequest;
import com.acme.insurance.v1.RenewPolicyResponse;
import com.acme.insurance.v1.SchedulePolicyExpiryRequest;
import com.acme.insurance.v1.SchedulePolicyExpiryResponse;
import com.acme.insurance.v1.SuspendPolicyRequest;
import com.acme.insurance.v1.SuspendPolicyResponse;
import com.acme.insurance.v1.TerminatePolicyRequest;
import com.acme.insurance.v1.TerminatePolicyResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.PostingBatchLineTax;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ValidatePostingBatchRequest;
import com.acme.ledger.v1.ValidatePostingBatchResponse;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.ExplainPayStubCalculationRequest;
import com.acme.payroll.v1.ExplainPayStubCalculationResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.treasury.v1.BatchCreateCashPositionsRequest;
import com.acme.treasury.v1.BatchCreateCashPositionsResponse;
import com.acme.treasury.v1.CashPositionServiceGrpc;
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
 * Moves a policy between the states of its lifecycle. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class PolicyLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(PolicyLifecycleApplication.class);

    private final ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking;

    private final AnalyticsEventServiceGrpc.AnalyticsEventServiceBlockingStub analyticsAnalyticsEventServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceBlockingStub complianceComplianceCaseServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceFutureStub customersCustomerSegmentValidationServiceFuture;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final GiftCardServiceGrpc.GiftCardServiceBlockingStub giftcardsGiftCardServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final PolicyLifecycleRequestMapper mapper;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final CashPositionServiceGrpc.CashPositionServiceStub treasuryCashPositionServiceAsync;

    public PolicyLifecycleApplication(
            ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking,
            AnalyticsEventServiceGrpc.AnalyticsEventServiceBlockingStub analyticsAnalyticsEventServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceBlockingStub complianceComplianceCaseServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceFutureStub customersCustomerSegmentValidationServiceFuture,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            GiftCardServiceGrpc.GiftCardServiceBlockingStub giftcardsGiftCardServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            PolicyLifecycleRequestMapper mapper,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            CashPositionServiceGrpc.CashPositionServiceStub treasuryCashPositionServiceAsync) {
        this.accountingChartOfAccountsValidationServiceBlocking = accountingChartOfAccountsValidationServiceBlocking;
        this.analyticsAnalyticsEventServiceBlocking = analyticsAnalyticsEventServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.complianceComplianceCaseServiceBlocking = complianceComplianceCaseServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.customersCustomerSegmentValidationServiceFuture = customersCustomerSegmentValidationServiceFuture;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.giftcardsGiftCardServiceBlocking = giftcardsGiftCardServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.mapper = mapper;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.treasuryCashPositionServiceAsync = treasuryCashPositionServiceAsync;
    }

    /**
     * Activates a policy.
     */
    public ActivatePolicyResponse activatePolicy(ActivatePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivatePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "ActivatePolicy");

        var outbound0 = mapper.activatePolicyStage0(request);
        var reply0 = insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainUnderwritingDecision(outbound0);

        var outbound1 = CountSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSupplierContractsRequest.Filter.newBuilder()
                        .setOwnerId(switch (reply0.getUnderwriting().getOriginCase()) {
                            case CUSTOMER -> reply0.getUnderwriting().getCustomer().getClaimId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply1 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSupplierContracts(outbound1);

        var outbound2 = LintDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getUnderwriting().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            LintDocumentsResponse fetch(LintDocumentsRequest input) {
                return documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintDocuments(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = BatchCreateCashPositionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCashPositionsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateCashPositionsResponse.getDefaultInstance());
        var input3 = treasuryCashPositionServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCashPositions(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivatePolicy", request.getDetails().getReasonCode());
        return ActivatePolicyResponse.newBuilder()
                .setOutcome(ActivatePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getDocument().getAudit().getEtag())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .build();
    }

    /**
     * Suspends a policy.
     */
    public SuspendPolicyResponse suspendPolicy(SuspendPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "SuspendPolicy");

        var outbound0 = ListComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListComplianceCasesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> complianceComplianceCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listComplianceCases(outbound0), rpcExecutor);

        var outbound1 = ExplainPayStubCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPayStubCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPayStubCalculation(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var builder2 = QuoteCoverageRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setCoverage(Coverage.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(reply1.getPayStub().getAudit().getUpdatedBy())
                        .build())
                .build());
        builder2.setDetails(QuoteCoverageRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .addAllLines(reply1.getPayStub().getBillingAddress().getLinesList())
                        .setRegion(reply1.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCoverage(outbound2);

        var builder3 = ExplainCoverageDecisionRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setCoverage(Coverage.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(reply2.getCoverage().getAudit().getUpdatedBy())
                        .build())
                .build());
        builder3.setDetails(ExplainCoverageDecisionRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCoverageDecision(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendPolicy", request.getDetails().getReasonCode());
        return SuspendPolicyResponse.newBuilder()
                .setOutcome(SuspendPolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setReferenceId(reply3.getOutcome().getReferenceId())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getCoverage().getAudit().getEtag())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Reinstates a policy.
     */
    public ReinstatePolicyResponse reinstatePolicy(ReinstatePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstatePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "ReinstatePolicy");

        var outbound0 = ValidateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setDetails(ValidateCustomerSegmentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        Function<ValidateCustomerSegmentRequest, ValidateCustomerSegmentResponse> operation0 = customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateCustomerSegment;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = JsonCodec.roundTrip(EstimateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setEtag(reply0.getCustomerSegment().getAudit().getEtag())
                                        .build())
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getCustomerSegment().getAudit().getCreatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply0.getCustomerSegment().getOwner().getEmail())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                                        .build())
                                .build())
                        .build())
                .build(), EstimateHouseholdRequest.class);
        var reply1 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateHousehold(outbound1);

        var builder2 = CreateAnalyticsEventRequest.newBuilder();
        builder2.setAnalyticsEvent(AnalyticsEvent.newBuilder()
                .setCorrelationId(reply1.getResult().getCorrelationId())
                .setDetail(Any.pack(request.getContext()))
                .setSubject(FunnelRef.newBuilder()
                        .setDisplayName(reply0.getCustomerSegment().getOwner().getDisplayName())
                        .build())
                .build());
        builder2.setContext(request.getContext());
        var outbound2 = builder2.build();
        var reply2 = analyticsAnalyticsEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAnalyticsEvent(outbound2);

        var outbound3 = ExplainCoverageCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setRoute(CoverageRouteAction.newBuilder()
                                .setTarget(switch (reply0.getCustomerSegment().getActionCase()) {
                                    case ROUTE -> reply0.getCustomerSegment().getRoute().getTarget();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setDetails(ExplainCoverageCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getCustomerSegment().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCoverageCalculation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstatePolicy", request.getDetails().getReasonCode());
        return ReinstatePolicyResponse.newBuilder()
                .setOutcome(ReinstatePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply3.getOutcome().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getHousehold().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply3.getCoverage().getOwner().getEmail())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getAnalyticsEvent().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates a policy.
     */
    public TerminatePolicyResponse terminatePolicy(TerminatePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminatePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "TerminatePolicy");

        var outbound0 = BeanCopies.copy(ExplainClaimCalculationRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainClaimCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), ExplainClaimCalculationRequest.class);
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainClaimCalculation(outbound0);

        var outbound1 = LintCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getClaim().getRequester().getEmail())
                                .build())
                        .build())
                .setDetails(LintCustomerSegmentsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending1 = customersCustomerSegmentValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomerSegments(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var builder2 = CreateGiftCardRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setGiftCard(GiftCard.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(reply1.getCustomerSegment().getOwner().getEmail())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = giftcardsGiftCardServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createGiftCard(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminatePolicy", request.getDetails().getReasonCode());
        return TerminatePolicyResponse.newBuilder()
                .setOutcome(TerminatePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getGiftCard().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getClaim().getRequester().getEmail())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Renews a policy.
     */
    public RenewPolicyResponse renewPolicy(RenewPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "RenewPolicy");

        var outbound0 = CalculateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                        .build())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<CalculateHouseholdRequest, CalculateHouseholdResponse>() {
            @Override
            public CalculateHouseholdResponse apply(CalculateHouseholdRequest input) {
                return customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateHousehold(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = RecalculateHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        Function<RecalculateHouseholdsRequest, RecalculateHouseholdsResponse> operation1 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateHouseholds;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewPolicy", request.getDetails().getReasonCode());
        return RenewPolicyResponse.newBuilder()
                .setOutcome(RenewPolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getHousehold().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getHousehold().getManager().getEmail())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply1.getHousehold().getManager().getDisplayName())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .build();
    }

    /**
     * Expires a policy.
     */
    public ExpirePolicyResponse expirePolicy(ExpirePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpirePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "ExpirePolicy");

        var outbound0 = RpcSupport.unpack(Any.pack(CalculatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build()), CalculatePolicyRequest.class);
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePolicy(outbound0);

        var outbound1 = CheckFixedAssetEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckFixedAssetEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkFixedAssetEligibility(value));

        var outbound2 = (ValidateChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getPolicy().getHolder().getDisplayName())
                                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply2 = accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateChartOfAccounts(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpirePolicy", request.getDetails().getReasonCode());
        return ExpirePolicyResponse.newBuilder()
                .setOutcome(ExpirePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply2.getOutcome().getMessage())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAvailable(reply0.getPolicy().getAvailable())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a policy.
     */
    public GetPolicyLifecycleResponse getPolicyLifecycle(GetPolicyLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPolicyLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "GetPolicyLifecycle");

        var outbound0 = RpcSupport.unpack(Any.pack(NormalizeCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeCoverageRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build()), NormalizeCoverageRequest.class);
        var reply0 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCoverage(outbound0);
        return GetPolicyLifecycleResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCoverage().getAudit().getCreatedBy())
                                .build())
                        .addLimits(PolicyLimit.newBuilder()
                                .setName(reply0.getCoverage().getName())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .build();
    }

    /**
     * Schedules a policy.
     */
    public SchedulePolicyExpiryResponse schedulePolicyExpiry(SchedulePolicyExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SchedulePolicyExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = GetCoverageRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverageId(request.getPolicy().getHolder().getCoverageId())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getCoverageRules(value));

        var results1 = new ArrayList<SimulateCustomerSegmentResponse>();
        for (var element : request.getPolicy().getLimitsList()) {
            SimulateCustomerSegmentRequest itemRequest = SimulateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setDiscount(CustomerSegmentDiscountAction.newBuilder()
                                .setAmount(element.getAmount())
                                .setPercent(reply0.getCoverage().getDiscount().getPercent())
                                .build())
                        .setName(element.getName())
                        .build())
                .build();
            results1.add(customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCustomerSegment(itemRequest));
        }
        var reply1 = results1.isEmpty() ? SimulateCustomerSegmentResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = mapper.schedulePolicyExpiryStage2(request, reply1);
        var reply2 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePostingBatch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SchedulePolicyExpiry", request.getPolicy().getPolicyId());
        return SchedulePolicyExpiryResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setHolder(request.getPolicy().getHolder())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
