package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareConsentRecordOptionsRequest;
import com.acme.customers.v1.CompareConsentRecordOptionsResponse;
import com.acme.customers.v1.ComparePreferenceOptionsRequest;
import com.acme.customers.v1.ComparePreferenceOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExplainHouseholdCalculationRequest;
import com.acme.customers.v1.ExplainHouseholdCalculationResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceBlockAction;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.fleet.v1.ResolveVehiclesRequest;
import com.acme.fleet.v1.ResolveVehiclesResponse;
import com.acme.fleet.v1.VehicleQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivatePremiumRequest;
import com.acme.insurance.v1.ActivatePremiumResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.CountClaimsRequest;
import com.acme.insurance.v1.CountClaimsResponse;
import com.acme.insurance.v1.ExpirePremiumRequest;
import com.acme.insurance.v1.ExpirePremiumResponse;
import com.acme.insurance.v1.GetPremiumLifecycleRequest;
import com.acme.insurance.v1.GetPremiumLifecycleResponse;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ReinstatePremiumRequest;
import com.acme.insurance.v1.ReinstatePremiumResponse;
import com.acme.insurance.v1.RenewPremiumRequest;
import com.acme.insurance.v1.RenewPremiumResponse;
import com.acme.insurance.v1.SchedulePremiumExpiryRequest;
import com.acme.insurance.v1.SchedulePremiumExpiryResponse;
import com.acme.insurance.v1.ScoreClaimRequest;
import com.acme.insurance.v1.ScoreClaimResponse;
import com.acme.insurance.v1.ShareUnderwritingReportRequest;
import com.acme.insurance.v1.ShareUnderwritingReportResponse;
import com.acme.insurance.v1.SummarizeClaimsRequest;
import com.acme.insurance.v1.SummarizeClaimsResponse;
import com.acme.insurance.v1.SuspendPremiumRequest;
import com.acme.insurance.v1.SuspendPremiumResponse;
import com.acme.insurance.v1.TerminatePremiumRequest;
import com.acme.insurance.v1.TerminatePremiumResponse;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.lending.v1.ExportLoanApplicationsRequest;
import com.acme.lending.v1.ExportLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.payments.v1.Payment;
import com.acme.payments.v1.PaymentCalculationServiceGrpc;
import com.acme.payments.v1.RefundRef;
import com.acme.payments.v1.SimulatePaymentRequest;
import com.acme.payments.v1.SimulatePaymentResponse;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PreviewPayStubRequest;
import com.acme.payroll.v1.PreviewPayStubResponse;
import com.acme.procurement.v1.CalculatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CalculatePurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.treasury.v1.CalculateFxHedgeRequest;
import com.acme.treasury.v1.CalculateFxHedgeResponse;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.google.protobuf.Any;
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
 * Moves a premium between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class PremiumLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(PremiumLifecycleApplication.class);

    private final ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync;

    private final DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final VehicleQueryServiceGrpc.VehicleQueryServiceBlockingStub fleetVehicleQueryServiceBlocking;

    private final ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceFutureStub insuranceClaimValidationServiceFuture;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking;

    private final PremiumLifecycleRequestMapper mapper;

    private final PaymentCalculationServiceGrpc.PaymentCalculationServiceBlockingStub paymentsPaymentCalculationServiceBlocking;

    private final PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking;

    private final InsuranceRepository repository;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    public PremiumLifecycleApplication(
            ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync,
            DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            ChannelRegistry channels,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            VehicleQueryServiceGrpc.VehicleQueryServiceBlockingStub fleetVehicleQueryServiceBlocking,
            ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking,
            ClaimValidationServiceGrpc.ClaimValidationServiceFutureStub insuranceClaimValidationServiceFuture,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking,
            PremiumLifecycleRequestMapper mapper,
            PaymentCalculationServiceGrpc.PaymentCalculationServiceBlockingStub paymentsPaymentCalculationServiceBlocking,
            PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking,
            InsuranceRepository repository,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking) {
        this.accountingChartOfAccountsServiceAsync = accountingChartOfAccountsServiceAsync;
        this.assetsDepreciationCalculationServiceBlocking = assetsDepreciationCalculationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.channels = channels;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.fleetVehicleQueryServiceBlocking = fleetVehicleQueryServiceBlocking;
        this.insuranceClaimQueryServiceBlocking = insuranceClaimQueryServiceBlocking;
        this.insuranceClaimValidationServiceFuture = insuranceClaimValidationServiceFuture;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.lendingLoanApplicationQueryServiceBlocking = lendingLoanApplicationQueryServiceBlocking;
        this.mapper = mapper;
        this.paymentsPaymentCalculationServiceBlocking = paymentsPaymentCalculationServiceBlocking;
        this.payrollPayStubValidationServiceBlocking = payrollPayStubValidationServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceBlocking = procurementPurchaseRequisitionCalculationServiceBlocking;
        this.repository = repository;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
    }

    /**
     * Activates a premium.
     */
    public ActivatePremiumResponse activatePremium(ActivatePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivatePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "ActivatePremium");

        var outbound0 = ComparePreferenceOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setBlock(PreferenceBlockAction.newBuilder()
                                .setNotifyEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ComparePreferenceOptionsResponse fetch(ComparePreferenceOptionsRequest input) {
                return customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePreferenceOptions(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var builder1 = CreateSupplierRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setSupplier(Supplier.newBuilder()
                .setRelationshipManager(UserRef.newBuilder()
                        .setEmail(request.getDetails().getNewOwner().getEmail())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createSupplier(outbound1);

        var outbound2 = BatchCreateChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateChartOfAccountsesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateChartOfAccountsesResponse.getDefaultInstance());
        var input2 = accountingChartOfAccountsServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateChartOfAccountses(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
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
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivatePremium", request.getDetails().getReasonCode());
        return ActivatePremiumResponse.newBuilder()
                .setOutcome(ActivatePremiumResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getSupplier().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getPreference().getAudit().getUpdatedBy())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .build();
    }

    /**
     * Suspends a premium.
     */
    public SuspendPremiumResponse suspendPremium(SuspendPremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendPremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "SuspendPremium");

        var outbound0 = (SimulatePaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setPayment(Payment.newBuilder()
                        .setCounterparty(RefundRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply0 = paymentsPaymentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePayment(outbound0);

        var outbound1 = CalculatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getPayment().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePurchaseRequisition(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = (ResolveVehiclesRequest.newBuilder()
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply2 = fleetVehicleQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveVehicles(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendPremium", request.getDetails().getReasonCode());
        return SuspendPremiumResponse.newBuilder()
                .setOutcome(SuspendPremiumResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setDueAt(reply0.getPayment().getDueAt())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Reinstates a premium.
     */
    public ReinstatePremiumResponse reinstatePremium(ReinstatePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstatePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "ReinstatePremium");

        var outbound0 = SummarizeClaimsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        Function<SummarizeClaimsRequest, SummarizeClaimsResponse> operation0 = insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::summarizeClaims;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = QuoteDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                                        .setName(request.getDetails().getNotifyContact().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteDepreciation(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = ScoreClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending2 = insuranceClaimValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scoreClaim(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstatePremium", request.getDetails().getReasonCode());
        return ReinstatePremiumResponse.newBuilder()
                .setOutcome(ReinstatePremiumResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply2.getClaim().getCustomer().getEmail())
                                .build())
                        .setDueAt(reply2.getClaim().getSla().getDueAt())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Terminates a premium.
     */
    public TerminatePremiumResponse terminatePremium(TerminatePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminatePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "TerminatePremium");

        var outbound0 = CalculateFxHedgeRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        RpcPort<CalculateFxHedgeRequest, CalculateFxHedgeResponse> port0 = RpcPort.adapt(treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::calculateFxHedge);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = BeanCopies.copy(ListPremiumConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPremiumConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getFxHedge().getDueAt())
                        .setRegion(reply0.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .build(), ListPremiumConflictsRequest.class);
        var reply1 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPremiumConflicts(outbound1);

        var outbound2 = ShareUnderwritingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareUnderwritingReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.insurance.v1.UnderwritingReportingService");
        var serviceName2 = "acme.insurance.v1" + "." + "UnderwritingReportingService";
        var methodName2 = "Sha" + "reUnderwritingReport";
        var descriptor2 = MethodDescriptor.<ShareUnderwritingReportRequest, ShareUnderwritingReportResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ShareUnderwritingReportRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ShareUnderwritingReportResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(UpdateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getFxHedge().getBillingAddress().getLinesList())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getFxHedge().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getUnderwriting().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build()), UpdateSearchIndexRequest.class);
        var reply3 = searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSearchIndex(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminatePremium", request.getDetails().getReasonCode());
        return TerminatePremiumResponse.newBuilder()
                .setOutcome(TerminatePremiumResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getUnderwriting().getAudit().getEtag())
                                .setUpdatedBy(reply0.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .putAllMetadata(reply0.getFxHedge().getMetadataMap())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply2.getOutcome().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Renews a premium.
     */
    public RenewPremiumResponse renewPremium(RenewPremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewPremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "RenewPremium");

        var outbound0 = PreviewPayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewPayStubRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> payrollPayStubValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPayStub(value));

        var outbound1 = CompareConsentRecordOptionsRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(reply0.getPayStub().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareConsentRecordOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPayStub().getBillingAddress().getCountryCode())
                                .setPostalCode(reply0.getPayStub().getBillingAddress().getPostalCode())
                                .setRegion(reply0.getPayStub().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply1 = customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareConsentRecordOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewPremium", request.getDetails().getReasonCode());
        return RenewPremiumResponse.newBuilder()
                .setOutcome(RenewPremiumResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getPayStub().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply1.getConsentRecord().getSubject().getDisplayName())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .build();
    }

    /**
     * Expires a premium.
     */
    public ExpirePremiumResponse expirePremium(ExpirePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpirePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "ExpirePremium");

        var outbound0 = ExportLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var stream0 = lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportLoanApplications(outbound0);
        var reply0 = ExportLoanApplicationsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RpcSupport.unpack(Any.pack(RecalculateMandatesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build()), RecalculateMandatesRequest.class);
        var reply1 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateMandates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpirePremium", request.getDetails().getReasonCode());
        return ExpirePremiumResponse.newBuilder()
                .setOutcome(ExpirePremiumResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setPremiumId(request.getPremiumId())
                        .setTotal(reply1.getMandate().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a premium.
     */
    public GetPremiumLifecycleResponse getPremiumLifecycle(GetPremiumLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPremiumLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "GetPremiumLifecycle");

        var outbound0 = CountClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountClaimsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getPremiumId())
                        .build())
                .build();
        RpcPort<CountClaimsRequest, CountClaimsResponse> port0 = RpcPort.adapt(insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::countClaims);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = mapper.getPremiumLifecycleStage1(request);
        var reply1 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePreferenceOptions(outbound1);
        return GetPremiumLifecycleResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getPreference().getAudit().getUpdatedAt())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Schedules a premium.
     */
    public SchedulePremiumExpiryResponse schedulePremiumExpiry(SchedulePremiumExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SchedulePremiumExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = JsonCodec.roundTrip(ExplainHouseholdCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getPremium().getBillingAddress().getLocality())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getPremium().getCounterparty().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), ExplainHouseholdCalculationRequest.class);
        var reply0 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainHouseholdCalculation(outbound0);

        var outbound1 = JsonCodec.roundTrip(ExplainMandateCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainMandateCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build(), ExplainMandateCalculationRequest.class);
        var reply1 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMandateCalculation(outbound1);

        var outbound2 = ListUnderwritingConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListUnderwritingConflictsRequest.Filter.newBuilder()
                        .setRegion(reply1.getMandate().getBillingAddress().getRegion())
                        .build())
                .build();
        Function<ListUnderwritingConflictsRequest, ListUnderwritingConflictsResponse> operation2 = insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listUnderwritingConflicts;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SchedulePremiumExpiry", request.getPremium().getPremiumId());
        return SchedulePremiumExpiryResponse.newBuilder()
                .setOutcome(SchedulePremiumExpiryResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getHousehold().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getHousehold().getManager().getEmail())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .setTotal(request.getPremium().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }
}
