package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.ExplainAllocationDecisionRequest;
import com.acme.accounting.v1.ExplainAllocationDecisionResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.billing.v1.ExplainPaymentTermCalculationRequest;
import com.acme.billing.v1.ExplainPaymentTermCalculationResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.ExplainHouseholdCalculationRequest;
import com.acme.customers.v1.ExplainHouseholdCalculationResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvancePolicyWorkflowRequest;
import com.acme.insurance.v1.AdvancePolicyWorkflowResponse;
import com.acme.insurance.v1.ApprovePolicyRequest;
import com.acme.insurance.v1.ApprovePolicyResponse;
import com.acme.insurance.v1.ApprovePremiumRequest;
import com.acme.insurance.v1.ApprovePremiumResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimLifecycleServiceGrpc;
import com.acme.insurance.v1.CommentPremiumApprovalRequest;
import com.acme.insurance.v1.CommentPremiumApprovalResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.DelegatePremiumApprovalRequest;
import com.acme.insurance.v1.DelegatePremiumApprovalResponse;
import com.acme.insurance.v1.EscalatePremiumRequest;
import com.acme.insurance.v1.EscalatePremiumResponse;
import com.acme.insurance.v1.GetPremiumApprovalStatusRequest;
import com.acme.insurance.v1.GetPremiumApprovalStatusResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCardSettlement;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumLineTax;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.RecallPremiumRequest;
import com.acme.insurance.v1.RecallPremiumResponse;
import com.acme.insurance.v1.RejectPremiumRequest;
import com.acme.insurance.v1.RejectPremiumResponse;
import com.acme.insurance.v1.ScheduleClaimExpiryRequest;
import com.acme.insurance.v1.ScheduleClaimExpiryResponse;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.insurance.v1.StreamUnderwritingMetricsRequest;
import com.acme.insurance.v1.StreamUnderwritingMetricsResponse;
import com.acme.insurance.v1.SubmitPremiumRequest;
import com.acme.insurance.v1.SubmitPremiumResponse;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.ledger.v1.SearchLedgerAccountsRequest;
import com.acme.ledger.v1.SearchLedgerAccountsResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.ValidatePurchaseOrderRequest;
import com.acme.procurement.v1.ValidatePurchaseOrderResponse;
import com.acme.suppliers.v1.DeleteSupplierRequest;
import com.acme.suppliers.v1.DeleteSupplierResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Routes premiums through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class PremiumApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(PremiumApprovalApplication.class);

    private final AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking;

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final ClaimLifecycleServiceGrpc.ClaimLifecycleServiceBlockingStub insuranceClaimLifecycleServiceBlocking;

    private final CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final PremiumApprovalRequestMapper mapper;

    private final PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public PremiumApprovalApplication(
            AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking,
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking,
            ChannelRegistry channels,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            ClaimLifecycleServiceGrpc.ClaimLifecycleServiceBlockingStub insuranceClaimLifecycleServiceBlocking,
            CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            PremiumApprovalRequestMapper mapper,
            PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.accountingAllocationValidationServiceBlocking = accountingAllocationValidationServiceBlocking;
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.billingPaymentTermCalculationServiceBlocking = billingPaymentTermCalculationServiceBlocking;
        this.channels = channels;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.insuranceClaimLifecycleServiceBlocking = insuranceClaimLifecycleServiceBlocking;
        this.insuranceCoverageLifecycleServiceBlocking = insuranceCoverageLifecycleServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.insurancePolicyWorkflowServiceBlocking = insurancePolicyWorkflowServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.mapper = mapper;
        this.procurementPurchaseOrderValidationServiceBlocking = procurementPurchaseOrderValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    /**
     * Submits a premium.
     */
    public SubmitPremiumResponse submitPremium(SubmitPremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitPremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = JsonCodec.roundTrip(CompareHouseholdOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedAt(request.getPremium().getAudit().getUpdatedAt())
                                        .build())
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getPremium().getCounterparty().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), CompareHouseholdOptionsRequest.class);
        var reply0 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareHouseholdOptions(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ExplainHouseholdCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getPremium().getAudit().getUpdatedAt())
                                                .setUpdatedBy(reply0.getHousehold().getAudit().getUpdatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(reply0.getHousehold().getManager().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()), ExplainHouseholdCalculationRequest.class);
        var reply1 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainHouseholdCalculation(outbound1);

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input2 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPremium().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPremium().getAudit().getCreatedBy())
                                .setEtag(request.getPremium().getAudit().getEtag())
                                .setUpdatedAt(request.getPremium().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitPremium", request.getPremium().getPremiumId());
        return SubmitPremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getHousehold().getLocation().getPostalCode())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getHousehold().getManager().getEmail())
                                .build())
                        .setIssuedAt(request.getPremium().getIssuedAt())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Approves a premium.
     */
    public ApprovePremiumResponse approvePremium(ApprovePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApprovePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = DeleteSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setEtag(request.getPremium().getAudit().getEtag())
                .setReason(request.getReason())
                .setSupplierId(request.getPremium().getCounterparty().getSupplierId())
                .build();
        final class Stage0Adapter {
            DeleteSupplierResponse fetch(DeleteSupplierRequest input) {
                return suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deleteSupplier(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var builder1 = ListLedgerAccountsRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(ListLedgerAccountsRequest.Filter.newBuilder()
                .setRegion(request.getPremium().getBillingAddress().getRegion())
                .build());
        var outbound1 = builder1.build();
        var reply1 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccounts(outbound1);

        var outbound2 = ValidatePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getPremium().getBillingAddress().getCountryCode())
                                .setRegion(request.getPremium().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<ValidatePurchaseOrderRequest, ValidatePurchaseOrderResponse> port2 = RpcPort.adapt(procurementPurchaseOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validatePurchaseOrder);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApprovePremium", request.getPremium().getPremiumId());
        return ApprovePremiumResponse.newBuilder()
                .setOutcome(ApprovePremiumResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply2.getPurchaseOrder().getBillingAddress().getLinesList())
                                .build())
                        .setNumber(request.getPremium().getNumber())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Rejects a premium.
     */
    public RejectPremiumResponse rejectPremium(RejectPremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectPremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = SearchLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPremium().getIssuedAt())
                        .setRegion(request.getPremium().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.ledger.v1.LedgerAccountService");
        var selected0 = LedgerAccountServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.searchLedgerAccounts(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectPremium", request.getPremium().getPremiumId());
        return RejectPremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setPremiumId(request.getPremiumId())
                        .setTotal(request.getPremium().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Escalates a premium.
     */
    public EscalatePremiumResponse escalatePremium(EscalatePremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalatePremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = RecalculateCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPremium().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getPremium().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomerSegments(outbound0), rpcExecutor);

        var outbound1 = AdvancePolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setBank(PolicyBankFunding.newBuilder()
                                .setBic(request.getPremium().getBank().getBic())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePolicyWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advancePolicyWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalatePremium", request.getPremium().getPremiumId());
        return EscalatePremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCustomerSegment().getAudit().getCreatedBy())
                                .setEtag(reply0.getCustomerSegment().getAudit().getEtag())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply1.getPolicy().getContact().getEmail())
                                .build())
                        .setDueAt(request.getPremium().getDueAt())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .build();
    }

    /**
     * Delegates a premium.
     */
    public DelegatePremiumApprovalResponse delegatePremiumApproval(DelegatePremiumApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegatePremiumApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = ScheduleClaimExpiryRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<ScheduleClaimExpiryRequest, ScheduleClaimExpiryResponse>() {
            @Override
            public ScheduleClaimExpiryResponse apply(ScheduleClaimExpiryRequest input) {
                return insuranceClaimLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleClaimExpiry(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegatePremiumApproval", request.getPremium().getPremiumId());
        return DelegatePremiumApprovalResponse.newBuilder()
                .setOutcome(DelegatePremiumApprovalResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setNumber(request.getPremium().getNumber())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .build();
    }

    /**
     * Recalls a premium.
     */
    public RecallPremiumResponse recallPremium(RecallPremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallPremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = ExplainPaymentTermCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPremium().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.billing.v1.PaymentTermCalculationService/ExplainPaymentTermCalculation", outbound0);
        var reply0 = billingPaymentTermCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPaymentTermCalculation(command0.payload());

        List<ApprovePolicyResponse> results1 = request.getPremium().getLinesList().stream()
                .map(element -> ApprovePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPremium().getAudit().getEtag())
                                .build())
                        .addLimits(PolicyLimit.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approvePolicy(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? ApprovePolicyResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallPremium", request.getPremium().getPremiumId());
        return RecallPremiumResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(request.getPremium().getBillingAddress())
                        .addLines(PremiumLine.newBuilder()
                                .setTax(PremiumLineTax.newBuilder()
                                        .setAmount(reply0.getPaymentTerm().getDiscount().getAmount())
                                        .build())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a premium.
     */
    public GetPremiumApprovalStatusResponse getPremiumApprovalStatus(GetPremiumApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPremiumApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "GetPremiumApprovalStatus");

        var outbound0 = BeanCopies.copy(ScheduleCoverageExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), ScheduleCoverageExpiryRequest.class);
        var reply0 = insuranceCoverageLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleCoverageExpiry(outbound0);

        var outbound1 = StreamUnderwritingMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamUnderwritingMetricsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCoverage().getAudit().getUpdatedAt())
                        .build())
                .setSince(reply0.getOutcome().getEffectiveAt())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getCoverage().getOwner().getEmail())
                        .build())
                .build();
        var stream1 = insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingMetrics(outbound1);
        var reply1 = StreamUnderwritingMetricsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        return GetPremiumApprovalStatusResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCoverage().getAudit().getCreatedBy())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .build();
    }

    /**
     * Comments a premium.
     */
    public CommentPremiumApprovalResponse commentPremiumApproval(CommentPremiumApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentPremiumApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = mapper.commentPremiumApprovalStage0(request);
        var reply0 = accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainAllocationDecision(outbound0);

        var outbound1 = SyncPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getPremium().getBillingAddress().getLocality())
                                .build())
                        .setCard(PremiumCardSettlement.newBuilder()
                                .setLast4(switch (request.getPremium().getSettlementCase()) {
                                    case CARD -> request.getPremium().getCard().getLast4();
                                    default -> "";
                                })
                                .setToken(switch (request.getPremium().getSettlementCase()) {
                                    case CARD -> request.getPremium().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .addLines(PremiumLine.newBuilder()
                                .setTax(PremiumLineTax.newBuilder()
                                        .setAmount(reply0.getAllocation().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncPremiums(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentPremiumApproval", request.getPremium().getPremiumId());
        return CommentPremiumApprovalResponse.newBuilder()
                .setOutcome(CommentPremiumApprovalResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(request.getPremium().getBillingAddress())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getAllocation().getOwner().getDisplayName())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
