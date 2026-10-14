package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.AccrualQueryServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.BrowseAccrualsRequest;
import com.acme.accounting.v1.BrowseAccrualsResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.assets.v1.BatchGetFixedAssetsRequest;
import com.acme.assets.v1.BatchGetFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.UpdateAuditEventRequest;
import com.acme.audit.v1.UpdateAuditEventResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivatePolicyRequest;
import com.acme.insurance.v1.ActivatePolicyResponse;
import com.acme.insurance.v1.ApprovePolicyRequest;
import com.acme.insurance.v1.ApprovePolicyResponse;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.EscalatePolicyRequest;
import com.acme.insurance.v1.EscalatePolicyResponse;
import com.acme.insurance.v1.GeneratePremiumReportRequest;
import com.acme.insurance.v1.GeneratePremiumReportResponse;
import com.acme.insurance.v1.GenerateUnderwritingReportRequest;
import com.acme.insurance.v1.GenerateUnderwritingReportResponse;
import com.acme.insurance.v1.GetPolicyApprovalStatusRequest;
import com.acme.insurance.v1.GetPolicyApprovalStatusResponse;
import com.acme.insurance.v1.ListPendingPoliciesRequest;
import com.acme.insurance.v1.ListPendingPoliciesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumBankSettlement;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.insurance.v1.RejectPolicyRequest;
import com.acme.insurance.v1.RejectPolicyResponse;
import com.acme.insurance.v1.SubmitPolicyRequest;
import com.acme.insurance.v1.SubmitPolicyResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.procurement.v1.PreviewRfqRequest;
import com.acme.procurement.v1.PreviewRfqResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.ValidateRfqRequest;
import com.acme.procurement.v1.ValidateRfqResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.suppliers.v1.SummarizeSuppliersRequest;
import com.acme.suppliers.v1.SummarizeSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes policies through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class PolicyApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(PolicyApprovalApplication.class);

    private final AccrualQueryServiceGrpc.AccrualQueryServiceBlockingStub accountingAccrualQueryServiceBlocking;

    private final AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking;

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceFutureStub auditRetentionPolicyExportServiceFuture;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking;

    private final InsuranceRepository repository;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync;

    private final Executor rpcExecutor;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    public PolicyApprovalApplication(
            AccrualQueryServiceGrpc.AccrualQueryServiceBlockingStub accountingAccrualQueryServiceBlocking,
            AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking,
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceFutureStub auditRetentionPolicyExportServiceFuture,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            ApplicationEventPublisher events,
            PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking,
            InsuranceRepository repository,
            RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking) {
        this.accountingAccrualQueryServiceBlocking = accountingAccrualQueryServiceBlocking;
        this.accountingAllocationValidationServiceBlocking = accountingAllocationValidationServiceBlocking;
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.auditRetentionPolicyExportServiceFuture = auditRetentionPolicyExportServiceFuture;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.events = events;
        this.insurancePolicyLifecycleServiceBlocking = insurancePolicyLifecycleServiceBlocking;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.procurementRfqValidationServiceBlocking = procurementRfqValidationServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterServiceAsync = riskRiskRegisterServiceAsync;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
    }

    public record RecallPolicyStage0Requested(PreviewRfqRequest request, CompletableFuture<PreviewRfqResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a policy.
     */
    public SubmitPolicyResponse submitPolicy(SubmitPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = BatchGetFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        Function<BatchGetFixedAssetsRequest, BatchGetFixedAssetsResponse> operation0 = assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::batchGetFixedAssets;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = BatchCreateRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateRiskRegistersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getPolicy().getContact())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateRiskRegistersResponse.getDefaultInstance());
        var input1 = riskRiskRegisterServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateRiskRegisters(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPolicy().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPolicy().getAudit().getCreatedBy())
                                .setEtag(request.getPolicy().getAudit().getEtag())
                                .setUpdatedAt(request.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitPolicy", request.getPolicy().getPolicyId());
        return SubmitPolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setContact(request.getPolicy().getContact())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Approves a policy.
     */
    public ApprovePolicyResponse approvePolicy(ApprovePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApprovePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = PreviewAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getPolicy().getHolder().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAllocation(outbound0), rpcExecutor);

        var outbound1 = ResolveCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCustomers(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApprovePolicy", request.getPolicy().getPolicyId());
        return ApprovePolicyResponse.newBuilder()
                .setOutcome(ApprovePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(request.getPolicy().getContact())
                        .addLimits(PolicyLimit.newBuilder()
                                .setAmount(reply0.getAllocation().getDiscount().getAmount())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Rejects a policy.
     */
    public RejectPolicyResponse rejectPolicy(RejectPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = ShareRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareRetentionPolicyExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setPhone(request.getPolicy().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var pending0 = auditRetentionPolicyExportServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).shareRetentionPolicyExport(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = UpdateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateAuditEvent(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectPolicy", request.getPolicy().getPolicyId());
        return RejectPolicyResponse.newBuilder()
                .setOutcome(RejectPolicyResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(request.getPolicy().getContact())
                        .setHolder(CoverageRef.newBuilder()
                                .setVersion(reply0.getRetentionPolicy().getVersion())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getAuditEvent().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Escalates a policy.
     */
    public EscalatePolicyResponse escalatePolicy(EscalatePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalatePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = ActivatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ActivatePolicyRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setPolicyId(request.getPolicyId())
                .build();
        var reply0 = insurancePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).activatePolicy(outbound0);

        var outbound1 = SummarizeSuppliersRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeSuppliersRequest.Details.newBuilder()
                        .putAllFilters(request.getPolicy().getLabelsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply1 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeSuppliers(outbound1);

        var outbound2 = QuoteForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPolicy().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setBank(ForecastBankSettlement.newBuilder()
                                .setBic(switch (reply0.getPolicy().getFundingCase()) {
                                    case BANK -> reply0.getPolicy().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteForecast(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalatePolicy", request.getPolicy().getPolicyId());
        return EscalatePolicyResponse.newBuilder()
                .setOutcome(EscalatePolicyResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .setCurrencyCode(request.getPolicy().getCurrencyCode())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists policies a page at a time.
     */
    public ListPendingPoliciesResponse listPendingPolicies(ListPendingPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "ListPendingPolicies");

        var outbound0 = RecalculatePremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        Function<RecalculatePremiumsRequest, RecalculatePremiumsResponse> operation0 = insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculatePremiums;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingPolicies", request.getPage().getPageToken());
        return ListPendingPoliciesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalls a policy.
     */
    public RecallPolicyResponse recallPolicy(RecallPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = PreviewRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewRfqRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getPolicy().getContact().getPhone())
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<PreviewRfqResponse>();
        events.publishEvent(new RecallPolicyStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = PushPremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushPremiumRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setBank(PremiumBankSettlement.newBuilder()
                                .setIban(request.getPolicy().getBank().getIban())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getRfq().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.insurance.v1.PremiumSyncService/PushPremium", outbound1);
        var reply1 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushPremium(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallPolicy", request.getPolicy().getPolicyId());
        return RecallPolicyResponse.newBuilder()
                .setOutcome(RecallPolicyResponse.Outcome.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPremium().getAudit().getEtag())
                                .build())
                        .setHolder(request.getPolicy().getHolder())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a policy.
     */
    public GetPolicyApprovalStatusResponse getPolicyApprovalStatus(GetPolicyApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPolicyApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "GetPolicyApprovalStatus");

        var outbound0 = BrowseLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getPolicyId())
                        .build())
                .build();
        final class Stage0Adapter {
            BrowseLedgerAccountsResponse fetch(BrowseLedgerAccountsRequest input) {
                return ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseLedgerAccounts(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return GetPolicyApprovalStatusResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Comments a policy.
     */
    public CommentPolicyApprovalResponse commentPolicyApproval(CommentPolicyApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentPolicyApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = ValidateRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getPolicy().getHolder().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getPolicy().getContact().getName())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.procurement.v1.RfqValidationService/ValidateRfq", outbound0);
        var reply0 = procurementRfqValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRfq(command0.payload());

        var outbound1 = GeneratePremiumReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GeneratePremiumReportRequest.Details.newBuilder()
                        .putAllFilters(request.getPolicy().getLabelsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generatePremiumReport(value)).orElseThrow();

        var outbound2 = BrowseAccrualsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAccrualsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getRfq().getAudit().getUpdatedAt())
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .setOwnerId(switch (request.getPolicy().getFundingCase()) {
                            case EXTERNAL_ACCOUNT_ID -> request.getPolicy().getExternalAccountId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply2 = accountingAccrualQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAccruals(outbound2);

        var outbound3 = GenerateUnderwritingReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateUnderwritingReportRequest.Details.newBuilder()
                        .setPeriod(reply1.getPeriod())
                        .build())
                .setReason(request.getReason())
                .build();
        final class Stage3Adapter {
            GenerateUnderwritingReportResponse fetch(GenerateUnderwritingReportRequest input) {
                return insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateUnderwritingReport(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentPolicyApproval", request.getPolicy().getPolicyId());
        return CommentPolicyApprovalResponse.newBuilder()
                .setOutcome(CommentPolicyApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getRfq().getAudit().getUpdatedBy())
                                .build())
                        .setBalance(request.getPolicy().getBalance())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }
}
