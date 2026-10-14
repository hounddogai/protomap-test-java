package com.acme.finance.domains.insurance;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.CreateFixedAssetRequest;
import com.acme.assets.v1.CreateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.NormalizeAssetTransferRequest;
import com.acme.assets.v1.NormalizeAssetTransferResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermBlockAction;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.QuotePaymentTermRequest;
import com.acme.billing.v1.QuotePaymentTermResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastLine;
import com.acme.budgeting.v1.ForecastLineTax;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.CountHouseholdsRequest;
import com.acme.customers.v1.CountHouseholdsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.QuoteConsentRecordRequest;
import com.acme.customers.v1.QuoteConsentRecordResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvanceClaimWorkflowRequest;
import com.acme.insurance.v1.AdvanceClaimWorkflowResponse;
import com.acme.insurance.v1.AdvanceCoverageWorkflowRequest;
import com.acme.insurance.v1.AdvanceCoverageWorkflowResponse;
import com.acme.insurance.v1.CancelCoverageWorkflowRequest;
import com.acme.insurance.v1.CancelCoverageWorkflowResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.CommentPremiumApprovalRequest;
import com.acme.insurance.v1.CommentPremiumApprovalResponse;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.InspectCoverageWorkflowRequest;
import com.acme.insurance.v1.InspectCoverageWorkflowResponse;
import com.acme.insurance.v1.ListCoverageWorkflowsRequest;
import com.acme.insurance.v1.ListCoverageWorkflowsResponse;
import com.acme.insurance.v1.ListPendingClaimsRequest;
import com.acme.insurance.v1.ListPendingClaimsResponse;
import com.acme.insurance.v1.PauseCoverageWorkflowRequest;
import com.acme.insurance.v1.PauseCoverageWorkflowResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumLineTax;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.insurance.v1.ResumeCoverageWorkflowRequest;
import com.acme.insurance.v1.ResumeCoverageWorkflowResponse;
import com.acme.insurance.v1.RetryCoverageWorkflowRequest;
import com.acme.insurance.v1.RetryCoverageWorkflowResponse;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.insurance.v1.SignalCoverageWorkflowRequest;
import com.acme.insurance.v1.SignalCoverageWorkflowResponse;
import com.acme.insurance.v1.StartCoverageWorkflowRequest;
import com.acme.insurance.v1.StartCoverageWorkflowResponse;
import com.acme.insurance.v1.SubmitPolicyRequest;
import com.acme.insurance.v1.SubmitPolicyResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityRequest;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.acme.procurement.v1.GoodsReceiptLineTax;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.subscriptions.v1.ListSubscriptionsRequest;
import com.acme.subscriptions.v1.ListSubscriptionsResponse;
import com.acme.subscriptions.v1.SubscriptionServiceGrpc;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.UpdateSupplierRequest;
import com.acme.suppliers.v1.UpdateSupplierResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a coverage from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class CoverageWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(CoverageWorkflowApplication.class);

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking;

    private final ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking;

    private final CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking;

    private final PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final CoverageWorkflowRequestMapper mapper;

    private final GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final SubscriptionServiceGrpc.SubscriptionServiceBlockingStub subscriptionsSubscriptionServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public CoverageWorkflowApplication(
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            ChannelRegistry channels,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            ApplicationEventPublisher events,
            ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking,
            ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking,
            CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking,
            PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            CoverageWorkflowRequestMapper mapper,
            GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SubscriptionServiceGrpc.SubscriptionServiceBlockingStub subscriptionsSubscriptionServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.billingPaymentTermCalculationServiceBlocking = billingPaymentTermCalculationServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.channels = channels;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.events = events;
        this.insuranceClaimApprovalServiceBlocking = insuranceClaimApprovalServiceBlocking;
        this.insuranceClaimWorkflowServiceBlocking = insuranceClaimWorkflowServiceBlocking;
        this.insuranceCoverageLifecycleServiceBlocking = insuranceCoverageLifecycleServiceBlocking;
        this.insurancePremiumApprovalServiceBlocking = insurancePremiumApprovalServiceBlocking;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.mapper = mapper;
        this.procurementGoodsReceiptValidationServiceBlocking = procurementGoodsReceiptValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.subscriptionsSubscriptionServiceBlocking = subscriptionsSubscriptionServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    public record CancelCoverageWorkflowStage0Requested(QuoteConsentRecordRequest request, CompletableFuture<QuoteConsentRecordResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a coverage.
     */
    public StartCoverageWorkflowResponse startCoverageWorkflow(StartCoverageWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartCoverageWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = JsonCodec.roundTrip(ListPendingClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingClaimsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCoverage().getAudit().getUpdatedAt())
                        .putAllLabels(request.getCoverage().getParametersMap())
                        .setOwnerId(request.getCoverage().getOwner().getUserId())
                        .build())
                .build(), ListPendingClaimsRequest.class);
        var reply0 = insuranceClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingClaims(outbound0);

        var outbound1 = ScheduleCoverageExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setDiscount(CoverageDiscountAction.newBuilder()
                                .setAmount(request.getCoverage().getDiscount().getAmount())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<ScheduleCoverageExpiryRequest, ScheduleCoverageExpiryResponse> port1 = RpcPort.adapt(insuranceCoverageLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleCoverageExpiry);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = SubmitPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCoverage().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.insurance.v1.PolicyApprovalService");
        var serviceName2 = "acme.insurance.v1" + "." + "PolicyApprovalService";
        var methodName2 = "Sub" + "mitPolicy";
        var descriptor2 = MethodDescriptor.<SubmitPolicyRequest, SubmitPolicyResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(SubmitPolicyRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SubmitPolicyResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(AdvanceClaimWorkflowRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply2.getPolicy().getContact().getPhone())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(AdvanceClaimWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(reply2.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build()), AdvanceClaimWorkflowRequest.class);
        var reply3 = insuranceClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceClaimWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartCoverageWorkflow", request.getCoverage().getCoverageId());
        return StartCoverageWorkflowResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply3.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setDescription(request.getCoverage().getDescription())
                        .build())
                .setOutcome(StartCoverageWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Advances a coverage.
     */
    public AdvanceCoverageWorkflowResponse advanceCoverageWorkflow(AdvanceCoverageWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceCoverageWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = CountPremiumTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPremiumTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCoverage().getAudit().getCreatedAt())
                        .setOwnerId(request.getCoverage().getOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countPremiumTotals(outbound0), rpcExecutor);

        var outbound1 = CloneLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLedgerAccount(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = CheckGoodsReceiptEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCoverage().getAudit().getEtag())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply1.getLedgerAccount().getHolder().getNumber())
                                .build())
                        .addLines(GoodsReceiptLine.newBuilder()
                                .setTax(GoodsReceiptLineTax.newBuilder()
                                        .setAmount(request.getCoverage().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            CheckGoodsReceiptEligibilityResponse fetch(CheckGoodsReceiptEligibilityRequest input) {
                return procurementGoodsReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkGoodsReceiptEligibility(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceCoverageWorkflow", request.getCoverage().getCoverageId());
        return AdvanceCoverageWorkflowResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getGoodsReceipt().getCounterparty().getDisplayName())
                                .build())
                        .setStatus(request.getCoverage().getStatus())
                        .build())
                .setOutcome(AdvanceCoverageWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Cancels a coverage.
     */
    public CancelCoverageWorkflowResponse cancelCoverageWorkflow(CancelCoverageWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelCoverageWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = QuoteConsentRecordRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getCoverage().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteConsentRecordRequest.Details.newBuilder()
                        .setBudget(request.getCoverage().getDiscount().getAmount())
                        .build())
                .setReason(request.getCoverage().getBlock().getReason())
                .build();
        var completion0 = new CompletableFuture<QuoteConsentRecordResponse>();
        events.publishEvent(new CancelCoverageWorkflowStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = BeanCopies.copy(NormalizeAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), NormalizeAssetTransferRequest.class);
        var reply1 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAssetTransfer(outbound1);

        var outbound2 = mapper.cancelCoverageWorkflowStage2(request, reply1);
        var reply2 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePremiumConflict(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelCoverageWorkflow", request.getCoverage().getCoverageId());
        return CancelCoverageWorkflowResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setPriority(request.getCoverage().getPriority())
                        .build())
                .setOutcome(CancelCoverageWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Retrys a coverage.
     */
    public RetryCoverageWorkflowResponse retryCoverageWorkflow(RetryCoverageWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryCoverageWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = QuotePaymentTermRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setBlock(PaymentTermBlockAction.newBuilder()
                                .setNotifyEmail(switch (request.getCoverage().getActionCase()) {
                                    case BLOCK -> request.getCoverage().getBlock().getNotifyEmail();
                                    default -> "";
                                })
                                .setReason(switch (request.getCoverage().getActionCase()) {
                                    case BLOCK -> request.getCoverage().getBlock().getReason();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = billingPaymentTermCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePaymentTerm(outbound0);

        var outbound1 = ListSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPaymentTerm().getAudit().getCreatedAt())
                        .build())
                .build();
        var reply1 = subscriptionsSubscriptionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSubscriptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryCoverageWorkflow", request.getCoverage().getCoverageId());
        return RetryCoverageWorkflowResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPaymentTerm().getAudit().getUpdatedAt())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setOwner(request.getCoverage().getOwner())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Pauses a coverage.
     */
    public PauseCoverageWorkflowResponse pauseCoverageWorkflow(PauseCoverageWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseCoverageWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = CountHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCoverage().getAudit().getCreatedAt())
                        .putAllLabels(request.getCoverage().getParametersMap())
                        .setOwnerId(request.getCoverage().getOwner().getUserId())
                        .build())
                .build();
        var adapter0 = new Function<CountHouseholdsRequest, CountHouseholdsResponse>() {
            @Override
            public CountHouseholdsResponse apply(CountHouseholdsRequest input) {
                return customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countHouseholds(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = BeanCopies.copy(QuoteForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCoverage().getAudit().getEtag())
                                .setUpdatedAt(request.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .addLines(ForecastLine.newBuilder()
                                .setTax(ForecastLineTax.newBuilder()
                                        .setAmount(request.getCoverage().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build(), QuoteForecastRequest.class);
        var reply1 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteForecast(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseCoverageWorkflow", request.getCoverage().getCoverageId());
        return PauseCoverageWorkflowResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setName(request.getCoverage().getName())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes a coverage.
     */
    public ResumeCoverageWorkflowResponse resumeCoverageWorkflow(ResumeCoverageWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeCoverageWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = UpdateSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCoverage().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.suppliers.v1.SupplierService/UpdateSupplier", outbound0);
        var reply0 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSupplier(command0.payload());

        var outbound1 = BeanCopies.copy(ReconcileUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileUnderwritingsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getSupplier().getRelationshipManager().getDisplayName())
                                .setEmail(reply0.getSupplier().getRelationshipManager().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getSupplier().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build(), ReconcileUnderwritingsRequest.class);
        var reply1 = insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileUnderwritings(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeCoverageWorkflow", request.getCoverage().getCoverageId());
        return ResumeCoverageWorkflowResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setEffective(request.getCoverage().getEffective())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getUnderwriting().getAssignee().getEmail())
                                .setUserId(reply1.getUnderwriting().getAssignee().getUserId())
                                .build())
                        .build())
                .setOutcome(ResumeCoverageWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a coverage.
     */
    public InspectCoverageWorkflowResponse inspectCoverageWorkflow(InspectCoverageWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectCoverageWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "InspectCoverageWorkflow");

        var outbound0 = ShareFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareFiscalPeriodExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<ShareFiscalPeriodExportRequest, ShareFiscalPeriodExportResponse> port0 = RpcPort.adapt(ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::shareFiscalPeriodExport);
        var reply0 = port0.exchange(outbound0);
        return InspectCoverageWorkflowResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getFiscalPeriod().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists coverages a page at a time.
     */
    public ListCoverageWorkflowsResponse listCoverageWorkflows(ListCoverageWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListCoverageWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getContext().getTraceId(), "ListCoverageWorkflows");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListCoverageWorkflows", request.getPage().getPageToken());
        return ListCoverageWorkflowsResponse.getDefaultInstance();
    }

    /**
     * Signals a coverage.
     */
    public SignalCoverageWorkflowResponse signalCoverageWorkflow(SignalCoverageWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalCoverageWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = CreateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCoverage().getAudit().getCreatedAt())
                                .setCreatedBy(request.getCoverage().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getCoverage().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createFixedAsset(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = mapper.signalCoverageWorkflowStage1(request, reply0);
        var reply1 = insurancePremiumApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPremiumApproval(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalCoverageWorkflow", request.getCoverage().getCoverageId());
        return SignalCoverageWorkflowResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPremium().getAudit().getCreatedBy())
                                .setEtag(reply0.getFixedAsset().getAudit().getEtag())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setPriority(request.getCoverage().getPriority())
                        .build())
                .setOutcome(SignalCoverageWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }
}
