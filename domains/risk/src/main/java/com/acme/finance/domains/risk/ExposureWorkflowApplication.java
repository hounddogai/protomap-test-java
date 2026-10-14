package com.acme.finance.domains.risk;

import com.acme.banking.v1.CommentStatementApprovalRequest;
import com.acme.banking.v1.CommentStatementApprovalResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.RecallMandateRequest;
import com.acme.banking.v1.RecallMandateResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.EstimateConsentRecordRequest;
import com.acme.customers.v1.EstimateConsentRecordResponse;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuoteHouseholdRequest;
import com.acme.customers.v1.QuoteHouseholdResponse;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.RetryClaimWorkflowRequest;
import com.acme.insurance.v1.RetryClaimWorkflowResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchBankSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ScorePostingBatchRequest;
import com.acme.ledger.v1.ScorePostingBatchResponse;
import com.acme.risk.v1.AdvanceExposureWorkflowRequest;
import com.acme.risk.v1.AdvanceExposureWorkflowResponse;
import com.acme.risk.v1.CancelExposureWorkflowRequest;
import com.acme.risk.v1.CancelExposureWorkflowResponse;
import com.acme.risk.v1.EstimateExposureRequest;
import com.acme.risk.v1.EstimateExposureResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureBankFunding;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.InspectExposureWorkflowRequest;
import com.acme.risk.v1.InspectExposureWorkflowResponse;
import com.acme.risk.v1.PauseExposureWorkflowRequest;
import com.acme.risk.v1.PauseExposureWorkflowResponse;
import com.acme.risk.v1.RejectRiskRegisterRequest;
import com.acme.risk.v1.RejectRiskRegisterResponse;
import com.acme.risk.v1.ResumeExposureWorkflowRequest;
import com.acme.risk.v1.ResumeExposureWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.SignalExposureWorkflowRequest;
import com.acme.risk.v1.SignalExposureWorkflowResponse;
import com.acme.risk.v1.StartExposureWorkflowRequest;
import com.acme.risk.v1.StartExposureWorkflowResponse;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.sellers.v1.SellerQueryServiceGrpc;
import com.acme.sellers.v1.SuggestSellersRequest;
import com.acme.sellers.v1.SuggestSellersResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes an exposure from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ExposureWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ExposureWorkflowApplication.class);

    private final MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking;

    private final StatementApprovalServiceGrpc.StatementApprovalServiceBlockingStub bankingStatementApprovalServiceBlocking;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final RiskRepository repository;

    private final ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking;

    private final RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceFutureStub searchSearchIndexQueryServiceFuture;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final SellerQueryServiceGrpc.SellerQueryServiceBlockingStub sellersSellerQueryServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public ExposureWorkflowApplication(
            MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking,
            StatementApprovalServiceGrpc.StatementApprovalServiceBlockingStub bankingStatementApprovalServiceBlocking,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            RiskRepository repository,
            ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking,
            RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceFutureStub searchSearchIndexQueryServiceFuture,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            SellerQueryServiceGrpc.SellerQueryServiceBlockingStub sellersSellerQueryServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.bankingMandateApprovalServiceBlocking = bankingMandateApprovalServiceBlocking;
        this.bankingStatementApprovalServiceBlocking = bankingStatementApprovalServiceBlocking;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.insuranceClaimWorkflowServiceBlocking = insuranceClaimWorkflowServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.repository = repository;
        this.riskExposureCalculationServiceBlocking = riskExposureCalculationServiceBlocking;
        this.riskRiskRegisterApprovalServiceBlocking = riskRiskRegisterApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceFuture = searchSearchIndexQueryServiceFuture;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.sellersSellerQueryServiceBlocking = sellersSellerQueryServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    /**
     * Starts an exposure.
     */
    public StartExposureWorkflowResponse startExposureWorkflow(StartExposureWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartExposureWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = WatchSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotify().getEmail())
                        .setName(request.getExposure().getContact().getName())
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build();
        var stream0 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchSuppliers(outbound0);
        var reply0 = WatchSuppliersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var builder1 = EstimateConsentRecordRequest.newBuilder();
        builder1.putAllAnnotations(request.getAnnotationsMap());
        builder1.setConsentRecord(ConsentRecord.newBuilder()
                .setDetail(Any.pack(request.getContext()))
                .setSubject(HouseholdRef.newBuilder()
                        .setDisplayName(request.getExposure().getHolder().getDisplayName())
                        .build())
                .build());
        builder1.setContext(request.getContext());
        builder1.setDetails(EstimateConsentRecordRequest.Details.newBuilder()
                .setCurrencyCode(request.getExposure().getCurrencyCode())
                .build());
        builder1.setReason(request.getReason());
        var outbound1 = builder1.build();
        var reply1 = customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateConsentRecord(outbound1);

        var outbound2 = CommentStatementApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExposure().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> bankingStatementApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentStatementApproval(value));

        var outbound3 = RpcSupport.unpack(Any.pack(PreviewSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSearchIndexRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExposure().getContact().getEmail())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build()), PreviewSearchIndexRequest.class);
        var reply3 = searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSearchIndex(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartExposureWorkflow", request.getExposure().getExposureId());
        return StartExposureWorkflowResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setExposureId(request.getExposureId())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(reply1.getConsentRecord().getSubject().getDisplayName())
                                .setEmail(reply3.getSearchIndex().getOwner().getEmail())
                                .build())
                        .setOpenedAt(request.getExposure().getOpenedAt())
                        .build())
                .setOutcome(StartExposureWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances an exposure.
     */
    public AdvanceExposureWorkflowResponse advanceExposureWorkflow(AdvanceExposureWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceExposureWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = RpcSupport.unpack(Any.pack(ScorePostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExposure().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getExposure().getAudit().getUpdatedBy())
                                .build())
                        .setBank(PostingBatchBankSettlement.newBuilder()
                                .setIban(request.getExposure().getBank().getIban())
                                .build())
                        .build())
                .build()), ScorePostingBatchRequest.class);
        var reply0 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePostingBatch(outbound0);

        var outbound1 = EstimateExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setBank(ExposureBankFunding.newBuilder()
                                .setIban(switch (request.getExposure().getFundingCase()) {
                                    case BANK -> request.getExposure().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExposure().getHolder().getEmail())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getPostingBatch().getCounterparty().getDisplayName())
                                .setEmail(request.getExposure().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = riskExposureCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateExposure(outbound1);

        var outbound2 = JsonCodec.roundTrip(QuoteHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(reply1.getExposure().getAudit().getCreatedBy())
                                                .setUpdatedAt(request.getExposure().getAudit().getUpdatedAt())
                                                .setUpdatedBy(reply1.getExposure().getAudit().getUpdatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply1.getExposure().getHolder().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), QuoteHouseholdRequest.class);
        var reply2 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteHousehold(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceExposureWorkflow", request.getExposure().getExposureId());
        return AdvanceExposureWorkflowResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAvailable(request.getExposure().getAvailable())
                        .setExposureId(request.getExposureId())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(reply2.getHousehold().getManager().getDisplayName())
                                .build())
                        .build())
                .setOutcome(AdvanceExposureWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Cancels an exposure.
     */
    public CancelExposureWorkflowResponse cancelExposureWorkflow(CancelExposureWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelExposureWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var results0 = new ArrayList<RecallMandateResponse>();
        for (var element : request.getExposure().getLimitsList()) {
            RecallMandateRequest itemRequest = RecallMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .addLines(MandateLine.newBuilder()
                                .setTax(MandateLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
            results0.add(bankingMandateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallMandate(itemRequest));
        }
        var reply0 = results0.isEmpty() ? RecallMandateResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelExposureWorkflow", request.getExposure().getExposureId());
        return CancelExposureWorkflowResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setContact(request.getExposure().getContact())
                        .setExposureId(request.getExposureId())
                        .build())
                .build();
    }

    /**
     * Pauses an exposure.
     */
    public PauseExposureWorkflowResponse pauseExposureWorkflow(PauseExposureWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseExposureWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExposure().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getExposure().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getExposure().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getExposure().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePreference(outbound0), rpcExecutor);

        var outbound1 = SuggestSellersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSellersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExposure().getAudit().getUpdatedAt())
                        .putAllLabels(request.getExposure().getLabelsMap())
                        .setOwnerId(request.getExposure().getExternalAccountId())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> sellersSellerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestSellers(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseExposureWorkflow", request.getExposure().getExposureId());
        return PauseExposureWorkflowResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPreference().getOwner().getEmail())
                                .build())
                        .setExposureId(request.getExposureId())
                        .setHolder(request.getExposure().getHolder())
                        .build())
                .setOutcome(PauseExposureWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Resumes an exposure.
     */
    public ResumeExposureWorkflowResponse resumeExposureWorkflow(ResumeExposureWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeExposureWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = RejectRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getExposure().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = riskRiskRegisterApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectRiskRegister(outbound0);

        var outbound1 = ListPremiumConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPremiumConflictsRequest.Filter.newBuilder()
                        .putAllLabels(request.getExposure().getLabelsMap())
                        .setRegion(reply0.getRiskRegister().getLocation().getRegion())
                        .build())
                .build();
        var reply1 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPremiumConflicts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeExposureWorkflow", request.getExposure().getExposureId());
        return ResumeExposureWorkflowResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setExposureId(request.getExposureId())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getRiskRegister().getManager().getDisplayName())
                                .build())
                        .putAllLabels(request.getExposure().getLabelsMap())
                        .build())
                .setOutcome(ResumeExposureWorkflowResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an exposure.
     */
    public InspectExposureWorkflowResponse inspectExposureWorkflow(InspectExposureWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectExposureWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Exposure", request.getExposureId(), "InspectExposureWorkflow");

        var outbound0 = BeanCopies.copy(EstimateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build(), EstimateHouseholdRequest.class);
        var reply0 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateHousehold(outbound0);

        var outbound1 = BrowseSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSearchIndexesRequest.Filter.newBuilder()
                        .setOwnerId(reply0.getHousehold().getManager().getUserId())
                        .setRegion(reply0.getHousehold().getLocation().getRegion())
                        .build())
                .build();
        var pending1 = searchSearchIndexQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).browseSearchIndexes(outbound1);
        var reply1 = RpcSupport.await(pending1);
        return InspectExposureWorkflowResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getHousehold().getAudit().getUpdatedBy())
                                .build())
                        .setExposureId(request.getExposureId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Signals an exposure.
     */
    public SignalExposureWorkflowResponse signalExposureWorkflow(SignalExposureWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalExposureWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExposure(request.getContext().getTenantId(), request.getExposure());

        var outbound0 = RetryClaimWorkflowRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getExposure().getAudit().getEtag())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RetryClaimWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getExposure().getContact().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            RetryClaimWorkflowResponse fetch(RetryClaimWorkflowRequest input) {
                return insuranceClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryClaimWorkflow(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ReconcileUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileUnderwritingsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getClaim().getAssignee().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getClaim().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileUnderwritings(value));
        log.atDebug().addKeyValue("auditValue", request.getExposure().getExposureId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalExposureWorkflow", request.getExposure().getExposureId());
        return SignalExposureWorkflowResponse.newBuilder()
                .setExposure(Exposure.newBuilder()
                        .setBalance(request.getExposure().getBalance())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getClaim().getAssignee().getEmail())
                                .build())
                        .setExposureId(request.getExposureId())
                        .setHolder(SupplierRef.newBuilder()
                                .setEmail(reply1.getUnderwriting().getRequester().getEmail())
                                .build())
                        .build())
                .build();
    }
}
