package com.acme.finance.domains.suppliers;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.ExplainDepreciationDecisionRequest;
import com.acme.assets.v1.ExplainDepreciationDecisionResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAttachment;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.audit.v1.SyncAuditTrailsRequest;
import com.acme.audit.v1.SyncAuditTrailsResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.ListBankAccountsRequest;
import com.acme.banking.v1.ListBankAccountsResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastLine;
import com.acme.budgeting.v1.ForecastLineTax;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.NormalizeForecastRequest;
import com.acme.budgeting.v1.NormalizeForecastResponse;
import com.acme.budgeting.v1.ScoreForecastRequest;
import com.acme.budgeting.v1.ScoreForecastResponse;
import com.acme.collections.v1.ScoreWriteOffRequest;
import com.acme.collections.v1.ScoreWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.SuggestControlsRequest;
import com.acme.compliance.v1.SuggestControlsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.ledger.v1.CompareLedgerAccountVersionsRequest;
import com.acme.ledger.v1.CompareLedgerAccountVersionsResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.lending.v1.BrowseLoanApplicationsRequest;
import com.acme.lending.v1.BrowseLoanApplicationsResponse;
import com.acme.lending.v1.ExportLoanApplicationsRequest;
import com.acme.lending.v1.ExportLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.ResolveLoanApplicationsRequest;
import com.acme.lending.v1.ResolveLoanApplicationsResponse;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.ExplainSearchIndexDecisionRequest;
import com.acme.search.v1.ExplainSearchIndexDecisionResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexComponent;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.sponsorships.v1.FindSponsorsRequest;
import com.acme.sponsorships.v1.FindSponsorsResponse;
import com.acme.sponsorships.v1.SponsorQueryServiceGrpc;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionResponse;
import com.acme.suppliers.v1.GetSupplierRatingRulesRequest;
import com.acme.suppliers.v1.GetSupplierRatingRulesResponse;
import com.acme.suppliers.v1.LintSupplierRatingsRequest;
import com.acme.suppliers.v1.LintSupplierRatingsResponse;
import com.acme.suppliers.v1.NormalizeSupplierRatingRequest;
import com.acme.suppliers.v1.NormalizeSupplierRatingResponse;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
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
 * Checks supplier ratings against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class SupplierRatingValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierRatingValidationApplication.class);

    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking;

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking;

    private final ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceFutureStub notificationsNotificationServiceFuture;

    private final SuppliersRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final SponsorQueryServiceGrpc.SponsorQueryServiceFutureStub sponsorshipsSponsorQueryServiceFuture;

    public SupplierRatingValidationApplication(
            DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking,
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking,
            ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking,
            NotificationServiceGrpc.NotificationServiceFutureStub notificationsNotificationServiceFuture,
            SuppliersRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            SponsorQueryServiceGrpc.SponsorQueryServiceFutureStub sponsorshipsSponsorQueryServiceFuture) {
        this.assetsDepreciationValidationServiceBlocking = assetsDepreciationValidationServiceBlocking;
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingBankAccountServiceBlocking = bankingBankAccountServiceBlocking;
        this.budgetingForecastValidationServiceBlocking = budgetingForecastValidationServiceBlocking;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.complianceControlQueryServiceBlocking = complianceControlQueryServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.lendingLoanApplicationQueryServiceBlocking = lendingLoanApplicationQueryServiceBlocking;
        this.notificationsNotificationServiceFuture = notificationsNotificationServiceFuture;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.sponsorshipsSponsorQueryServiceFuture = sponsorshipsSponsorQueryServiceFuture;
    }

    /**
     * Validates a supplier rating.
     */
    public ValidateSupplierRatingResponse validateSupplierRating(ValidateSupplierRatingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateSupplierRating")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = FindSponsorsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSponsorsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRating().getIssuedAt())
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = sponsorshipsSponsorQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).findSponsors(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var results1 = new ArrayList<ScoreForecastResponse>();
        for (var element : request.getSupplierRating().getLinesList()) {
            ScoreForecastRequest itemRequest = ScoreForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .addLines(ForecastLine.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .setTax(ForecastLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(budgetingForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreForecast(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ScoreForecastResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = SuggestAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRating().getAudit().getCreatedAt())
                        .setOwnerId(reply1.getForecast().getCounterparty().getBudgetId())
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        final class Stage2Adapter {
            SuggestAuditEventsResponse fetch(SuggestAuditEventsRequest input) {
                return auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestAuditEvents(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = BatchCreateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateFixedAssetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateFixedAssetsResponse.getDefaultInstance());
        var input3 = assetsFixedAssetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFixedAssets(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSupplierRating().getAudit().getCreatedAt())
                                .setCreatedBy(request.getSupplierRating().getAudit().getCreatedBy())
                                .setEtag(request.getSupplierRating().getAudit().getEtag())
                                .setUpdatedAt(request.getSupplierRating().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateSupplierRating", request.getSupplierRating().getSupplierRatingId());
        return ValidateSupplierRatingResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setIssuedAt(request.getSupplierRating().getIssuedAt())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Previews a supplier rating.
     */
    public PreviewSupplierRatingResponse previewSupplierRating(PreviewSupplierRatingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewSupplierRating")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var futures0 = request.getSupplierRating().getLinesList().stream()
                .map(element -> NormalizeForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .addLines(ForecastLine.newBuilder()
                                .setDescription(element.getDescription())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> budgetingForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeForecast(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? NormalizeForecastResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RpcSupport.unpack(Any.pack(SubscribeJournalEntryUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getSupplierRating().getCounterparty().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build()), SubscribeJournalEntryUpdatesRequest.class);
        var reply1 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeJournalEntryUpdates(outbound1);

        var outbound2 = (ScoreWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getSupplierRating().getBillingAddress().getCountryCode())
                                .build())
                        .setCard(WriteOffCardSettlement.newBuilder()
                                .setHolderName(reply1.getJournalEntry().getCard().getHolderName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreWriteOff(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewSupplierRating", request.getSupplierRating().getSupplierRatingId());
        return PreviewSupplierRatingResponse.newBuilder()
                .setOutcome(PreviewSupplierRatingResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getJournalEntry().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply2.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .setNumber(request.getSupplierRating().getNumber())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Checks a supplier rating.
     */
    public CheckSupplierRatingEligibilityResponse checkSupplierRatingEligibility(CheckSupplierRatingEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckSupplierRatingEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = ExplainSearchIndexDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainSearchIndexDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(request.getSupplierRating().getBillingAddress().getLocality())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainSearchIndexDecision(outbound0), rpcExecutor);

        var outbound1 = SuggestControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestControlsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRating().getDueAt())
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> complianceControlQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestControls(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = RpcSupport.unpack(Any.pack(BrowseLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getSearchIndex().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getSearchIndex().getComponentsOrDefault("primary", SearchIndexComponent.getDefaultInstance()).getComponentId())
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build()), BrowseLoanApplicationsRequest.class);
        var reply2 = lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseLoanApplications(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckSupplierRatingEligibility", request.getSupplierRating().getSupplierRatingId());
        return CheckSupplierRatingEligibilityResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setStatus(request.getSupplierRating().getStatus())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Normalizes a supplier rating.
     */
    public NormalizeSupplierRatingResponse normalizeSupplierRating(NormalizeSupplierRatingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeSupplierRating")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var futures0 = request.getSupplierRating().getAttachmentsList().stream()
                .map(element -> SyncAuditTrailsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .addAttachments(AuditTrailAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setFileName(element.getFileName())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SyncAuditTrailsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncAuditTrails(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? SyncAuditTrailsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ResolveLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var adapter1 = new Function<ResolveLoanApplicationsRequest, ResolveLoanApplicationsResponse>() {
            @Override
            public ResolveLoanApplicationsResponse apply(ResolveLoanApplicationsRequest input) {
                return lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveLoanApplications(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeSupplierRating", request.getSupplierRating().getSupplierRatingId());
        return NormalizeSupplierRatingResponse.newBuilder()
                .setOutcome(NormalizeSupplierRatingResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .putAllMetadata(request.getSupplierRating().getMetadataMap())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Explains a supplier rating.
     */
    public ExplainSupplierRatingDecisionResponse explainSupplierRatingDecision(ExplainSupplierRatingDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainSupplierRatingDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = ListBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBankAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRating().getAudit().getCreatedAt())
                        .setOwnerId(request.getSupplierRating().getSupplierRatingId())
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> bankingBankAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listBankAccounts(value));
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getCounterparty().getCustomerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainSupplierRatingDecision", request.getSupplierRating().getCounterparty().getCustomerId());
        return ExplainSupplierRatingDecisionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setNumber(request.getSupplierRating().getNumber())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Scores a supplier rating.
     */
    public ScoreSupplierRatingResponse scoreSupplierRating(ScoreSupplierRatingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreSupplierRating")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var results0 = new ArrayList<ListAuditEventVersionsResponse>();
        for (var element : request.getSupplierRating().getLinesList()) {
            ListAuditEventVersionsRequest itemRequest = ListAuditEventVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventVersionsRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
            results0.add(auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEventVersions(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ListAuditEventVersionsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ExplainDepreciationDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(request.getSupplierRating().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsDepreciationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainDepreciationDecision(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = EstimatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimatePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getDepreciation().getLocation().getPostalCode())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getDepreciation().getAudit().getEtag())
                                .build())
                        .setCard(PolicyCardFunding.newBuilder()
                                .setLast4(switch (request.getSupplierRating().getSettlementCase()) {
                                    case CARD -> request.getSupplierRating().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getSupplierRating().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePolicy(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreSupplierRating", request.getSupplierRating().getSupplierRatingId());
        return ScoreSupplierRatingResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCounterparty(request.getSupplierRating().getCounterparty())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .setTotal(reply2.getBreakdown().getTotal())
                        .build())
                .build();
    }

    /**
     * Lints a supplier rating.
     */
    public LintSupplierRatingsResponse lintSupplierRatings(LintSupplierRatingsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintSupplierRatings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = SearchNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRating().getAudit().getUpdatedAt())
                        .putAllLabels(request.getSupplierRating().getMetadataMap())
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = notificationsNotificationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).searchNotifications(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = ExportLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRating().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportLoanApplications(outbound1);
        var reply1 = ExportLoanApplicationsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var futures2 = request.getSupplierRating().getLinesList().stream()
                .map(element -> BrowseSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSearchIndexesRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSearchIndexes(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? BrowseSearchIndexesResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintSupplierRatings", request.getSupplierRating().getSupplierRatingId());
        return LintSupplierRatingsResponse.newBuilder()
                .setOutcome(LintSupplierRatingsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setNumber(request.getSupplierRating().getNumber())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Returns a supplier rating.
     */
    public GetSupplierRatingRulesResponse getSupplierRatingRules(GetSupplierRatingRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetSupplierRatingRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierRating", request.getSupplierRatingId(), "GetSupplierRatingRules");

        var outbound0 = CompareLedgerAccountVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareLedgerAccountVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getSupplierRatingId())
                        .build())
                .build();
        var reply0 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareLedgerAccountVersions(outbound0);
        return GetSupplierRatingRulesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }
}
