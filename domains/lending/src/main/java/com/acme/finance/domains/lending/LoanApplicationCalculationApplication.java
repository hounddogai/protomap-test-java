package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.PushAuditTrailRequest;
import com.acme.audit.v1.PushAuditTrailResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseComment;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.UpdateCollectionCaseRequest;
import com.acme.collections.v1.UpdateCollectionCaseResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffBankSettlement;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.content.v1.ArticleQueryServiceGrpc;
import com.acme.content.v1.ResolveArticlesRequest;
import com.acme.content.v1.ResolveArticlesResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.BrowseFeatureFlagsRequest;
import com.acme.featureflags.v1.BrowseFeatureFlagsResponse;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityRequest;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagDiscountAction;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.LintFeatureFlagsRequest;
import com.acme.featureflags.v1.LintFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.NormalizeClaimRequest;
import com.acme.insurance.v1.NormalizeClaimResponse;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSla;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingRequest;
import com.acme.insurance.v1.ValidateUnderwritingResponse;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchAdminServiceGrpc;
import com.acme.ledger.v1.RepairPostingBatchRequest;
import com.acme.ledger.v1.RepairPostingBatchResponse;
import com.acme.lending.v1.CalculateLoanApplicationRequest;
import com.acme.lending.v1.CalculateLoanApplicationResponse;
import com.acme.lending.v1.CompareLoanApplicationOptionsRequest;
import com.acme.lending.v1.CompareLoanApplicationOptionsResponse;
import com.acme.lending.v1.EstimateLoanApplicationRequest;
import com.acme.lending.v1.EstimateLoanApplicationResponse;
import com.acme.lending.v1.ExplainLoanApplicationCalculationRequest;
import com.acme.lending.v1.ExplainLoanApplicationCalculationResponse;
import com.acme.lending.v1.GetLoanApplicationRatesRequest;
import com.acme.lending.v1.GetLoanApplicationRatesResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.QuoteLoanApplicationRequest;
import com.acme.lending.v1.QuoteLoanApplicationResponse;
import com.acme.lending.v1.RecalculateLoanApplicationsRequest;
import com.acme.lending.v1.RecalculateLoanApplicationsResponse;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.ticketing.v1.Ticket;
import com.acme.ticketing.v1.TicketBankSettlement;
import com.acme.ticketing.v1.TicketServiceGrpc;
import com.acme.ticketing.v1.UpdateTicketRequest;
import com.acme.ticketing.v1.UpdateTicketResponse;
import com.acme.venues.v1.BrowseVenuesRequest;
import com.acme.venues.v1.BrowseVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a loan application, such as totals, taxes, and estimates. Coordinates validation,
 * durable audit writes, and downstream operations.
 */
@Service
public class LoanApplicationCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanApplicationCalculationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final ArticleQueryServiceGrpc.ArticleQueryServiceBlockingStub contentArticleQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceFutureStub insuranceClaimValidationServiceFuture;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final PostingBatchAdminServiceGrpc.PostingBatchAdminServiceBlockingStub ledgerPostingBatchAdminServiceBlocking;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    private final TicketServiceGrpc.TicketServiceBlockingStub ticketingTicketServiceBlocking;

    public LoanApplicationCalculationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            ArticleQueryServiceGrpc.ArticleQueryServiceBlockingStub contentArticleQueryServiceBlocking,
            ApplicationEventPublisher events,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            ClaimValidationServiceGrpc.ClaimValidationServiceFutureStub insuranceClaimValidationServiceFuture,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            PostingBatchAdminServiceGrpc.PostingBatchAdminServiceBlockingStub ledgerPostingBatchAdminServiceBlocking,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            TicketServiceGrpc.TicketServiceBlockingStub ticketingTicketServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.collectionsCollectionCaseServiceBlocking = collectionsCollectionCaseServiceBlocking;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.contentArticleQueryServiceBlocking = contentArticleQueryServiceBlocking;
        this.events = events;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceClaimValidationServiceFuture = insuranceClaimValidationServiceFuture;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.ledgerPostingBatchAdminServiceBlocking = ledgerPostingBatchAdminServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.ticketingTicketServiceBlocking = ticketingTicketServiceBlocking;
    }

    public record ExplainLoanApplicationCalculationStage0Requested(NormalizeClaimRequest request, CompletableFuture<NormalizeClaimResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a loan application.
     */
    public CalculateLoanApplicationResponse calculateLoanApplication(CalculateLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = UpdateTicketRequest.newBuilder()
                .setContext(request.getContext())
                .setTicket(Ticket.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setBank(TicketBankSettlement.newBuilder()
                                .setBic(switch (request.getLoanApplication().getSettlementCase()) {
                                    case BANK -> request.getLoanApplication().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = ticketingTicketServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateTicket(outbound0);

        var builder1 = ListJournalEntryVersionsRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(ListJournalEntryVersionsRequest.Filter.newBuilder()
                .setCreatedAfter(request.getLoanApplication().getAudit().getUpdatedAt())
                .setOwnerId(reply0.getTicket().getCounterparty().getVenueId())
                .setRegion(request.getDetails().getDestination().getRegion())
                .build());
        var outbound1 = builder1.build();
        var reply1 = ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listJournalEntryVersions(outbound1);

        var futures2 = request.getLoanApplication().getLinesList().stream()
                .map(element -> CheckFeatureFlagEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setDiscount(FeatureFlagDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkFeatureFlagEligibility(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? CheckFeatureFlagEligibilityResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
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
                                .setCreatedAt(request.getLoanApplication().getAudit().getCreatedAt())
                                .setCreatedBy(request.getLoanApplication().getAudit().getCreatedBy())
                                .setEtag(request.getLoanApplication().getAudit().getEtag())
                                .setUpdatedAt(request.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return CalculateLoanApplicationResponse.newBuilder()
                .setBreakdown(CalculateLoanApplicationResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getTicket().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getTicket().getCounterparty().getDisplayName())
                                .build())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates a loan application.
     */
    public EstimateLoanApplicationResponse estimateLoanApplication(EstimateLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = RpcSupport.unpack(Any.pack(PushAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBank(AuditTrailBankSettlement.newBuilder()
                                .setBic(request.getLoanApplication().getBank().getBic())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .setRecipient(request.getLoanApplication().getBillingAddress().getRecipient())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PushAuditTrailRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build()), PushAuditTrailRequest.class);
        var reply0 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushAuditTrail(outbound0);

        var outbound1 = BrowseVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLoanApplication().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getAuditTrail().getMetadataMap())
                        .setOwnerId(reply0.getAuditTrail().getAuditTrailId())
                        .setRegion(reply0.getAuditTrail().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.venues.v1.VenueQueryService");
        var selected1 = VenueQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.browseVenues(outbound1);

        var outbound2 = NormalizeWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAuditTrail().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .setBank(WriteOffBankSettlement.newBuilder()
                                .setBic(switch (request.getLoanApplication().getSettlementCase()) {
                                    case BANK -> request.getLoanApplication().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getLoanApplication().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply2 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeWriteOff(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return EstimateLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getWriteOff().getAudit().getUpdatedAt())
                                .build())
                        .setDueAt(request.getLoanApplication().getDueAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .putAllMetadata(reply0.getAuditTrail().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes a loan application.
     */
    public QuoteLoanApplicationResponse quoteLoanApplication(QuoteLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = RpcSupport.unpack(Any.pack(SimulateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateClaimRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build()), SimulateClaimRequest.class);
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateClaim(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return QuoteLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getClaim().getAssignee().getEmail())
                                .build())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .setNotes(request.getLoanApplication().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Compares a loan application.
     */
    public CompareLoanApplicationOptionsResponse compareLoanApplicationOptions(CompareLoanApplicationOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareLoanApplicationOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = BrowseFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseFeatureFlagsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseFeatureFlags(value)).orElseThrow();

        var outbound1 = UpdateCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getLoanApplication().getCounterparty().getDisplayName())
                                .build())
                        .addComments(CollectionCaseComment.newBuilder()
                                .setCreatedAt(request.getLoanApplication().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateCollectionCase(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareLoanApplicationOptions", request.getLoanApplication().getLoanApplicationId());
        return CompareLoanApplicationOptionsResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getCollectionCase().getAudit().getUpdatedBy())
                                .build())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .setNotes(request.getLoanApplication().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Recalculates a loan application.
     */
    public RecalculateLoanApplicationsResponse recalculateLoanApplications(RecalculateLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = ResolveArticlesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = contentArticleQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveArticles(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateLoanApplications", request.getLoanApplication().getLoanApplicationId());
        return RecalculateLoanApplicationsResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(request.getLoanApplication().getCounterparty())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Simulates a loan application.
     */
    public SimulateLoanApplicationResponse simulateLoanApplication(SimulateLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = NormalizeClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getLoanApplication().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setCustomerId(request.getLoanApplication().getCounterparty().getCustomerId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeClaimRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = insuranceClaimValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeClaim(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return SimulateLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getClaim().getAudit().getEtag())
                                .build())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .putAllMetadata(request.getLoanApplication().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a loan application.
     */
    public ExplainLoanApplicationCalculationResponse explainLoanApplicationCalculation(ExplainLoanApplicationCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainLoanApplicationCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = NormalizeClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeClaimRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<NormalizeClaimResponse>();
        events.publishEvent(new ExplainLoanApplicationCalculationStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = ValidateUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLoanApplication().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .setSla(UnderwritingSla.newBuilder()
                                .setPausedFor(reply0.getClaim().getSla().getPausedFor())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUnderwriting(value)).orElseThrow();

        var outbound2 = LintFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getClaim().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getClaim().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintFeatureFlags(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainLoanApplicationCalculation", request.getLoanApplication().getLoanApplicationId());
        return ExplainLoanApplicationCalculationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getClaim().getRequester().getEmail())
                                .build())
                        .setDueAt(request.getLoanApplication().getDueAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a loan application.
     */
    public GetLoanApplicationRatesResponse getLoanApplicationRates(GetLoanApplicationRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLoanApplicationRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getLoanApplicationId(), "GetLoanApplicationRates");

        var outbound0 = RepairPostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RepairPostingBatchRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .putAllMetadata(request.getContext().getBaggageMap())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> ledgerPostingBatchAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).repairPostingBatch(value)).orElseThrow();
        return GetLoanApplicationRatesResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
