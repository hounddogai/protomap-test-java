package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.audit.v1.StreamAuditTrailNoticesRequest;
import com.acme.audit.v1.StreamAuditTrailNoticesResponse;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.LintFeatureFlagsRequest;
import com.acme.featureflags.v1.LintFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RetryFiscalPeriodExportRequest;
import com.acme.ledger.v1.RetryFiscalPeriodExportResponse;
import com.acme.lending.v1.BrowseLoanApplicationsRequest;
import com.acme.lending.v1.BrowseLoanApplicationsResponse;
import com.acme.lending.v1.BrowseLoansRequest;
import com.acme.lending.v1.BrowseLoansResponse;
import com.acme.lending.v1.CountLoansRequest;
import com.acme.lending.v1.CountLoansResponse;
import com.acme.lending.v1.ExportLoansRequest;
import com.acme.lending.v1.ExportLoansResponse;
import com.acme.lending.v1.FindLoansRequest;
import com.acme.lending.v1.FindLoansResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LookupLoanRequest;
import com.acme.lending.v1.LookupLoanResponse;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.lending.v1.PeekLoanRequest;
import com.acme.lending.v1.PeekLoanResponse;
import com.acme.lending.v1.QuoteLoanRequest;
import com.acme.lending.v1.QuoteLoanResponse;
import com.acme.lending.v1.ResolveLoansRequest;
import com.acme.lending.v1.ResolveLoansResponse;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.lending.v1.SuggestLoansRequest;
import com.acme.lending.v1.SuggestLoansResponse;
import com.acme.lending.v1.SummarizeLoansRequest;
import com.acme.lending.v1.SummarizeLoansResponse;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import com.acme.localization.v1.SearchTranslationKeysRequest;
import com.acme.localization.v1.SearchTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about loans, for screens and reports. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class LoanQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanQueryApplication.class);

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceFutureStub featureflagsFeatureFlagServiceFuture;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceFutureStub featureflagsFeatureFlagValidationServiceFuture;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final TranslationKeyServiceGrpc.TranslationKeyServiceBlockingStub localizationTranslationKeyServiceBlocking;

    private final LoanQueryRequestMapper mapper;

    private final LendingRepository repository;

    public LoanQueryApplication(
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            FeatureFlagServiceGrpc.FeatureFlagServiceFutureStub featureflagsFeatureFlagServiceFuture,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceFutureStub featureflagsFeatureFlagValidationServiceFuture,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            TranslationKeyServiceGrpc.TranslationKeyServiceBlockingStub localizationTranslationKeyServiceBlocking,
            LoanQueryRequestMapper mapper,
            LendingRepository repository) {
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.featureflagsFeatureFlagServiceFuture = featureflagsFeatureFlagServiceFuture;
        this.featureflagsFeatureFlagValidationServiceFuture = featureflagsFeatureFlagValidationServiceFuture;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.lendingLoanApplicationQueryServiceBlocking = lendingLoanApplicationQueryServiceBlocking;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.localizationTranslationKeyServiceBlocking = localizationTranslationKeyServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    public record BrowseLoansStage0Requested(CalculateCoverageRequest request, CompletableFuture<CalculateCoverageResponse> completion, io.grpc.Context context) {}

    /**
     * Returns a loan.
     */
    public LookupLoanResponse lookupLoan(LookupLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "LookupLoan");

        var outbound0 = LintFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintFeatureFlagsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = featureflagsFeatureFlagValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).lintFeatureFlags(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = BatchGetFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = featureflagsFeatureFlagServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetFeatureFlags(outbound1);
        var reply1 = RpcSupport.await(pending1);
        return LookupLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getFeatureFlag().getAudit().getCreatedBy())
                                .build())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists loans a page at a time.
     */
    public BrowseLoansResponse browseLoans(BrowseLoansRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseLoans")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getContext().getTraceId(), "BrowseLoans");

        var outbound0 = CalculateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CalculateCoverageResponse>();
        events.publishEvent(new BrowseLoansStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseLoans", request.getPage().getPageToken());
        return BrowseLoansResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists loans a page at a time.
     */
    public FindLoansResponse findLoans(FindLoansRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindLoans")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getContext().getTraceId(), "FindLoans");

        var outbound0 = BrowseLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        Function<BrowseLoanApplicationsRequest, BrowseLoanApplicationsResponse> operation0 = lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::browseLoanApplications;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindLoans", request.getPage().getPageToken());
        return FindLoansResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Counts loans by status.
     */
    public CountLoansResponse countLoans(CountLoansRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountLoans")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getContext().getTraceId(), "CountLoans");

        var outbound0 = mapper.countLoansStage0(request);
        var reply0 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryFiscalPeriodExport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountLoans", request.getPage().getPageToken());
        return CountLoansResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several loans at once.
     */
    public ResolveLoansResponse resolveLoans(ResolveLoansRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveLoans")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getContext().getTraceId(), "ResolveLoans");

        var outbound0 = ValidatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePolicyRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getContext().getActor().getUserId())
                        .setOpenedAt(request.getContext().getDeadline())
                        .build())
                .build();
        var reply0 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePolicy(outbound0);

        var outbound1 = NormalizeLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeLoanApplicationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setCard(LoanApplicationCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getPolicy().getFundingCase()) {
                                    case CARD -> reply0.getPolicy().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLoanApplication(outbound1);
        return ResolveLoansResponse.newBuilder()
                .addLoans(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getLoanApplication().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getPolicy().getContact().getName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Streams every matching loan.
     */
    public ExportLoansResponse exportLoans(ExportLoansRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportLoans")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getContext().getTraceId(), "ExportLoans");

        var outbound0 = SearchTranslationKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchTranslationKeysRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> localizationTranslationKeyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchTranslationKeys(value));

        var outbound1 = StreamAuditTrailNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditTrailNoticesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream1 = auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditTrailNotices(outbound1);
        var reply1 = StreamAuditTrailNoticesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportLoans", request.getFilter().getQuery());
        return ExportLoansResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Summarizes a loan.
     */
    public SummarizeLoansResponse summarizeLoans(SummarizeLoansRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeLoans")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "SummarizeLoans");

        var outbound0 = RevertAuditTrailRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setAuditTrail(AuditTrail.newBuilder()
                        .addLines(AuditTrailLine.newBuilder()
                                .putAllAttributes(request.getDetails().getFiltersMap())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RevertAuditTrailRequest.Details.newBuilder()
                        .setAuthorEmail(request.getContext().getActor().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        Function<RevertAuditTrailRequest, RevertAuditTrailResponse> operation0 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::revertAuditTrail;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = PreviewStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply0.getAuditTrail().getBillingAddress().getLocality())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getAuditTrail().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.banking.v1.StandingOrderValidationService");
        var selected1 = StandingOrderValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.previewStandingOrder(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(ValidateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateLoanApplicationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getStandingOrder().getOrganizer().getEmail())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getAuditTrail().getAudit().getEtag())
                                .build())
                        .setCard(LoanApplicationCardSettlement.newBuilder()
                                .setHolderName(reply0.getAuditTrail().getCard().getHolderName())
                                .setLast4(reply0.getAuditTrail().getCard().getLast4())
                                .build())
                        .build())
                .build()), ValidateLoanApplicationRequest.class);
        var reply2 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateLoanApplication(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeLoans", request.getLoanId());
        return SummarizeLoansResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists loans a page at a time.
     */
    public SuggestLoansResponse suggestLoans(SuggestLoansRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestLoans")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getContext().getTraceId(), "SuggestLoans");

        var outbound0 = QuoteLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
        final class Stage0Adapter {
            QuoteLoanResponse fetch(QuoteLoanRequest input) {
                return lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteLoan(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestLoans", request.getPage().getPageToken());
        return SuggestLoansResponse.newBuilder()
                .addLoans(Loan.newBuilder()
                        .setOpenedAt(reply0.getLoan().getOpenedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a loan.
     */
    public PeekLoanResponse peekLoan(PeekLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "PeekLoan");

        var outbound0 = SimulateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setDueAt(request.getContext().getDeadline())
                        .addLines(LoanApplicationLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build();
        Function<SimulateLoanApplicationRequest, SimulateLoanApplicationResponse> operation0 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulateLoanApplication;
        var reply0 = operation0.apply(outbound0);
        return PeekLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getLoanApplication().getAudit().getEtag())
                                .build())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
