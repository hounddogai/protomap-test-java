package com.acme.finance.domains.insurance;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.NormalizePreferenceRequest;
import com.acme.customers.v1.NormalizePreferenceResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.expenses.v1.BatchCreateExpenseReportsRequest;
import com.acme.expenses.v1.BatchCreateExpenseReportsResponse;
import com.acme.expenses.v1.ExpenseReportServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.CountCoverageTotalsRequest;
import com.acme.insurance.v1.CountCoverageTotalsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.DeleteCoverageReportRequest;
import com.acme.insurance.v1.DeleteCoverageReportResponse;
import com.acme.insurance.v1.FetchCoverageReportRequest;
import com.acme.insurance.v1.FetchCoverageReportResponse;
import com.acme.insurance.v1.GenerateCoverageReportRequest;
import com.acme.insurance.v1.GenerateCoverageReportResponse;
import com.acme.insurance.v1.LintPoliciesRequest;
import com.acme.insurance.v1.LintPoliciesResponse;
import com.acme.insurance.v1.ListCoverageReportsRequest;
import com.acme.insurance.v1.ListCoverageReportsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCardSettlement;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.ScheduleCoverageReportRequest;
import com.acme.insurance.v1.ScheduleCoverageReportResponse;
import com.acme.insurance.v1.ShareCoverageReportRequest;
import com.acme.insurance.v1.ShareCoverageReportResponse;
import com.acme.insurance.v1.StreamCoverageMetricsRequest;
import com.acme.insurance.v1.StreamCoverageMetricsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchAdminServiceGrpc;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.RepairPostingBatchRequest;
import com.acme.ledger.v1.RepairPostingBatchResponse;
import com.acme.ledger.v1.RetryFiscalPeriodExportRequest;
import com.acme.ledger.v1.RetryFiscalPeriodExportResponse;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.ledger.v1.StreamJournalEntryNoticesRequest;
import com.acme.ledger.v1.StreamJournalEntryNoticesResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ResolveLoansRequest;
import com.acme.lending.v1.ResolveLoansResponse;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.lending.v1.SimulateRepaymentRequest;
import com.acme.lending.v1.SimulateRepaymentResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about coverages. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class CoverageReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(CoverageReportingApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExpenseReportServiceGrpc.ExpenseReportServiceStub expensesExpenseReportServiceAsync;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final PostingBatchAdminServiceGrpc.PostingBatchAdminServiceBlockingStub ledgerPostingBatchAdminServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final LoanQueryServiceGrpc.LoanQueryServiceFutureStub lendingLoanQueryServiceFuture;

    private final CoverageReportingRequestMapper mapper;

    private final InsuranceRepository repository;

    public CoverageReportingApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            ApplicationEventPublisher events,
            ExpenseReportServiceGrpc.ExpenseReportServiceStub expensesExpenseReportServiceAsync,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            PostingBatchAdminServiceGrpc.PostingBatchAdminServiceBlockingStub ledgerPostingBatchAdminServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            LoanQueryServiceGrpc.LoanQueryServiceFutureStub lendingLoanQueryServiceFuture,
            CoverageReportingRequestMapper mapper,
            InsuranceRepository repository) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.events = events;
        this.expensesExpenseReportServiceAsync = expensesExpenseReportServiceAsync;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingImportServiceBlocking = insuranceUnderwritingImportServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.ledgerPostingBatchAdminServiceBlocking = ledgerPostingBatchAdminServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.lendingLoanQueryServiceFuture = lendingLoanQueryServiceFuture;
        this.mapper = mapper;
        this.repository = repository;
    }

    public record ShareCoverageReportStage1Requested(LintPoliciesRequest request, CompletableFuture<LintPoliciesResponse> completion, io.grpc.Context context) {}

    public record DeleteCoverageReportStage1Requested(ValidatePremiumRequest request, CompletableFuture<ValidatePremiumResponse> completion, io.grpc.Context context) {}

    /**
     * Generates a coverage.
     */
    public GenerateCoverageReportResponse generateCoverageReport(GenerateCoverageReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateCoverageReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "GenerateCoverageReport");

        var outbound0 = ValidateUnderwritingImportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ValidateUnderwritingImportRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insuranceUnderwritingImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUnderwritingImport(value)).orElseThrow();

        var outbound1 = SendPostingBatchTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendPostingBatchTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getUnderwriting().getAssignee().getEmail())
                                .setName(reply0.getUnderwriting().getRequester().getName())
                                .build())
                        .build())
                .build();
        var reply1 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendPostingBatchTestNotice(outbound1);

        var outbound2 = SimulateRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateRepaymentRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getPostingBatch().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply1.getPostingBatch().getCounterparty().getDisplayName())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getUnderwriting().getAssignee().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.lending.v1.RepaymentCalculationService");
        var selected2 = RepaymentCalculationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.simulateRepayment(outbound2);

        var outbound3 = StreamJournalEntryNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getUnderwriting().getAssignee().getEmail())
                        .setPreferredChannel(reply0.getUnderwriting().getRequester().getPreferredChannel())
                        .build())
                .build();
        var stream3 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamJournalEntryNotices(outbound3);
        var reply3 = StreamJournalEntryNoticesResponse.getDefaultInstance();
        while (stream3.hasNext()) {
            reply3 = stream3.next();
        }

        var outbound4 = BatchCreateExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateExpenseReportsRequest.StreamHeader.newBuilder()
                        .setFormat(request.getDetails().getFormat())
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateExpenseReportsResponse.getDefaultInstance());
        var input4 = expensesExpenseReportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateExpenseReports(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamRetentionPolicyChangesRequest.newBuilder()
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
        var collector5 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input5 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateCoverageReport", request.getCoverageId());
        return GenerateCoverageReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a coverage.
     */
    public FetchCoverageReportResponse fetchCoverageReport(FetchCoverageReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchCoverageReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "FetchCoverageReport");

        var outbound0 = ResolveLoansRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = lendingLoanQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).resolveLoans(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = mapper.fetchCoverageReportStage1(request);
        var reply1 = customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePreference(outbound1);
        return FetchCoverageReportResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPreference().getAudit().getEtag())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Schedules a coverage.
     */
    public ScheduleCoverageReportResponse scheduleCoverageReport(ScheduleCoverageReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleCoverageReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "ScheduleCoverageReport");

        var outbound0 = PullPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var command0 = new DispatchCommand<>("acme.insurance.v1.PremiumSyncService/PullPremiums", outbound0);
        var reply0 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullPremiums(command0.payload());

        var outbound1 = SubscribePostingBatchUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SubscribePostingBatchUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply1 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribePostingBatchUpdates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleCoverageReport", request.getCoverageId());
        return ScheduleCoverageReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of coverages as they happen.
     */
    public StreamCoverageMetricsResponse streamCoverageMetrics(StreamCoverageMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamCoverageMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getContext().getTraceId(), "StreamCoverageMetrics");

        var outbound0 = SimulateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateCollateralRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        Function<SimulateCollateralRequest, SimulateCollateralResponse> operation0 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulateCollateral;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = CalculateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCollateral().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getCollateral().getOwner().getUserId())
                                .build())
                        .build())
                .setDetails(CalculateCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getCollateral().getAddress().getLinesList())
                                .setLocality(switch (reply0.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> reply0.getCollateral().getAddress().getLocality();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCoverage(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamCoverageMetrics", request.getFilter().getQuery());
        return StreamCoverageMetricsResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCollateral().getAudit().getCreatedBy())
                                .build())
                        .setOwner(reply1.getCoverage().getOwner())
                        .addAllScopes(reply1.getCoverage().getScopesList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lists coverages a page at a time.
     */
    public ListCoverageReportsResponse listCoverageReports(ListCoverageReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListCoverageReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getContext().getTraceId(), "ListCoverageReports");

        var outbound0 = PullPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllMetadata(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var reply0 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullPremiums(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListCoverageReports", request.getPage().getPageToken());
        return ListCoverageReportsResponse.newBuilder()
                .addCoverages(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPremium().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Shares a coverage.
     */
    public ShareCoverageReportResponse shareCoverageReport(ShareCoverageReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareCoverageReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "ShareCoverageReport");

        var outbound0 = RetryFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .addParticipants(FiscalPeriodParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getRecipient().getEmail())
                                        .setPhone(request.getDetails().getRecipient().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryFiscalPeriodExport(value));

        var outbound1 = LintPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply0.getFiscalPeriod().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<LintPoliciesResponse>();
        events.publishEvent(new ShareCoverageReportStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPreferredChannel()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareCoverageReport", request.getDetails().getRecipient().getPreferredChannel());
        return ShareCoverageReportResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFiscalPeriod().getAudit().getCreatedAt())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .setOutcome(ShareCoverageReportResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Counts coverages by status.
     */
    public CountCoverageTotalsResponse countCoverageTotals(CountCoverageTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountCoverageTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getContext().getTraceId(), "CountCoverageTotals");

        var outbound0 = SuggestJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.ledger.v1.JournalEntryQueryService/SuggestJournalEntries", outbound0);
        var reply0 = ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestJournalEntries(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountCoverageTotals", request.getPage().getPageToken());
        return CountCoverageTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Deletes a coverage.
     */
    public DeleteCoverageReportResponse deleteCoverageReport(DeleteCoverageReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteCoverageReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getCoverageId(), "DeleteCoverageReport");

        var builder0 = RepairPostingBatchRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(RepairPostingBatchRequest.Details.newBuilder()
                .setPayload(JsonCodec.encode(request.getContext()))
                .build());
        builder0.setPostingBatch(PostingBatch.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setEtag(request.getEtag())
                        .build())
                .build());
        builder0.setReason(request.getReason());
        var outbound0 = builder0.build();
        var reply0 = ledgerPostingBatchAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).repairPostingBatch(outbound0);

        var outbound1 = ValidatePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPostingBatch().getAudit().getEtag())
                                .setUpdatedBy(reply0.getPostingBatch().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getPostingBatch().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ValidatePremiumResponse>();
        events.publishEvent(new DeleteCoverageReportStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = PullPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getPremium().getBillingAddress().getRegion())
                                .build())
                        .setCard(PremiumCardSettlement.newBuilder()
                                .setToken(reply1.getPremium().getCard().getToken())
                                .build())
                        .build())
                .build();
        var reply2 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullPremiums(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteCoverageReport", request.getCoverageId());
        return DeleteCoverageReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
