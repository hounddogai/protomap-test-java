package com.acme.finance.domains.expenses;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetryRetentionPolicyExportRequest;
import com.acme.audit.v1.RetryRetentionPolicyExportResponse;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.budgeting.v1.DelegateVarianceApprovalRequest;
import com.acme.budgeting.v1.DelegateVarianceApprovalResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.ComparePreferenceOptionsRequest;
import com.acme.customers.v1.ComparePreferenceOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.NormalizePreferenceRequest;
import com.acme.customers.v1.NormalizePreferenceResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuoteConsentRecordRequest;
import com.acme.customers.v1.QuoteConsentRecordResponse;
import com.acme.expenses.v1.ActivateMileageClaimRequest;
import com.acme.expenses.v1.ActivateMileageClaimResponse;
import com.acme.expenses.v1.CompareReceiptOptionsRequest;
import com.acme.expenses.v1.CompareReceiptOptionsResponse;
import com.acme.expenses.v1.CountExpenseReportsRequest;
import com.acme.expenses.v1.CountExpenseReportsResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ExpenseReportServiceGrpc;
import com.acme.expenses.v1.ExpireMileageClaimRequest;
import com.acme.expenses.v1.ExpireMileageClaimResponse;
import com.acme.expenses.v1.GetMileageClaimLifecycleRequest;
import com.acme.expenses.v1.GetMileageClaimLifecycleResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.ReinstateMileageClaimRequest;
import com.acme.expenses.v1.ReinstateMileageClaimResponse;
import com.acme.expenses.v1.RenewMileageClaimRequest;
import com.acme.expenses.v1.RenewMileageClaimResponse;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryRequest;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryResponse;
import com.acme.expenses.v1.ShareMileageClaimReportRequest;
import com.acme.expenses.v1.ShareMileageClaimReportResponse;
import com.acme.expenses.v1.SuspendMileageClaimRequest;
import com.acme.expenses.v1.SuspendMileageClaimResponse;
import com.acme.expenses.v1.TerminateMileageClaimRequest;
import com.acme.expenses.v1.TerminateMileageClaimResponse;
import com.acme.expenses.v1.UpdateExpenseReportRequest;
import com.acme.expenses.v1.UpdateExpenseReportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.CloneUserRequest;
import com.acme.identity.v1.CloneUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.PreviewJournalEntryRequest;
import com.acme.ledger.v1.PreviewJournalEntryResponse;
import com.acme.search.v1.ExportSearchIndexesRequest;
import com.acme.search.v1.ExportSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.sponsorships.v1.SponsorQueryServiceGrpc;
import com.acme.sponsorships.v1.SummarizeSponsorsRequest;
import com.acme.sponsorships.v1.SummarizeSponsorsResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Moves a mileage claim between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class MileageClaimLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(MileageClaimLifecycleApplication.class);

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final ExpenseReportServiceGrpc.ExpenseReportServiceBlockingStub expensesExpenseReportServiceBlocking;

    private final MileageClaimReportingServiceGrpc.MileageClaimReportingServiceBlockingStub expensesMileageClaimReportingServiceBlocking;

    private final ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceFutureStub ledgerJournalEntryValidationServiceFuture;

    private final MileageClaimLifecycleRequestMapper mapper;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final SponsorQueryServiceGrpc.SponsorQueryServiceBlockingStub sponsorshipsSponsorQueryServiceBlocking;

    public MileageClaimLifecycleApplication(
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking,
            ChannelRegistry channels,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            ExpenseReportServiceGrpc.ExpenseReportServiceBlockingStub expensesExpenseReportServiceBlocking,
            MileageClaimReportingServiceGrpc.MileageClaimReportingServiceBlockingStub expensesMileageClaimReportingServiceBlocking,
            ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceFutureStub ledgerJournalEntryValidationServiceFuture,
            MileageClaimLifecycleRequestMapper mapper,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            SponsorQueryServiceGrpc.SponsorQueryServiceBlockingStub sponsorshipsSponsorQueryServiceBlocking) {
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.budgetingVarianceApprovalServiceBlocking = budgetingVarianceApprovalServiceBlocking;
        this.channels = channels;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.expensesExpenseReportServiceBlocking = expensesExpenseReportServiceBlocking;
        this.expensesMileageClaimReportingServiceBlocking = expensesMileageClaimReportingServiceBlocking;
        this.expensesReceiptCalculationServiceBlocking = expensesReceiptCalculationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.ledgerJournalEntryValidationServiceFuture = ledgerJournalEntryValidationServiceFuture;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.sponsorshipsSponsorQueryServiceBlocking = sponsorshipsSponsorQueryServiceBlocking;
    }

    /**
     * Activates a mileage claim.
     */
    public ActivateMileageClaimResponse activateMileageClaim(ActivateMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "ActivateMileageClaim");

        var outbound0 = StreamRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRetentionPolicyExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var stream0 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyExport(outbound0);
        var reply0 = StreamRetentionPolicyExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = JsonCodec.roundTrip(CompareReceiptOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build(), CompareReceiptOptionsRequest.class);
        var reply1 = expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareReceiptOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateMileageClaim", request.getDetails().getReasonCode());
        return ActivateMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getReceipt().getAudit().getCreatedAt())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setNumber(reply1.getReceipt().getCounterparty().getNumber())
                        .build())
                .setOutcome(ActivateMileageClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Suspends a mileage claim.
     */
    public SuspendMileageClaimResponse suspendMileageClaim(SuspendMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "SuspendMileageClaim");

        var outbound0 = SummarizeSponsorsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeSponsorsRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> sponsorshipsSponsorQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeSponsors(outbound0), rpcExecutor);

        var outbound1 = UpdateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> expensesExpenseReportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateExpenseReport(outbound1), rpcExecutor);

        var outbound2 = DelegateVarianceApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> budgetingVarianceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateVarianceApproval(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendMileageClaim", request.getDetails().getReasonCode());
        return SuspendMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getExpenseReport().getBillingAddress().getCountryCode())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setOutcome(SuspendMileageClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a mileage claim.
     */
    public ReinstateMileageClaimResponse reinstateMileageClaim(ReinstateMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "ReinstateMileageClaim");

        var outbound0 = RevertRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertRetentionPolicyRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<RevertRetentionPolicyRequest, RevertRetentionPolicyResponse> port0 = RpcPort.adapt(auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::revertRetentionPolicy);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ScoreAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply0.getRetentionPolicy().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setReason(switch (reply0.getRetentionPolicy().getActionCase()) {
                    case BLOCK -> reply0.getRetentionPolicy().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply1 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAuditEvent(outbound1);

        var outbound2 = JsonCodec.roundTrip(CloneUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build(), CloneUserRequest.class);
        var reply2 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneUser(outbound2);

        var outbound3 = NormalizePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.customers.v1.PreferenceValidationService");
        var serviceName3 = "acme.customers.v1" + "." + "PreferenceValidationService";
        var methodName3 = "Nor" + "malizePreference";
        var descriptor3 = MethodDescriptor.<NormalizePreferenceRequest, NormalizePreferenceResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(NormalizePreferenceRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(NormalizePreferenceResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateMileageClaim", request.getDetails().getReasonCode());
        return ReinstateMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply3.getPreference().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply2.getUser().getPrimaryAddress().getRecipient())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setNumber(reply1.getAuditEvent().getSubject().getNumber())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setOutcome(ReinstateMileageClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Terminates a mileage claim.
     */
    public TerminateMileageClaimResponse terminateMileageClaim(TerminateMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "TerminateMileageClaim");

        var outbound0 = ComparePreferenceOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePreferenceOptions(outbound0);

        var outbound1 = CalculateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedAt(reply0.getPreference().getAudit().getCreatedAt())
                                        .build())
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(reply0.getPreference().getAudit().getCreatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<CalculateHouseholdRequest, CalculateHouseholdResponse>() {
            @Override
            public CalculateHouseholdResponse apply(CalculateHouseholdRequest input) {
                return customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateHousehold(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateMileageClaim", request.getDetails().getReasonCode());
        return TerminateMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getHousehold().getLocation().getRegion())
                                .build())
                        .addLines(MileageClaimLine.newBuilder()
                                .setDescription(reply0.getPreference().getDescription())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setOutcome(TerminateMileageClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Renews a mileage claim.
     */
    public RenewMileageClaimResponse renewMileageClaim(RenewMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "RenewMileageClaim");

        var outbound0 = CountExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountExpenseReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.ExpenseReportQueryService");
        var selected0 = ExpenseReportQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.countExpenseReports(outbound0);

        var builder1 = NotifyRetentionPolicyWatchersRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(NotifyRetentionPolicyWatchersRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyRetentionPolicyWatchers(outbound1);

        var outbound2 = PreviewJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewJournalEntryRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setDisplayName(reply1.getRetentionPolicy().getOwner().getDisplayName())
                                .build())
                        .addLines(JournalEntryLine.newBuilder()
                                .setTax(JournalEntryLineTax.newBuilder()
                                        .setAmount(reply1.getRetentionPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending2 = ledgerJournalEntryValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).previewJournalEntry(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewMileageClaim", request.getDetails().getReasonCode());
        return RenewMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getJournalEntry().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(ReceiptRef.newBuilder()
                                .setDisplayName(reply2.getJournalEntry().getCounterparty().getDisplayName())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setOutcome(RenewMileageClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Expires a mileage claim.
     */
    public ExpireMileageClaimResponse expireMileageClaim(ExpireMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "ExpireMileageClaim");

        var outbound0 = mapper.expireMileageClaimStage0(request);
        var reply0 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryRetentionPolicyExport(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(UpdateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRetentionPolicy().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getRetentionPolicy().getAudit().getCreatedBy())
                                .setEtag(reply0.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(reply0.getRetentionPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build()), UpdateExpenseReportRequest.class);
        var reply1 = expensesExpenseReportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateExpenseReport(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireMileageClaim", request.getDetails().getReasonCode());
        return ExpireMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getExpenseReport().getAudit().getCreatedAt())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setOutcome(ExpireMileageClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a mileage claim.
     */
    public GetMileageClaimLifecycleResponse getMileageClaimLifecycle(GetMileageClaimLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetMileageClaimLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "GetMileageClaimLifecycle");

        var outbound0 = ShareMileageClaimReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareMileageClaimReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getContext().getBaggageMap())
                        .build())
                .setMileageClaimId(request.getMileageClaimId())
                .build();
        var command0 = new DispatchCommand<>("acme.expenses.v1.MileageClaimReportingService/ShareMileageClaimReport", outbound0);
        var reply0 = expensesMileageClaimReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareMileageClaimReport(command0.payload());

        var outbound1 = ExportSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getMileageClaim().getAudit().getUpdatedAt())
                        .setRegion(reply0.getMileageClaim().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSearchIndexes(outbound1);
        var reply1 = ExportSearchIndexesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        return GetMileageClaimLifecycleResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Schedules a mileage claim.
     */
    public ScheduleMileageClaimExpiryResponse scheduleMileageClaimExpiry(ScheduleMileageClaimExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleMileageClaimExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        List<QuoteConsentRecordResponse> results0 = request.getMileageClaim().getLinesList().stream()
                .map(element -> QuoteConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .putAllAttributes(element.getAttributesMap())
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getMileageClaim().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteConsentRecordRequest.Details.newBuilder()
                        .setBudget(element.getTax().getAmount())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteConsentRecord(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? QuoteConsentRecordResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleMileageClaimExpiry", request.getMileageClaim().getMileageClaimId());
        return ScheduleMileageClaimExpiryResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setCounterparty(request.getMileageClaim().getCounterparty())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .build();
    }
}
