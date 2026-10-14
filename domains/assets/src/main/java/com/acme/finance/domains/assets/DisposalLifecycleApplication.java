package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateDisposalRequest;
import com.acme.assets.v1.ActivateDisposalResponse;
import com.acme.assets.v1.CountDisposalTotalsRequest;
import com.acme.assets.v1.CountDisposalTotalsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.DisposalSla;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.ExpireDisposalRequest;
import com.acme.assets.v1.ExpireDisposalResponse;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GenerateDisposalReportRequest;
import com.acme.assets.v1.GenerateDisposalReportResponse;
import com.acme.assets.v1.GetDisposalLifecycleRequest;
import com.acme.assets.v1.GetDisposalLifecycleResponse;
import com.acme.assets.v1.GetDisposalSyncStateRequest;
import com.acme.assets.v1.GetDisposalSyncStateResponse;
import com.acme.assets.v1.ListDisposalConflictsRequest;
import com.acme.assets.v1.ListDisposalConflictsResponse;
import com.acme.assets.v1.ListDisposalReportsRequest;
import com.acme.assets.v1.ListDisposalReportsResponse;
import com.acme.assets.v1.PushDisposalRequest;
import com.acme.assets.v1.PushDisposalResponse;
import com.acme.assets.v1.ReinstateDisposalRequest;
import com.acme.assets.v1.ReinstateDisposalResponse;
import com.acme.assets.v1.RenewDisposalRequest;
import com.acme.assets.v1.RenewDisposalResponse;
import com.acme.assets.v1.ScheduleDisposalExpiryRequest;
import com.acme.assets.v1.ScheduleDisposalExpiryResponse;
import com.acme.assets.v1.ScoreDepreciationRequest;
import com.acme.assets.v1.ScoreDepreciationResponse;
import com.acme.assets.v1.ScoreDisposalRequest;
import com.acme.assets.v1.ScoreDisposalResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.assets.v1.SuspendDisposalRequest;
import com.acme.assets.v1.SuspendDisposalResponse;
import com.acme.assets.v1.TerminateDisposalRequest;
import com.acme.assets.v1.TerminateDisposalResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.PreviewAccessPolicyRequest;
import com.acme.authorization.v1.PreviewAccessPolicyResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.RecallWriteOffRequest;
import com.acme.collections.v1.RecallWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.ExportJournalEntriesRequest;
import com.acme.ledger.v1.ExportJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersRequest;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersResponse;
import com.acme.ledger.v1.NotifyPostingBatchWatchersRequest;
import com.acme.ledger.v1.NotifyPostingBatchWatchersResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.WatchLedgerAccountsRequest;
import com.acme.ledger.v1.WatchLedgerAccountsResponse;
import com.acme.scheduling.v1.CountShiftsRequest;
import com.acme.scheduling.v1.CountShiftsResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.shows.v1.CreateShowRequest;
import com.acme.shows.v1.CreateShowResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowParticipant;
import com.acme.shows.v1.ShowServiceGrpc;
import com.acme.telematics.v1.CompareTelemetryReadingOptionsRequest;
import com.acme.telematics.v1.CompareTelemetryReadingOptionsResponse;
import com.acme.telematics.v1.TelemetryReading;
import com.acme.telematics.v1.TelemetryReadingCalculationServiceGrpc;
import com.google.protobuf.Any;
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
 * Moves a disposal between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class DisposalLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(DisposalLifecycleApplication.class);

    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceFutureStub assetsDepreciationValidationServiceFuture;

    private final DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceBlockingStub assetsDisposalSyncServiceBlocking;

    private final DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync;

    private final WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking;

    private final ApplicationEventPublisher events;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final DisposalLifecycleRequestMapper mapper;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    private final ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking;

    private final ShowServiceGrpc.ShowServiceBlockingStub showsShowServiceBlocking;

    private final TelemetryReadingCalculationServiceGrpc.TelemetryReadingCalculationServiceBlockingStub telematicsTelemetryReadingCalculationServiceBlocking;

    public DisposalLifecycleApplication(
            DepreciationValidationServiceGrpc.DepreciationValidationServiceFutureStub assetsDepreciationValidationServiceFuture,
            DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking,
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            DisposalSyncServiceGrpc.DisposalSyncServiceBlockingStub assetsDisposalSyncServiceBlocking,
            DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync,
            WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking,
            ApplicationEventPublisher events,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            DisposalLifecycleRequestMapper mapper,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking,
            ShowServiceGrpc.ShowServiceBlockingStub showsShowServiceBlocking,
            TelemetryReadingCalculationServiceGrpc.TelemetryReadingCalculationServiceBlockingStub telematicsTelemetryReadingCalculationServiceBlocking) {
        this.assetsDepreciationValidationServiceFuture = assetsDepreciationValidationServiceFuture;
        this.assetsDisposalReportingServiceBlocking = assetsDisposalReportingServiceBlocking;
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.assetsDisposalSyncServiceBlocking = assetsDisposalSyncServiceBlocking;
        this.assetsDisposalValidationServiceBlocking = assetsDisposalValidationServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.budgetingBudgetServiceAsync = budgetingBudgetServiceAsync;
        this.collectionsWriteOffApprovalServiceBlocking = collectionsWriteOffApprovalServiceBlocking;
        this.events = events;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.schedulingShiftQueryServiceBlocking = schedulingShiftQueryServiceBlocking;
        this.showsShowServiceBlocking = showsShowServiceBlocking;
        this.telematicsTelemetryReadingCalculationServiceBlocking = telematicsTelemetryReadingCalculationServiceBlocking;
    }

    public record ReinstateDisposalStage0Requested(CheckPostingBatchEligibilityRequest request, CompletableFuture<CheckPostingBatchEligibilityResponse> completion, io.grpc.Context context) {}

    public record TerminateDisposalStage0Requested(ListDisposalConflictsRequest request, CompletableFuture<ListDisposalConflictsResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a disposal.
     */
    public ActivateDisposalResponse activateDisposal(ActivateDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "ActivateDisposal");

        var outbound0 = JsonCodec.roundTrip(CountShiftsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountShiftsRequest.Filter.newBuilder()
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build(), CountShiftsRequest.class);
        var reply0 = schedulingShiftQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countShifts(outbound0);

        var outbound1 = BatchCreateBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBudgetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateBudgetsResponse.getDefaultInstance());
        var input1 = budgetingBudgetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBudgets(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
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
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateDisposal", request.getDetails().getReasonCode());
        return ActivateDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setOutcome(ActivateDisposalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends a disposal.
     */
    public SuspendDisposalResponse suspendDisposal(SuspendDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "SuspendDisposal");

        var outbound0 = ValidateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateFixedAssetRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ValidateFixedAssetRequest, ValidateFixedAssetResponse>() {
            @Override
            public ValidateFixedAssetResponse apply(ValidateFixedAssetRequest input) {
                return assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateFixedAsset(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendDisposal", request.getDetails().getReasonCode());
        return SuspendDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setOutcome(SuspendDisposalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a disposal.
     */
    public ReinstateDisposalResponse reinstateDisposal(ReinstateDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "ReinstateDisposal");

        var outbound0 = CheckPostingBatchEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckPostingBatchEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CheckPostingBatchEligibilityResponse>();
        events.publishEvent(new ReinstateDisposalStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = RecallWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPostingBatch().getBillingAddress().getCountryCode())
                                .build())
                        .setCard(WriteOffCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getPostingBatch().getSettlementCase()) {
                                    case CARD -> reply0.getPostingBatch().getCard().getLast4();
                                    default -> "";
                                })
                                .setNetwork(switch (reply0.getPostingBatch().getSettlementCase()) {
                                    case CARD -> reply0.getPostingBatch().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setDisplayName(reply0.getPostingBatch().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallWriteOff(outbound1);

        var builder2 = CompareTelemetryReadingOptionsRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setDetails(CompareTelemetryReadingOptionsRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setRegion(reply1.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build());
        builder2.setTelemetryReading(TelemetryReading.newBuilder()
                .setDetail(Any.pack(request.getContext()))
                .build());
        var outbound2 = builder2.build();
        var reply2 = telematicsTelemetryReadingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareTelemetryReadingOptions(outbound2);

        var outbound3 = CountDisposalTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountDisposalTotalsRequest.Filter.newBuilder()
                        .setRegion(reply0.getPostingBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countDisposalTotals(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateDisposal", request.getDetails().getReasonCode());
        return ReinstateDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPostingBatch().getAudit().getCreatedAt())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setOutcome(ReinstateDisposalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Terminates a disposal.
     */
    public TerminateDisposalResponse terminateDisposal(TerminateDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "TerminateDisposal");

        var outbound0 = ListDisposalConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDisposalConflictsRequest.Filter.newBuilder()
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ListDisposalConflictsResponse>();
        events.publishEvent(new TerminateDisposalStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = WatchLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setName(request.getDetails().getNotifyContact().getName())
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .build())
                .build();
        var stream1 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchLedgerAccounts(outbound1);
        var reply1 = WatchLedgerAccountsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = ScoreDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                                        .setName(request.getDetails().getNotifyContact().getName())
                                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending2 = assetsDepreciationValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scoreDepreciation(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getDepreciation().getAudit().getUpdatedBy())
                                .build())
                        .setCustomer(FixedAssetRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply2.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var collector3 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input3 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateDisposal", request.getDetails().getReasonCode());
        return TerminateDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply2.getDepreciation().getOrganizer().getUserId())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setOutcome(TerminateDisposalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Renews a disposal.
     */
    public RenewDisposalResponse renewDisposal(RenewDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "RenewDisposal");

        var builder0 = CreateShowRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setShow(Show.newBuilder()
                .setOrganizer(UserRef.newBuilder()
                        .setUserId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .addParticipants(ShowParticipant.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = showsShowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createShow(outbound0);

        var outbound1 = GenerateDisposalReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposalId(request.getDisposalId())
                .setReason(request.getReason())
                .build();
        var command1 = new DispatchCommand<>("acme.assets.v1.DisposalReportingService/GenerateDisposalReport", outbound1);
        var reply1 = assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateDisposalReport(command1.payload());

        var outbound2 = NotifyPostingBatchWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyPostingBatchWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            NotifyPostingBatchWatchersResponse fetch(NotifyPostingBatchWatchersRequest input) {
                return ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyPostingBatchWatchers(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = mapper.renewDisposalStage3(request);
        var reply3 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyLedgerAccountWatchers(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewDisposal", request.getDetails().getReasonCode());
        return RenewDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getShow().getAudit().getUpdatedBy())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(reply3.getLedgerAccount().getContact().getName())
                                .build())
                        .setSla(DisposalSla.newBuilder()
                                .setDueAt(reply2.getPostingBatch().getDueAt())
                                .build())
                        .build())
                .setOutcome(RenewDisposalResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply3.getOutcome().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Expires a disposal.
     */
    public ExpireDisposalResponse expireDisposal(ExpireDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "ExpireDisposal");

        var outbound0 = RpcSupport.unpack(Any.pack(PushDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushDisposalRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build()), PushDisposalRequest.class);
        var reply0 = assetsDisposalSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushDisposal(outbound0);

        var outbound1 = mapper.expireDisposalStage1(request, reply0);
        var reply1 = assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDisposalReports(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireDisposal", request.getDetails().getReasonCode());
        return ExpireDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setOutcome(ExpireDisposalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a disposal.
     */
    public GetDisposalLifecycleResponse getDisposalLifecycle(GetDisposalLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDisposalLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "GetDisposalLifecycle");

        var outbound0 = PreviewAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAccessPolicy(outbound0), rpcExecutor);

        var outbound1 = GetDisposalSyncStateRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposalId(request.getDisposalId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsDisposalSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getDisposalSyncState(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        return GetDisposalLifecycleResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAccessPolicy().getAudit().getCreatedBy())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getAccessPolicy().getOwner().getEmail())
                                .build())
                        .setSla(DisposalSla.newBuilder()
                                .setBreached(reply1.getDisposal().getSla().getBreached())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Schedules a disposal.
     */
    public ScheduleDisposalExpiryResponse scheduleDisposalExpiry(ScheduleDisposalExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleDisposalExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = ExportJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDisposal().getSla().getDueAt())
                        .setOwnerId(request.getDisposal().getCustomer().getFixedAssetId())
                        .build())
                .build();
        var stream0 = ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportJournalEntries(outbound0);
        var reply0 = ExportJournalEntriesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ScoreDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDisposal().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        Function<ScoreDisposalRequest, ScoreDisposalResponse> operation1 = assetsDisposalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreDisposal;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleDisposalExpiry", request.getDisposal().getDisposalId());
        return ScheduleDisposalExpiryResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getDisposal().getAssignee().getEmail())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setSubject(request.getDisposal().getSubject())
                        .build())
                .build();
    }
}
