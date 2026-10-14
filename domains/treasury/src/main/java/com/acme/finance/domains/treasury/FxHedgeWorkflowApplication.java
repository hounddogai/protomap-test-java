package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareCustomerSegmentOptionsRequest;
import com.acme.customers.v1.CompareCustomerSegmentOptionsResponse;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.customers.v1.QuoteHouseholdRequest;
import com.acme.customers.v1.QuoteHouseholdResponse;
import com.acme.customers.v1.RecalculatePreferencesRequest;
import com.acme.customers.v1.RecalculatePreferencesResponse;
import com.acme.expenses.v1.ApproveMileageClaimRequest;
import com.acme.expenses.v1.ApproveMileageClaimResponse;
import com.acme.expenses.v1.EscalateMileageClaimRequest;
import com.acme.expenses.v1.EscalateMileageClaimResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimApprovalServiceGrpc;
import com.acme.expenses.v1.MileageClaimBankSettlement;
import com.acme.expenses.v1.MileageClaimCardSettlement;
import com.acme.expenses.v1.SubmitMileageClaimRequest;
import com.acme.expenses.v1.SubmitMileageClaimResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.RecallCoverageRequest;
import com.acme.insurance.v1.RecallCoverageResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.WatchLedgerAccountsRequest;
import com.acme.ledger.v1.WatchLedgerAccountsResponse;
import com.acme.notifications.v1.FindNotificationsRequest;
import com.acme.notifications.v1.FindNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.treasury.v1.AdvanceFxHedgeWorkflowRequest;
import com.acme.treasury.v1.AdvanceFxHedgeWorkflowResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferApprovalServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.BankTransferWorkflowServiceGrpc;
import com.acme.treasury.v1.CancelFxHedgeWorkflowRequest;
import com.acme.treasury.v1.CancelFxHedgeWorkflowResponse;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CommentBankTransferApprovalRequest;
import com.acme.treasury.v1.CommentBankTransferApprovalResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeLine;
import com.acme.treasury.v1.InspectFxHedgeWorkflowRequest;
import com.acme.treasury.v1.InspectFxHedgeWorkflowResponse;
import com.acme.treasury.v1.ListBankTransferReportsRequest;
import com.acme.treasury.v1.ListBankTransferReportsResponse;
import com.acme.treasury.v1.ListFxHedgeWorkflowsRequest;
import com.acme.treasury.v1.ListFxHedgeWorkflowsResponse;
import com.acme.treasury.v1.PauseFxHedgeWorkflowRequest;
import com.acme.treasury.v1.PauseFxHedgeWorkflowResponse;
import com.acme.treasury.v1.ResumeFxHedgeWorkflowRequest;
import com.acme.treasury.v1.ResumeFxHedgeWorkflowResponse;
import com.acme.treasury.v1.RetryFxHedgeWorkflowRequest;
import com.acme.treasury.v1.RetryFxHedgeWorkflowResponse;
import com.acme.treasury.v1.SignalBankTransferWorkflowRequest;
import com.acme.treasury.v1.SignalBankTransferWorkflowResponse;
import com.acme.treasury.v1.SignalFxHedgeWorkflowRequest;
import com.acme.treasury.v1.SignalFxHedgeWorkflowResponse;
import com.acme.treasury.v1.StartFxHedgeWorkflowRequest;
import com.acme.treasury.v1.StartFxHedgeWorkflowResponse;
import com.acme.treasury.v1.SubmitBankTransferRequest;
import com.acme.treasury.v1.SubmitBankTransferResponse;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a fx hedge from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class FxHedgeWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(FxHedgeWorkflowApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final MileageClaimApprovalServiceGrpc.MileageClaimApprovalServiceBlockingStub expensesMileageClaimApprovalServiceBlocking;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final TreasuryRepository repository;

    private final BankTransferApprovalServiceGrpc.BankTransferApprovalServiceBlockingStub treasuryBankTransferApprovalServiceBlocking;

    private final BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking;

    private final BankTransferWorkflowServiceGrpc.BankTransferWorkflowServiceBlockingStub treasuryBankTransferWorkflowServiceBlocking;

    public FxHedgeWorkflowApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            ChannelRegistry channels,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            MileageClaimApprovalServiceGrpc.MileageClaimApprovalServiceBlockingStub expensesMileageClaimApprovalServiceBlocking,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            TreasuryRepository repository,
            BankTransferApprovalServiceGrpc.BankTransferApprovalServiceBlockingStub treasuryBankTransferApprovalServiceBlocking,
            BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking,
            BankTransferWorkflowServiceGrpc.BankTransferWorkflowServiceBlockingStub treasuryBankTransferWorkflowServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.channels = channels;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.expensesMileageClaimApprovalServiceBlocking = expensesMileageClaimApprovalServiceBlocking;
        this.insuranceCoverageApprovalServiceBlocking = insuranceCoverageApprovalServiceBlocking;
        this.insuranceUnderwritingSyncServiceAsync = insuranceUnderwritingSyncServiceAsync;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.repository = repository;
        this.treasuryBankTransferApprovalServiceBlocking = treasuryBankTransferApprovalServiceBlocking;
        this.treasuryBankTransferReportingServiceBlocking = treasuryBankTransferReportingServiceBlocking;
        this.treasuryBankTransferWorkflowServiceBlocking = treasuryBankTransferWorkflowServiceBlocking;
    }

    /**
     * Starts a fx hedge.
     */
    public StartFxHedgeWorkflowResponse startFxHedgeWorkflow(StartFxHedgeWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartFxHedgeWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = ListBankTransferReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBankTransferReportsRequest.Filter.newBuilder()
                        .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.treasury.v1.BankTransferReportingService/ListBankTransferReports", outbound0);
        var reply0 = treasuryBankTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listBankTransferReports(command0.payload());

        var outbound1 = SignalBankTransferWorkflowRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setCard(BankTransferCardSettlement.newBuilder()
                                .setToken(switch (request.getFxHedge().getSettlementCase()) {
                                    case CARD -> request.getFxHedge().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = treasuryBankTransferWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalBankTransferWorkflow(outbound1);

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input2 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamUnderwritingChangesRequest.newBuilder()
                .setChange(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getFxHedge().getCounterparty().getDisplayName())
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFxHedge().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamUnderwritingChangesResponse.getDefaultInstance());
        var input3 = insuranceUnderwritingSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartFxHedgeWorkflow", request.getFxHedge().getFxHedgeId());
        return StartFxHedgeWorkflowResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getBankTransfer().getAudit().getCreatedAt())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setNumber(request.getFxHedge().getNumber())
                        .build())
                .setOutcome(StartFxHedgeWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances a fx hedge.
     */
    public AdvanceFxHedgeWorkflowResponse advanceFxHedgeWorkflow(AdvanceFxHedgeWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceFxHedgeWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = PreviewPreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getFxHedge().getAudit().getCreatedBy())
                                .setEtag(request.getFxHedge().getAudit().getEtag())
                                .setUpdatedAt(request.getFxHedge().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            PreviewPreferenceResponse fetch(PreviewPreferenceRequest input) {
                return customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPreference(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ListAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPreference().getAudit().getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(reply0.getPreference().getOwner().getUserId())
                        .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEvents(value));
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceFxHedgeWorkflow", request.getFxHedge().getFxHedgeId());
        return AdvanceFxHedgeWorkflowResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setFxHedgeId(request.getFxHedgeId())
                        .putAllMetadata(request.getFxHedge().getMetadataMap())
                        .build())
                .setOutcome(AdvanceFxHedgeWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Cancels a fx hedge.
     */
    public CancelFxHedgeWorkflowResponse cancelFxHedgeWorkflow(CancelFxHedgeWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelFxHedgeWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = (CompareCustomerSegmentOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getFxHedge().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setDetails(CompareCustomerSegmentOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getFxHedge().getBillingAddress().getCountryCode())
                                .setLocality(request.getFxHedge().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCustomerSegmentOptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelFxHedgeWorkflow", request.getFxHedge().getFxHedgeId());
        return CancelFxHedgeWorkflowResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCustomerSegment().getAudit().getUpdatedAt())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .addLines(FxHedgeLine.newBuilder()
                                .setAmount(reply0.getCustomerSegment().getDiscount().getAmount())
                                .build())
                        .setTotal(request.getFxHedge().getTotal())
                        .build())
                .build();
    }

    /**
     * Retrys a fx hedge.
     */
    public RetryFxHedgeWorkflowResponse retryFxHedgeWorkflow(RetryFxHedgeWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryFxHedgeWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = (CompareHouseholdOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(request.getFxHedge().getAudit().getEtag())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setPostalCode(request.getFxHedge().getBillingAddress().getPostalCode())
                                                .setRecipient(request.getFxHedge().getBillingAddress().getRecipient())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareHouseholdOptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryFxHedgeWorkflow", request.getFxHedge().getFxHedgeId());
        return RetryFxHedgeWorkflowResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setFxHedgeId(request.getFxHedgeId())
                        .putAllMetadata(request.getFxHedge().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Pauses a fx hedge.
     */
    public PauseFxHedgeWorkflowResponse pauseFxHedgeWorkflow(PauseFxHedgeWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseFxHedgeWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var builder0 = QuoteHouseholdRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setHousehold(Household.newBuilder()
                .addChildren(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getDetails().getNotify().getEmail())
                                        .build())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteHousehold(outbound0);

        var outbound1 = EscalateMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBank(MileageClaimBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getFxHedge().getSettlementCase()) {
                                    case BANK -> request.getFxHedge().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getHousehold().getLocation().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply1 = expensesMileageClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateMileageClaim(outbound1);

        var outbound2 = SubmitBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getMileageClaim().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> treasuryBankTransferApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitBankTransfer(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseFxHedgeWorkflow", request.getFxHedge().getFxHedgeId());
        return PauseFxHedgeWorkflowResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getHousehold().getLocation().getCountryCode())
                                .setLocality(reply1.getMileageClaim().getBillingAddress().getLocality())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply0.getHousehold().getManager().getDisplayName())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setIssuedAt(reply2.getBankTransfer().getIssuedAt())
                        .setNotes(request.getFxHedge().getNotes())
                        .setTotal(reply2.getBankTransfer().getTotal())
                        .build())
                .setOutcome(PauseFxHedgeWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Resumes a fx hedge.
     */
    public ResumeFxHedgeWorkflowResponse resumeFxHedgeWorkflow(ResumeFxHedgeWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeFxHedgeWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = SubmitMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .setCard(MileageClaimCardSettlement.newBuilder()
                                .setLast4(switch (request.getFxHedge().getSettlementCase()) {
                                    case CARD -> request.getFxHedge().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = expensesMileageClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitMileageClaim(outbound0);

        var outbound1 = CloneLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setCard(LedgerAccountCardFunding.newBuilder()
                                .setLast4(switch (request.getFxHedge().getSettlementCase()) {
                                    case CARD -> request.getFxHedge().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setDisplayName(reply0.getMileageClaim().getCounterparty().getDisplayName())
                                .setNumber(reply0.getMileageClaim().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var reply1 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLedgerAccount(outbound1);

        var outbound2 = BeanCopies.copy(ExplainCustomerCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getLedgerAccount().getAudit().getCreatedAt())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setLocality(request.getFxHedge().getBillingAddress().getLocality())
                                .setPostalCode(request.getFxHedge().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setDetails(ExplainCustomerCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getLedgerAccount().getContact().getName())
                                .build())
                        .build())
                .build(), ExplainCustomerCalculationRequest.class);
        var reply2 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerCalculation(outbound2);

        var outbound3 = ApproveMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getFxHedge().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply3 = expensesMileageClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveMileageClaim(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeFxHedgeWorkflow", request.getFxHedge().getFxHedgeId());
        return ResumeFxHedgeWorkflowResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getCustomer().getPrimaryAddress().getPostalCode())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply1.getLedgerAccount().getCurrencyCode())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setNotes(request.getFxHedge().getNotes())
                        .build())
                .setOutcome(ResumeFxHedgeWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setReferenceId(reply3.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Returns a fx hedge.
     */
    public InspectFxHedgeWorkflowResponse inspectFxHedgeWorkflow(InspectFxHedgeWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectFxHedgeWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "InspectFxHedgeWorkflow");

        var outbound0 = RecallCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallCoverage(outbound0);
        return InspectFxHedgeWorkflowResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply0.getCoverage().getOwner().getDisplayName())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists fx hedges a page at a time.
     */
    public ListFxHedgeWorkflowsResponse listFxHedgeWorkflows(ListFxHedgeWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListFxHedgeWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getContext().getTraceId(), "ListFxHedgeWorkflows");

        var builder0 = CommentBankTransferApprovalRequest.newBuilder();
        builder0.setBankTransfer(BankTransfer.newBuilder()
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        builder0.setDetails(CommentBankTransferApprovalRequest.Details.newBuilder()
                .setApprover(UserRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = treasuryBankTransferApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentBankTransferApproval(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListFxHedgeWorkflows", request.getPage().getPageToken());
        return ListFxHedgeWorkflowsResponse.newBuilder()
                .addFxHedges(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBankTransfer().getAudit().getEtag())
                                .build())
                        .build())
                .build();
    }

    /**
     * Signals a fx hedge.
     */
    public SignalFxHedgeWorkflowResponse signalFxHedgeWorkflow(SignalFxHedgeWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalFxHedgeWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = WatchLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotify().getEmail())
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build();
        var stream0 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchLedgerAccounts(outbound0);
        var reply0 = WatchLedgerAccountsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = FindHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFxHedge().getAudit().getUpdatedAt())
                        .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.customers.v1.HouseholdQueryService");
        var selected1 = HouseholdQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.findHouseholds(outbound1);

        var outbound2 = RecalculatePreferencesRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getFxHedge().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        Function<RecalculatePreferencesRequest, RecalculatePreferencesResponse> operation2 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculatePreferences;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = FindNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFxHedge().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(reply2.getPreference().getOwner().getUserId())
                        .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findNotifications(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalFxHedgeWorkflow", request.getFxHedge().getFxHedgeId());
        return SignalFxHedgeWorkflowResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply2.getPreference().getOwner().getDisplayName())
                                .build())
                        .setDueAt(request.getFxHedge().getDueAt())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setOutcome(SignalFxHedgeWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
