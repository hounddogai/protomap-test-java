package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.billing.v1.CreditNoteWorkflowServiceGrpc;
import com.acme.billing.v1.RetryCreditNoteWorkflowRequest;
import com.acme.billing.v1.RetryCreditNoteWorkflowResponse;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.customers.v1.ApproveHouseholdRequest;
import com.acme.customers.v1.ApproveHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdApprovalServiceGrpc;
import com.acme.edgegateway.v1.CloneRouteBindingRequest;
import com.acme.edgegateway.v1.CloneRouteBindingResponse;
import com.acme.edgegateway.v1.RouteBinding;
import com.acme.edgegateway.v1.RouteBindingServiceGrpc;
import com.acme.fanportal.v1.CountFanSessionsRequest;
import com.acme.fanportal.v1.CountFanSessionsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceSla;
import com.acme.financeportal.v1.GetFinanceWorkspacePreferencesRequest;
import com.acme.financeportal.v1.GetFinanceWorkspacePreferencesResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsResponse;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersRequest;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersResponse;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesRequest;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesResponse;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.SendFinanceWorkspaceTestNoticeRequest;
import com.acme.financeportal.v1.SendFinanceWorkspaceTestNoticeResponse;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesRequest;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesResponse;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.identity.v1.ExplainUserDecisionRequest;
import com.acme.identity.v1.ExplainUserDecisionResponse;
import com.acme.identity.v1.NormalizeUserRequest;
import com.acme.identity.v1.NormalizeUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionWorkflowServiceGrpc;
import com.acme.treasury.v1.ResumeCashPositionWorkflowRequest;
import com.acme.treasury.v1.ResumeCashPositionWorkflowResponse;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Tells the people who follow a finance workspace what changed. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class FinanceWorkspaceNotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(FinanceWorkspaceNotificationApplication.class);

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final CreditNoteWorkflowServiceGrpc.CreditNoteWorkflowServiceBlockingStub billingCreditNoteWorkflowServiceBlocking;

    private final VarianceImportServiceGrpc.VarianceImportServiceStub budgetingVarianceImportServiceAsync;

    private final HouseholdApprovalServiceGrpc.HouseholdApprovalServiceBlockingStub customersHouseholdApprovalServiceBlocking;

    private final RouteBindingServiceGrpc.RouteBindingServiceBlockingStub edgegatewayRouteBindingServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceStub financeportalApprovalInboxIntegrationServiceAsync;

    private final FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final FinanceWorkspaceNotificationRequestMapper mapper;

    private final FinanceportalRepository repository;

    private final CashPositionWorkflowServiceGrpc.CashPositionWorkflowServiceBlockingStub treasuryCashPositionWorkflowServiceBlocking;

    public FinanceWorkspaceNotificationApplication(
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            CreditNoteWorkflowServiceGrpc.CreditNoteWorkflowServiceBlockingStub billingCreditNoteWorkflowServiceBlocking,
            VarianceImportServiceGrpc.VarianceImportServiceStub budgetingVarianceImportServiceAsync,
            HouseholdApprovalServiceGrpc.HouseholdApprovalServiceBlockingStub customersHouseholdApprovalServiceBlocking,
            RouteBindingServiceGrpc.RouteBindingServiceBlockingStub edgegatewayRouteBindingServiceBlocking,
            ApplicationEventPublisher events,
            ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceStub financeportalApprovalInboxIntegrationServiceAsync,
            FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            FinanceWorkspaceNotificationRequestMapper mapper,
            FinanceportalRepository repository,
            CashPositionWorkflowServiceGrpc.CashPositionWorkflowServiceBlockingStub treasuryCashPositionWorkflowServiceBlocking) {
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.billingCreditNoteWorkflowServiceBlocking = billingCreditNoteWorkflowServiceBlocking;
        this.budgetingVarianceImportServiceAsync = budgetingVarianceImportServiceAsync;
        this.customersHouseholdApprovalServiceBlocking = customersHouseholdApprovalServiceBlocking;
        this.edgegatewayRouteBindingServiceBlocking = edgegatewayRouteBindingServiceBlocking;
        this.events = events;
        this.financeportalApprovalInboxIntegrationServiceAsync = financeportalApprovalInboxIntegrationServiceAsync;
        this.financeportalFinanceWorkspaceIntegrationServiceBlocking = financeportalFinanceWorkspaceIntegrationServiceBlocking;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.treasuryCashPositionWorkflowServiceBlocking = treasuryCashPositionWorkflowServiceBlocking;
    }

    public record ListFinanceWorkspaceSubscriptionsStage0Requested(CountFanSessionsRequest request, CompletableFuture<CountFanSessionsResponse> completion, io.grpc.Context context) {}

    /**
     * Subscribes a finance workspace.
     */
    public SubscribeFinanceWorkspaceUpdatesResponse subscribeFinanceWorkspaceUpdates(SubscribeFinanceWorkspaceUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubscribeFinanceWorkspaceUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "SubscribeFinanceWorkspaceUpdates");

        var outbound0 = mapper.subscribeFinanceWorkspaceUpdatesStage0(request);
        var reply0 = auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRetentionPolicy(outbound0);

        var builder1 = ExplainUserDecisionRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setUser(User.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getRecipient().getEmail())
                        .setName(request.getDetails().getRecipient().getName())
                        .build())
                .setRelationshipManager(UserRef.newBuilder()
                        .setUserId(reply0.getRetentionPolicy().getOwner().getUserId())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainUserDecision(outbound1);

        var outbound2 = ImportVariancesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(ImportVariancesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getRecipient())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(ImportVariancesResponse.getDefaultInstance());
        var input2 = budgetingVarianceImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importVariances(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = RelayApprovalInboxMessagesRequest.newBuilder()
                .setChange(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setDueAt(request.getContext().getDeadline())
                        .setIssuedAt(request.getContext().getDeadline())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(RelayApprovalInboxMessagesResponse.getDefaultInstance());
        var input3 = financeportalApprovalInboxIntegrationServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).relayApprovalInboxMessages(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubscribeFinanceWorkspaceUpdates", request.getDetails().getTemplateId());
        return SubscribeFinanceWorkspaceUpdatesResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply1.getUser().getRelationshipManager().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .build();
    }

    /**
     * Unsubscribes a finance workspace.
     */
    public UnsubscribeFinanceWorkspaceUpdatesResponse unsubscribeFinanceWorkspaceUpdates(UnsubscribeFinanceWorkspaceUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnsubscribeFinanceWorkspaceUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "UnsubscribeFinanceWorkspaceUpdates");

        var outbound0 = BeanCopies.copy(AnnotateRetentionPolicyVersionRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(AnnotateRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), AnnotateRetentionPolicyVersionRequest.class);
        var reply0 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateRetentionPolicyVersion(outbound0);

        var outbound1 = AcknowledgeFinanceWorkspaceEventRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AcknowledgeFinanceWorkspaceEventRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                .setReason(reply0.getRetentionPolicy().getBlock().getReason())
                .build();
        final class Stage1Adapter {
            AcknowledgeFinanceWorkspaceEventResponse fetch(AcknowledgeFinanceWorkspaceEventRequest input) {
                return financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).acknowledgeFinanceWorkspaceEvent(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPreferredChannel()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnsubscribeFinanceWorkspaceUpdates", request.getDetails().getRecipient().getPreferredChannel());
        return UnsubscribeFinanceWorkspaceUpdatesResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getRetentionPolicy().getOwner().getDisplayName())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setPriority(reply1.getFinanceWorkspace().getPriority())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .build())
                        .build())
                .setOutcome(UnsubscribeFinanceWorkspaceUpdatesResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Notifys a finance workspace.
     */
    public NotifyFinanceWorkspaceWatchersResponse notifyFinanceWorkspaceWatchers(NotifyFinanceWorkspaceWatchersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NotifyFinanceWorkspaceWatchers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "NotifyFinanceWorkspaceWatchers");

        var outbound0 = BeanCopies.copy(AcknowledgeFinanceWorkspaceEventRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AcknowledgeFinanceWorkspaceEventRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build(), AcknowledgeFinanceWorkspaceEventRequest.class);
        var reply0 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).acknowledgeFinanceWorkspaceEvent(outbound0);

        var outbound1 = NormalizeUserRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeUserRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getFinanceWorkspace().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.identity.v1.UserValidationService/NormalizeUser", outbound1);
        var reply1 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeUser(command1.payload());

        var outbound2 = ApproveHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply0.getFinanceWorkspace().getRequester().getEmail())
                                                .setUserId(reply1.getUser().getRelationshipManager().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = customersHouseholdApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveHousehold(outbound2);

        var outbound3 = RevertFiscalPeriodRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertFiscalPeriodRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .addParticipants(FiscalPeriodParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getFinanceWorkspace().getCustomer().getEmail())
                                        .setName(request.getDetails().getRecipient().getName())
                                        .setPhone(reply1.getUser().getContact().getPhone())
                                        .setPreferredChannel(reply1.getUser().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertFiscalPeriod(value));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NotifyFinanceWorkspaceWatchers", request.getDetails().getRecipient().getEmail());
        return NotifyFinanceWorkspaceWatchersResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getRecipient().getName())
                                .build())
                        .build())
                .setOutcome(NotifyFinanceWorkspaceWatchersResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists finance workspaces a page at a time.
     */
    public ListFinanceWorkspaceSubscriptionsResponse listFinanceWorkspaceSubscriptions(ListFinanceWorkspaceSubscriptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListFinanceWorkspaceSubscriptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "ListFinanceWorkspaceSubscriptions");

        var outbound0 = CountFanSessionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountFanSessionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CountFanSessionsResponse>();
        events.publishEvent(new ListFinanceWorkspaceSubscriptionsStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListFinanceWorkspaceSubscriptions", request.getPage().getPageToken());
        return ListFinanceWorkspaceSubscriptionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Sends a finance workspace.
     */
    public SendFinanceWorkspaceTestNoticeResponse sendFinanceWorkspaceTestNotice(SendFinanceWorkspaceTestNoticeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SendFinanceWorkspaceTestNotice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "SendFinanceWorkspaceTestNotice");

        var outbound0 = AcknowledgeFinanceWorkspaceEventRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(AcknowledgeFinanceWorkspaceEventRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .build();
        RpcPort<AcknowledgeFinanceWorkspaceEventRequest, AcknowledgeFinanceWorkspaceEventResponse> port0 = RpcPort.adapt(financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::acknowledgeFinanceWorkspaceEvent);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = RetryCreditNoteWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryCreditNoteWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(switch (reply0.getFinanceWorkspace().getOriginCase()) {
                                    case CUSTOMER -> reply0.getFinanceWorkspace().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = billingCreditNoteWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryCreditNoteWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SendFinanceWorkspaceTestNotice", request.getDetails().getRecipient().getEmail());
        return SendFinanceWorkspaceTestNoticeResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getFinanceWorkspace().getAssignee().getEmail())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .build())
                        .build())
                .setOutcome(SendFinanceWorkspaceTestNoticeResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Returns a finance workspace.
     */
    public GetFinanceWorkspacePreferencesResponse getFinanceWorkspacePreferences(GetFinanceWorkspacePreferencesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFinanceWorkspacePreferences")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "GetFinanceWorkspacePreferences");

        var outbound0 = ReplayFinanceWorkspaceEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReplayFinanceWorkspaceEventsRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getContext().getActor().getUserId())
                        .build())
                .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                .build();
        var reply0 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).replayFinanceWorkspaceEvents(outbound0);
        return GetFinanceWorkspacePreferencesResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setSla(FinanceWorkspaceSla.newBuilder()
                                .setDueAt(reply0.getFinanceWorkspace().getSla().getDueAt())
                                .setPausedFor(reply0.getFinanceWorkspace().getSla().getPausedFor())
                                .build())
                        .build())
                .build();
    }

    /**
     * Streams the changes of finance workspaces as they happen.
     */
    public StreamFinanceWorkspaceNoticesResponse streamFinanceWorkspaceNotices(StreamFinanceWorkspaceNoticesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamFinanceWorkspaceNotices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "StreamFinanceWorkspaceNotices");

        var outbound0 = BeanCopies.copy(CloneRouteBindingRequest.newBuilder()
                .setContext(request.getContext())
                .setRouteBinding(RouteBinding.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), CloneRouteBindingRequest.class);
        var reply0 = edgegatewayRouteBindingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneRouteBinding(outbound0);

        var outbound1 = ResumeCashPositionWorkflowRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRouteBinding().getAudit().getEtag())
                                .setUpdatedAt(reply0.getRouteBinding().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            ResumeCashPositionWorkflowResponse fetch(ResumeCashPositionWorkflowRequest input) {
                return treasuryCashPositionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeCashPositionWorkflow(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = ListAuditTrailVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailVersions(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getRegion()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamFinanceWorkspaceNotices", request.getFilter().getRegion());
        return StreamFinanceWorkspaceNoticesResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getRouteBinding().getOwner().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setName(request.getSubscriber().getName())
                                .setPreferredChannel(reply1.getCashPosition().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
