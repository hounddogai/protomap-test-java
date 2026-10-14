package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyBlockAction;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.CreateAccessPolicyRequest;
import com.acme.authorization.v1.CreateAccessPolicyResponse;
import com.acme.authorization.v1.PreviewAccessPolicyRequest;
import com.acme.authorization.v1.PreviewAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CommentConsentRecordApprovalRequest;
import com.acme.customers.v1.CommentConsentRecordApprovalResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordApprovalServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerLifecycleServiceGrpc;
import com.acme.customers.v1.CustomerPassport;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentSyncServiceGrpc;
import com.acme.customers.v1.CustomerWorkflowServiceGrpc;
import com.acme.customers.v1.PauseCustomerWorkflowRequest;
import com.acme.customers.v1.PauseCustomerWorkflowResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceImportServiceGrpc;
import com.acme.customers.v1.PushCustomerSegmentRequest;
import com.acme.customers.v1.PushCustomerSegmentResponse;
import com.acme.customers.v1.ScheduleCustomerExpiryRequest;
import com.acme.customers.v1.ScheduleCustomerExpiryResponse;
import com.acme.customers.v1.ValidatePreferenceImportRequest;
import com.acme.customers.v1.ValidatePreferenceImportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceNotificationServiceGrpc;
import com.acme.financeportal.v1.GetCloseChecklistPreferencesRequest;
import com.acme.financeportal.v1.GetCloseChecklistPreferencesResponse;
import com.acme.financeportal.v1.ListCloseChecklistSubscriptionsRequest;
import com.acme.financeportal.v1.ListCloseChecklistSubscriptionsResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesResponse;
import com.acme.financeportal.v1.NotifyCloseChecklistWatchersRequest;
import com.acme.financeportal.v1.NotifyCloseChecklistWatchersResponse;
import com.acme.financeportal.v1.SendCloseChecklistTestNoticeRequest;
import com.acme.financeportal.v1.SendCloseChecklistTestNoticeResponse;
import com.acme.financeportal.v1.StreamCloseChecklistNoticesRequest;
import com.acme.financeportal.v1.StreamCloseChecklistNoticesResponse;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesRequest;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesResponse;
import com.acme.financeportal.v1.SubscribeCloseChecklistUpdatesRequest;
import com.acme.financeportal.v1.SubscribeCloseChecklistUpdatesResponse;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.UnsubscribeCloseChecklistUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeCloseChecklistUpdatesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.suppliers.v1.ApproveSupplierRatingRequest;
import com.acme.suppliers.v1.ApproveSupplierRatingResponse;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsResponse;
import com.acme.suppliers.v1.SupplierContractWorkflowServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRatingLine;
import com.acme.suppliers.v1.SupplierRatingLineTax;
import com.acme.treasury.v1.CommentLiquidityForecastApprovalRequest;
import com.acme.treasury.v1.CommentLiquidityForecastApprovalResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastApprovalServiceGrpc;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Tells the people who follow a close checklist what changed. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class CloseChecklistNotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(CloseChecklistNotificationApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ConsentRecordApprovalServiceGrpc.ConsentRecordApprovalServiceBlockingStub customersConsentRecordApprovalServiceBlocking;

    private final CustomerLifecycleServiceGrpc.CustomerLifecycleServiceBlockingStub customersCustomerLifecycleServiceBlocking;

    private final CustomerSegmentSyncServiceGrpc.CustomerSegmentSyncServiceBlockingStub customersCustomerSegmentSyncServiceBlocking;

    private final CustomerWorkflowServiceGrpc.CustomerWorkflowServiceBlockingStub customersCustomerWorkflowServiceBlocking;

    private final PreferenceImportServiceGrpc.PreferenceImportServiceBlockingStub customersPreferenceImportServiceBlocking;

    private final FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking;

    private final FinanceWorkspaceNotificationServiceGrpc.FinanceWorkspaceNotificationServiceBlockingStub financeportalFinanceWorkspaceNotificationServiceBlocking;

    private final FinanceportalRepository repository;

    private final SupplierContractWorkflowServiceGrpc.SupplierContractWorkflowServiceFutureStub suppliersSupplierContractWorkflowServiceFuture;

    private final SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking;

    private final LiquidityForecastApprovalServiceGrpc.LiquidityForecastApprovalServiceBlockingStub treasuryLiquidityForecastApprovalServiceBlocking;

    private final VenueServiceGrpc.VenueServiceFutureStub venuesVenueServiceFuture;

    public CloseChecklistNotificationApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            ChannelRegistry channels,
            ConsentRecordApprovalServiceGrpc.ConsentRecordApprovalServiceBlockingStub customersConsentRecordApprovalServiceBlocking,
            CustomerLifecycleServiceGrpc.CustomerLifecycleServiceBlockingStub customersCustomerLifecycleServiceBlocking,
            CustomerSegmentSyncServiceGrpc.CustomerSegmentSyncServiceBlockingStub customersCustomerSegmentSyncServiceBlocking,
            CustomerWorkflowServiceGrpc.CustomerWorkflowServiceBlockingStub customersCustomerWorkflowServiceBlocking,
            PreferenceImportServiceGrpc.PreferenceImportServiceBlockingStub customersPreferenceImportServiceBlocking,
            FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking,
            FinanceWorkspaceNotificationServiceGrpc.FinanceWorkspaceNotificationServiceBlockingStub financeportalFinanceWorkspaceNotificationServiceBlocking,
            FinanceportalRepository repository,
            SupplierContractWorkflowServiceGrpc.SupplierContractWorkflowServiceFutureStub suppliersSupplierContractWorkflowServiceFuture,
            SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking,
            LiquidityForecastApprovalServiceGrpc.LiquidityForecastApprovalServiceBlockingStub treasuryLiquidityForecastApprovalServiceBlocking,
            VenueServiceGrpc.VenueServiceFutureStub venuesVenueServiceFuture) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.channels = channels;
        this.customersConsentRecordApprovalServiceBlocking = customersConsentRecordApprovalServiceBlocking;
        this.customersCustomerLifecycleServiceBlocking = customersCustomerLifecycleServiceBlocking;
        this.customersCustomerSegmentSyncServiceBlocking = customersCustomerSegmentSyncServiceBlocking;
        this.customersCustomerWorkflowServiceBlocking = customersCustomerWorkflowServiceBlocking;
        this.customersPreferenceImportServiceBlocking = customersPreferenceImportServiceBlocking;
        this.financeportalFinanceWorkspaceIntegrationServiceBlocking = financeportalFinanceWorkspaceIntegrationServiceBlocking;
        this.financeportalFinanceWorkspaceNotificationServiceBlocking = financeportalFinanceWorkspaceNotificationServiceBlocking;
        this.repository = repository;
        this.suppliersSupplierContractWorkflowServiceFuture = suppliersSupplierContractWorkflowServiceFuture;
        this.suppliersSupplierRatingApprovalServiceBlocking = suppliersSupplierRatingApprovalServiceBlocking;
        this.treasuryLiquidityForecastApprovalServiceBlocking = treasuryLiquidityForecastApprovalServiceBlocking;
        this.venuesVenueServiceFuture = venuesVenueServiceFuture;
    }

    /**
     * Subscribes a close checklist.
     */
    public SubscribeCloseChecklistUpdatesResponse subscribeCloseChecklistUpdates(SubscribeCloseChecklistUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubscribeCloseChecklistUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "SubscribeCloseChecklistUpdates");

        var outbound0 = SubscribeFinanceWorkspaceUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeFinanceWorkspaceUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.financeportal.v1.FinanceWorkspaceNotificationService");
        var serviceName0 = "acme.financeportal.v1" + "." + "FinanceWorkspaceNotificationService";
        var methodName0 = "Sub" + "scribeFinanceWorkspaceUpdates";
        var descriptor0 = MethodDescriptor.<SubscribeFinanceWorkspaceUpdatesRequest, SubscribeFinanceWorkspaceUpdatesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SubscribeFinanceWorkspaceUpdatesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SubscribeFinanceWorkspaceUpdatesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ScheduleCustomerExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getFinanceWorkspace().getRequester().getName())
                                .build())
                        .build())
                .setDetails(ScheduleCustomerExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getRecipient().getName())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<ScheduleCustomerExpiryRequest, ScheduleCustomerExpiryResponse>() {
            @Override
            public ScheduleCustomerExpiryResponse apply(ScheduleCustomerExpiryRequest input) {
                return customersCustomerLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleCustomerExpiry(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var builder2 = ListFinanceWorkspaceSourcesRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setFilter(ListFinanceWorkspaceSourcesRequest.Filter.newBuilder()
                .setCreatedAfter(reply0.getOutcome().getEffectiveAt())
                .setOwnerId(reply1.getOutcome().getReferenceId())
                .setRegion(reply1.getCustomer().getPrimaryAddress().getRegion())
                .build());
        var outbound2 = builder2.build();
        var reply2 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFinanceWorkspaceSources(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubscribeCloseChecklistUpdates", request.getDetails().getRecipient().getEmail());
        return SubscribeCloseChecklistUpdatesResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getFinanceWorkspace().getAudit().getCreatedBy())
                                .build())
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getFinanceWorkspace().getAssignee().getEmail())
                                .build())
                        .setNumber(reply1.getCustomer().getNationalId().getNumber())
                        .build())
                .setOutcome(SubscribeCloseChecklistUpdatesResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Unsubscribes a close checklist.
     */
    public UnsubscribeCloseChecklistUpdatesResponse unsubscribeCloseChecklistUpdates(UnsubscribeCloseChecklistUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnsubscribeCloseChecklistUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "UnsubscribeCloseChecklistUpdates");

        var outbound0 = ListSupplierContractWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSupplierContractWorkflowsRequest.Filter.newBuilder()
                        .putAllLabels(request.getDetails().getVariablesMap())
                        .build())
                .build();
        var pending0 = suppliersSupplierContractWorkflowServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listSupplierContractWorkflows(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = PushCustomerSegmentRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setBlock(CustomerSegmentBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setDetails(PushCustomerSegmentRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.customers.v1.CustomerSegmentSyncService/PushCustomerSegment", outbound1);
        var reply1 = customersCustomerSegmentSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushCustomerSegment(command1.payload());

        var outbound2 = ValidatePreferenceImportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePreferenceImportRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getRecipient().getName())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getCustomerSegment().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> customersPreferenceImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePreferenceImport(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPreferredChannel()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnsubscribeCloseChecklistUpdates", request.getDetails().getRecipient().getPreferredChannel());
        return UnsubscribeCloseChecklistUpdatesResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply2.getPreference().getOwner().getDisplayName())
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .setOutcome(UnsubscribeCloseChecklistUpdatesResponse.Outcome.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Notifys a close checklist.
     */
    public NotifyCloseChecklistWatchersResponse notifyCloseChecklistWatchers(NotifyCloseChecklistWatchersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NotifyCloseChecklistWatchers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "NotifyCloseChecklistWatchers");

        var outbound0 = JsonCodec.roundTrip(PreviewAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setBlock(AccessPolicyBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAccessPolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getRecipient().getName())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .build())
                        .build())
                .build(), PreviewAccessPolicyRequest.class);
        var reply0 = authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAccessPolicy(outbound0);

        var outbound1 = SearchVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getAccessPolicy().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getAccessPolicy().getAccessPolicyId())
                        .build())
                .build();
        var pending1 = venuesVenueServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).searchVenues(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = DisconnectFinanceWorkspaceSourceRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(DisconnectFinanceWorkspaceSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(reply0.getResult().getMessage())
                        .setReceivedAt(reply0.getAccessPolicy().getAudit().getUpdatedAt())
                        .build())
                .setReason(reply0.getAccessPolicy().getBlock().getReason())
                .build();
        var adapter2 = new Function<DisconnectFinanceWorkspaceSourceRequest, DisconnectFinanceWorkspaceSourceResponse>() {
            @Override
            public DisconnectFinanceWorkspaceSourceResponse apply(DisconnectFinanceWorkspaceSourceRequest input) {
                return financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).disconnectFinanceWorkspaceSource(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var outbound3 = ApproveSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .addLines(SupplierRatingLine.newBuilder()
                                .setTax(SupplierRatingLineTax.newBuilder()
                                        .setAmount(reply0.getAccessPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> suppliersSupplierRatingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveSupplierRating(value));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPreferredChannel()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NotifyCloseChecklistWatchers", request.getDetails().getRecipient().getPreferredChannel());
        return NotifyCloseChecklistWatchersResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply2.getFinanceWorkspace().getAssignee().getDisplayName())
                                .setEmail(reply2.getFinanceWorkspace().getRequester().getEmail())
                                .build())
                        .setNotes(reply3.getSupplierRating().getNotes())
                        .build())
                .setOutcome(NotifyCloseChecklistWatchersResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists close checklists a page at a time.
     */
    public ListCloseChecklistSubscriptionsResponse listCloseChecklistSubscriptions(ListCloseChecklistSubscriptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListCloseChecklistSubscriptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getContext().getTraceId(), "ListCloseChecklistSubscriptions");

        var outbound0 = SubscribeAuditEventUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeAuditEventUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<SubscribeAuditEventUpdatesRequest, SubscribeAuditEventUpdatesResponse>() {
            @Override
            public SubscribeAuditEventUpdatesResponse apply(SubscribeAuditEventUpdatesRequest input) {
                return auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeAuditEventUpdates(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = PauseCustomerWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPassport(CustomerPassport.newBuilder()
                                .setNumber(reply0.getAuditEvent().getSubject().getNumber())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.customers.v1.CustomerWorkflowService/PauseCustomerWorkflow", outbound1);
        var reply1 = customersCustomerWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseCustomerWorkflow(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListCloseChecklistSubscriptions", request.getPage().getPageToken());
        return ListCloseChecklistSubscriptionsResponse.newBuilder()
                .addCloseChecklists(CloseChecklist.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Sends a close checklist.
     */
    public SendCloseChecklistTestNoticeResponse sendCloseChecklistTestNotice(SendCloseChecklistTestNoticeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SendCloseChecklistTestNotice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "SendCloseChecklistTestNotice");

        var outbound0 = AcknowledgeFinanceWorkspaceEventRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(AcknowledgeFinanceWorkspaceEventRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getCloseChecklistId())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).acknowledgeFinanceWorkspaceEvent(outbound0);

        var outbound1 = CommentLiquidityForecastApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentLiquidityForecastApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(reply0.getFinanceWorkspace().getAssignee().getDisplayName())
                                .setEmail(switch (reply0.getFinanceWorkspace().getOriginCase()) {
                                    case CUSTOMER -> reply0.getFinanceWorkspace().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getFinanceWorkspace().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = treasuryLiquidityForecastApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentLiquidityForecastApproval(outbound1);

        var builder2 = CommentConsentRecordApprovalRequest.newBuilder();
        builder2.setConsentRecord(ConsentRecord.newBuilder()
                .setDetail(Any.pack(request.getContext()))
                .build());
        builder2.setContext(request.getContext());
        builder2.setDetails(CommentConsentRecordApprovalRequest.Details.newBuilder()
                .setApprover(UserRef.newBuilder()
                        .setDisplayName(reply0.getFinanceWorkspace().getAssignee().getDisplayName())
                        .setEmail(reply0.getFinanceWorkspace().getCustomer().getEmail())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = customersConsentRecordApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentConsentRecordApproval(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SendCloseChecklistTestNotice", request.getDetails().getRecipient().getPhone());
        return SendCloseChecklistTestNoticeResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .setOutcome(SendCloseChecklistTestNoticeResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a close checklist.
     */
    public GetCloseChecklistPreferencesResponse getCloseChecklistPreferences(GetCloseChecklistPreferencesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCloseChecklistPreferences")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "GetCloseChecklistPreferences");
        return GetCloseChecklistPreferencesResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .build())
                .build();
    }

    /**
     * Streams the changes of close checklists as they happen.
     */
    public StreamCloseChecklistNoticesResponse streamCloseChecklistNotices(StreamCloseChecklistNoticesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamCloseChecklistNotices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getContext().getTraceId(), "StreamCloseChecklistNotices");

        var outbound0 = StreamFinanceWorkspaceNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamFinanceWorkspaceNoticesRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPhone(request.getSubscriber().getPhone())
                        .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                        .build())
                .build();
        var stream0 = financeportalFinanceWorkspaceNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamFinanceWorkspaceNotices(outbound0);
        var reply0 = StreamFinanceWorkspaceNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = AcknowledgeFinanceWorkspaceEventRequest.newBuilder()
                .putAllAnnotations(request.getFilter().getLabelsMap())
                .setContext(request.getContext())
                .setDetails(AcknowledgeFinanceWorkspaceEventRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getFilter().getOwnerId())
                        .setReceivedAt(request.getSince())
                        .build())
                .build();
        var reply1 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).acknowledgeFinanceWorkspaceEvent(outbound1);

        var outbound2 = FindAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAuditEvents(value)).orElseThrow();

        var outbound3 = CreateAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getFinanceWorkspace().getAudit().getCreatedAt())
                                .setEtag(reply1.getFinanceWorkspace().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply1.getFinanceWorkspace().getAssignee().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<CreateAccessPolicyRequest, CreateAccessPolicyResponse> port3 = RpcPort.adapt(authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createAccessPolicy);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getSubscriber().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamCloseChecklistNotices", request.getSubscriber().getEmail());
        return StreamCloseChecklistNoticesResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply3.getAccessPolicy().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getFinanceWorkspace().getRequester().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }
}
