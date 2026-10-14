package com.acme.finance.domains.financeportal;

import com.acme.analytics.v1.AnalyticsEventQueryServiceGrpc;
import com.acme.analytics.v1.SummarizeAnalyticsEventsRequest;
import com.acme.analytics.v1.SummarizeAnalyticsEventsResponse;
import com.acme.budgeting.v1.CommentVarianceApprovalRequest;
import com.acme.budgeting.v1.CommentVarianceApprovalResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.compliance.v1.StartControlWorkflowRequest;
import com.acme.compliance.v1.StartControlWorkflowResponse;
import com.acme.customers.v1.GenerateHouseholdReportRequest;
import com.acme.customers.v1.GenerateHouseholdReportResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdReportingServiceGrpc;
import com.acme.customers.v1.HouseholdWorkflowServiceGrpc;
import com.acme.customers.v1.ListPreferenceConflictsRequest;
import com.acme.customers.v1.ListPreferenceConflictsResponse;
import com.acme.customers.v1.PreferenceImportServiceGrpc;
import com.acme.customers.v1.PreferenceSyncServiceGrpc;
import com.acme.customers.v1.RetryHouseholdWorkflowRequest;
import com.acme.customers.v1.RetryHouseholdWorkflowResponse;
import com.acme.customers.v1.ValidatePreferenceImportRequest;
import com.acme.customers.v1.ValidatePreferenceImportResponse;
import com.acme.expenses.v1.AdvanceReceiptWorkflowRequest;
import com.acme.expenses.v1.AdvanceReceiptWorkflowResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyBlockAction;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.financeportal.v1.AdvanceApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.AdvanceApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxNotificationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxQueryServiceGrpc;
import com.acme.financeportal.v1.BatchCreateFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BatchCreateFinanceWorkspacesResponse;
import com.acme.financeportal.v1.BatchGetFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BatchGetFinanceWorkspacesResponse;
import com.acme.financeportal.v1.CloneFinanceWorkspaceRequest;
import com.acme.financeportal.v1.CloneFinanceWorkspaceResponse;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.CountApprovalInboxesRequest;
import com.acme.financeportal.v1.CountApprovalInboxesResponse;
import com.acme.financeportal.v1.CreateFinanceWorkspaceRequest;
import com.acme.financeportal.v1.CreateFinanceWorkspaceResponse;
import com.acme.financeportal.v1.DeleteFinanceWorkspaceRequest;
import com.acme.financeportal.v1.DeleteFinanceWorkspaceResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceComment;
import com.acme.financeportal.v1.GetFinanceWorkspaceRequest;
import com.acme.financeportal.v1.GetFinanceWorkspaceResponse;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsRequest;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsResponse;
import com.acme.financeportal.v1.NotifyApprovalInboxWatchersRequest;
import com.acme.financeportal.v1.NotifyApprovalInboxWatchersResponse;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsRequest;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsResponse;
import com.acme.financeportal.v1.SearchFinanceWorkspacesRequest;
import com.acme.financeportal.v1.SearchFinanceWorkspacesResponse;
import com.acme.financeportal.v1.SuggestApprovalInboxesRequest;
import com.acme.financeportal.v1.SuggestApprovalInboxesResponse;
import com.acme.financeportal.v1.WatchFinanceWorkspacesRequest;
import com.acme.financeportal.v1.WatchFinanceWorkspacesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.notifications.v1.FindNotificationsRequest;
import com.acme.notifications.v1.FindNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.treasury.v1.ImportLiquidityForecastsRequest;
import com.acme.treasury.v1.ImportLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Manages finance workspaces: creates, reads, changes, and lists them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class FinanceWorkspaceApplication {
    private static final Logger log = LoggerFactory.getLogger(FinanceWorkspaceApplication.class);

    private final AnalyticsEventQueryServiceGrpc.AnalyticsEventQueryServiceBlockingStub analyticsAnalyticsEventQueryServiceBlocking;

    private final VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final ChannelRegistry channels;

    private final ControlWorkflowServiceGrpc.ControlWorkflowServiceBlockingStub complianceControlWorkflowServiceBlocking;

    private final HouseholdReportingServiceGrpc.HouseholdReportingServiceBlockingStub customersHouseholdReportingServiceBlocking;

    private final HouseholdWorkflowServiceGrpc.HouseholdWorkflowServiceBlockingStub customersHouseholdWorkflowServiceBlocking;

    private final PreferenceImportServiceGrpc.PreferenceImportServiceBlockingStub customersPreferenceImportServiceBlocking;

    private final PreferenceSyncServiceGrpc.PreferenceSyncServiceBlockingStub customersPreferenceSyncServiceBlocking;

    private final ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking;

    private final ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking;

    private final ApprovalInboxNotificationServiceGrpc.ApprovalInboxNotificationServiceBlockingStub financeportalApprovalInboxNotificationServiceBlocking;

    private final ApprovalInboxQueryServiceGrpc.ApprovalInboxQueryServiceBlockingStub financeportalApprovalInboxQueryServiceBlocking;

    private final CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking;

    private final CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking;

    private final FinanceWorkspaceRequestMapper mapper;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final FinanceportalRepository repository;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceStub treasuryLiquidityForecastImportServiceAsync;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public FinanceWorkspaceApplication(
            AnalyticsEventQueryServiceGrpc.AnalyticsEventQueryServiceBlockingStub analyticsAnalyticsEventQueryServiceBlocking,
            VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            ChannelRegistry channels,
            ControlWorkflowServiceGrpc.ControlWorkflowServiceBlockingStub complianceControlWorkflowServiceBlocking,
            HouseholdReportingServiceGrpc.HouseholdReportingServiceBlockingStub customersHouseholdReportingServiceBlocking,
            HouseholdWorkflowServiceGrpc.HouseholdWorkflowServiceBlockingStub customersHouseholdWorkflowServiceBlocking,
            PreferenceImportServiceGrpc.PreferenceImportServiceBlockingStub customersPreferenceImportServiceBlocking,
            PreferenceSyncServiceGrpc.PreferenceSyncServiceBlockingStub customersPreferenceSyncServiceBlocking,
            ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking,
            ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking,
            ApprovalInboxNotificationServiceGrpc.ApprovalInboxNotificationServiceBlockingStub financeportalApprovalInboxNotificationServiceBlocking,
            ApprovalInboxQueryServiceGrpc.ApprovalInboxQueryServiceBlockingStub financeportalApprovalInboxQueryServiceBlocking,
            CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking,
            CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking,
            FinanceWorkspaceRequestMapper mapper,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            FinanceportalRepository repository,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceStub treasuryLiquidityForecastImportServiceAsync,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.analyticsAnalyticsEventQueryServiceBlocking = analyticsAnalyticsEventQueryServiceBlocking;
        this.budgetingVarianceApprovalServiceBlocking = budgetingVarianceApprovalServiceBlocking;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.channels = channels;
        this.complianceControlWorkflowServiceBlocking = complianceControlWorkflowServiceBlocking;
        this.customersHouseholdReportingServiceBlocking = customersHouseholdReportingServiceBlocking;
        this.customersHouseholdWorkflowServiceBlocking = customersHouseholdWorkflowServiceBlocking;
        this.customersPreferenceImportServiceBlocking = customersPreferenceImportServiceBlocking;
        this.customersPreferenceSyncServiceBlocking = customersPreferenceSyncServiceBlocking;
        this.expensesExpensePolicySyncServiceBlocking = expensesExpensePolicySyncServiceBlocking;
        this.expensesReceiptWorkflowServiceBlocking = expensesReceiptWorkflowServiceBlocking;
        this.financeportalApprovalInboxNotificationServiceBlocking = financeportalApprovalInboxNotificationServiceBlocking;
        this.financeportalApprovalInboxQueryServiceBlocking = financeportalApprovalInboxQueryServiceBlocking;
        this.financeportalCloseChecklistIntegrationServiceBlocking = financeportalCloseChecklistIntegrationServiceBlocking;
        this.financeportalCloseChecklistWorkflowServiceBlocking = financeportalCloseChecklistWorkflowServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.repository = repository;
        this.treasuryLiquidityForecastImportServiceAsync = treasuryLiquidityForecastImportServiceAsync;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    /**
     * Creates a finance workspace.
     */
    public CreateFinanceWorkspaceResponse createFinanceWorkspace(CreateFinanceWorkspaceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateFinanceWorkspace")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getFinanceWorkspace());

        var outbound0 = ListPreferenceConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPreferenceConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFinanceWorkspace().getAudit().getCreatedAt())
                        .setOwnerId(request.getFinanceWorkspace().getAssignee().getUserId())
                        .build())
                .build();
        var reply0 = customersPreferenceSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPreferenceConflicts(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(SummarizeAnalyticsEventsRequest.newBuilder()
                .putAllAnnotations(request.getFinanceWorkspace().getCustomFieldsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeAnalyticsEventsRequest.Details.newBuilder()
                        .putAllFilters(reply0.getResult().getDetailsMap())
                        .build())
                .build()), SummarizeAnalyticsEventsRequest.class);
        var reply1 = analyticsAnalyticsEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeAnalyticsEvents(outbound1);

        var outbound2 = CountApprovalInboxesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountApprovalInboxesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFinanceWorkspace().getAudit().getCreatedAt())
                        .setOwnerId(switch (request.getFinanceWorkspace().getOriginCase()) {
                            case CUSTOMER -> request.getFinanceWorkspace().getCustomer().getSupplierId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply2 = financeportalApprovalInboxQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countApprovalInboxes(outbound2);

        var outbound3 = mapper.createFinanceWorkspaceStage3(request);
        var reply3 = complianceControlWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startControlWorkflow(outbound3);

        var outbound4 = ImportLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(ImportLiquidityForecastsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getFinanceWorkspace().getRequester())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(ImportLiquidityForecastsResponse.getDefaultInstance());
        var input4 = treasuryLiquidityForecastImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importLiquidityForecasts(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(request.getFinanceWorkspace().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFinanceWorkspace().getAudit().getCreatedAt())
                                .setCreatedBy(request.getFinanceWorkspace().getAudit().getCreatedBy())
                                .setEtag(request.getFinanceWorkspace().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input5 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getFinanceWorkspace().getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateFinanceWorkspace", request.getFinanceWorkspace().getFinanceWorkspaceId());
        return CreateFinanceWorkspaceResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(request.getFinanceWorkspace().getAssignee())
                        .setPriority(reply3.getControl().getPriority())
                        .setStatus(request.getFinanceWorkspace().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a finance workspace.
     */
    public GetFinanceWorkspaceResponse getFinanceWorkspace(GetFinanceWorkspaceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFinanceWorkspace")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "GetFinanceWorkspace");

        var outbound0 = CommentVarianceApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentVarianceApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            CommentVarianceApprovalResponse fetch(CommentVarianceApprovalRequest input) {
                return budgetingVarianceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentVarianceApproval(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return GetFinanceWorkspaceResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getVariance().getRequester().getEmail())
                                .build())
                        .putAllCustomFields(reply0.getVariance().getCustomFieldsMap())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .build())
                .build();
    }

    /**
     * Deletes a finance workspace.
     */
    public DeleteFinanceWorkspaceResponse deleteFinanceWorkspace(DeleteFinanceWorkspaceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteFinanceWorkspace")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "DeleteFinanceWorkspace");

        var outbound0 = AdvanceApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .setIssuedAt(request.getContext().getDeadline())
                        .build())
                .setContext(request.getContext())
                .setDetails(AdvanceApprovalInboxWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel0 = channels.lookup("acme.financeportal.v1.ApprovalInboxWorkflowService");
        var serviceName0 = "acme.financeportal.v1" + "." + "ApprovalInboxWorkflowService";
        var methodName0 = "Adv" + "anceApprovalInboxWorkflow";
        var descriptor0 = MethodDescriptor.<AdvanceApprovalInboxWorkflowRequest, AdvanceApprovalInboxWorkflowResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(AdvanceApprovalInboxWorkflowRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(AdvanceApprovalInboxWorkflowResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ReplayCloseChecklistEventsRequest.newBuilder()
                .setCloseChecklistId(reply0.getApprovalInbox().getCounterparty().getCloseChecklistId())
                .setContext(request.getContext())
                .setDetails(ReplayCloseChecklistEventsRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).replayCloseChecklistEvents(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = GenerateHouseholdReportRequest.newBuilder()
                .putAllAnnotations(reply0.getResult().getDetailsMap())
                .setContext(request.getContext())
                .setDetails(GenerateHouseholdReportRequest.Details.newBuilder()
                        .putAllFilters(reply1.getCloseChecklist().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        RpcPort<GenerateHouseholdReportRequest, GenerateHouseholdReportResponse> port2 = RpcPort.adapt(customersHouseholdReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::generateHouseholdReport);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteFinanceWorkspace", request.getFinanceWorkspaceId());
        return DeleteFinanceWorkspaceResponse.newBuilder()
                .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns several finance workspaces at once.
     */
    public BatchGetFinanceWorkspacesResponse batchGetFinanceWorkspaces(BatchGetFinanceWorkspacesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetFinanceWorkspaces")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "BatchGetFinanceWorkspaces");
        return BatchGetFinanceWorkspacesResponse.getDefaultInstance();
    }

    /**
     * Lists finance workspaces a page at a time.
     */
    public SearchFinanceWorkspacesResponse searchFinanceWorkspaces(SearchFinanceWorkspacesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchFinanceWorkspaces")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "SearchFinanceWorkspaces");

        var outbound0 = ListCloseChecklistWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCloseChecklistWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.financeportal.v1.CloseChecklistWorkflowService/ListCloseChecklistWorkflows", outbound0);
        var reply0 = financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCloseChecklistWorkflows(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchFinanceWorkspaces", request.getFilter().getQuery());
        return SearchFinanceWorkspacesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of finance workspaces as they happen.
     */
    public WatchFinanceWorkspacesResponse watchFinanceWorkspaces(WatchFinanceWorkspacesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchFinanceWorkspaces")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "WatchFinanceWorkspaces");

        var outbound0 = SuggestVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestVenuesRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestVenues(outbound0));

        var outbound1 = ValidatePreferenceImportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePreferenceImportRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setPhone(request.getSubscriber().getPhone())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = customersPreferenceImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePreferenceImport(outbound1);

        var outbound2 = ReconcileExpensePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileExpensePoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPreference().getAudit().getEtag())
                                .build())
                        .setBlock(ExpensePolicyBlockAction.newBuilder()
                                .setNotifyEmail(switch (reply1.getPreference().getActionCase()) {
                                    case BLOCK -> reply1.getPreference().getBlock().getNotifyEmail();
                                    default -> "";
                                })
                                .setReason(switch (reply1.getPreference().getActionCase()) {
                                    case BLOCK -> reply1.getPreference().getBlock().getReason();
                                    default -> "";
                                })
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getPreference().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = expensesExpensePolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileExpensePolicies(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getOwnerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchFinanceWorkspaces", request.getFilter().getOwnerId());
        return WatchFinanceWorkspacesResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPreference().getAudit().getEtag())
                                .build())
                        .addComments(FinanceWorkspaceComment.newBuilder()
                                .setCreatedAt(reply1.getPreference().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Takes a stream of finance workspaces and answers once at the end.
     */
    public BatchCreateFinanceWorkspacesResponse batchCreateFinanceWorkspaces(BatchCreateFinanceWorkspacesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateFinanceWorkspaces")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getRecord());
        }

        List<RetryHouseholdWorkflowResponse> results0 = request.getRecord().getCommentsList().stream()
                .map(element -> RetryHouseholdWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryHouseholdWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedAt(element.getCreatedAt())
                                        .build())
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getRecord().getCustomer().getDisplayName())
                                                .setEmail(request.getRecord().getAssignee().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersHouseholdWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryHouseholdWorkflow(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? RetryHouseholdWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SuggestApprovalInboxesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestApprovalInboxesRequest.Filter.newBuilder()
                        .setRegion(reply0.getHousehold().getLocation().getRegion())
                        .build())
                .build();
        var reply1 = financeportalApprovalInboxQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestApprovalInboxes(outbound1);

        var outbound2 = FindNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRecord().getSla().getDueAt())
                        .setOwnerId(switch (request.getRecord().getOriginCase()) {
                            case CUSTOMER -> request.getRecord().getCustomer().getSupplierId();
                            default -> "";
                        })
                        .setRegion(reply0.getHousehold().getLocation().getRegion())
                        .build())
                .build();
        var reply2 = notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findNotifications(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateFinanceWorkspaces", request.getHeader().getSource());
        return BatchCreateFinanceWorkspacesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Creates a finance workspace.
     */
    public CloneFinanceWorkspaceResponse cloneFinanceWorkspace(CloneFinanceWorkspaceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneFinanceWorkspace")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getFinanceWorkspace());

        var outbound0 = NotifyApprovalInboxWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyApprovalInboxWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getFinanceWorkspace().getRequester().getEmail())
                                .setName(request.getFinanceWorkspace().getRequester().getName())
                                .setPreferredChannel(request.getFinanceWorkspace().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> financeportalApprovalInboxNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyApprovalInboxWatchers(value)).orElseThrow();

        var outbound1 = AdvanceReceiptWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvanceReceiptWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(switch (request.getFinanceWorkspace().getOriginCase()) {
                                    case CUSTOMER -> request.getFinanceWorkspace().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getApprovalInbox().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getApprovalInbox().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getApprovalInbox().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply1 = expensesReceiptWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceReceiptWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFinanceWorkspace().getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneFinanceWorkspace", request.getFinanceWorkspace().getFinanceWorkspaceId());
        return CloneFinanceWorkspaceResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setPriority(request.getFinanceWorkspace().getPriority())
                        .setSubject(request.getFinanceWorkspace().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }
}
