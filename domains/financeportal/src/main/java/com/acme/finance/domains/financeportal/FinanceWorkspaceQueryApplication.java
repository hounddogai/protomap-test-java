package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ListRetentionPolicyConflictsRequest;
import com.acme.audit.v1.ListRetentionPolicyConflictsResponse;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.driverapp.v1.CloneDriverHomeRequest;
import com.acme.driverapp.v1.CloneDriverHomeResponse;
import com.acme.driverapp.v1.DriverHome;
import com.acme.driverapp.v1.DriverHomeServiceGrpc;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.CreateFeatureFlagRequest;
import com.acme.featureflags.v1.CreateFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventRequest;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventResponse;
import com.acme.financeportal.v1.BrowseFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BrowseFinanceWorkspacesResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistLine;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.CountFinanceWorkspacesRequest;
import com.acme.financeportal.v1.CountFinanceWorkspacesResponse;
import com.acme.financeportal.v1.DisconnectCloseChecklistSourceRequest;
import com.acme.financeportal.v1.DisconnectCloseChecklistSourceResponse;
import com.acme.financeportal.v1.ExportFinanceWorkspacesRequest;
import com.acme.financeportal.v1.ExportFinanceWorkspacesResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.LookupFinanceWorkspaceRequest;
import com.acme.financeportal.v1.LookupFinanceWorkspaceResponse;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.PeekFinanceWorkspaceRequest;
import com.acme.financeportal.v1.PeekFinanceWorkspaceResponse;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesRequest;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesResponse;
import com.acme.financeportal.v1.ResolveFinanceWorkspacesRequest;
import com.acme.financeportal.v1.ResolveFinanceWorkspacesResponse;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesRequest;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesResponse;
import com.acme.financeportal.v1.TestCloseChecklistConnectionRequest;
import com.acme.financeportal.v1.TestCloseChecklistConnectionResponse;
import com.acme.identity.v1.BatchGetUsersRequest;
import com.acme.identity.v1.BatchGetUsersResponse;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderWorkflowServiceGrpc;
import com.acme.procurement.v1.SignalPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.SignalPurchaseOrderWorkflowResponse;
import com.acme.suppliers.v1.EscalateSupplierRatingRequest;
import com.acme.suppliers.v1.EscalateSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about finance workspaces, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class FinanceWorkspaceQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(FinanceWorkspaceQueryApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final ChannelRegistry channels;

    private final DriverHomeServiceGrpc.DriverHomeServiceBlockingStub driverappDriverHomeServiceBlocking;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceStub financeportalCloseChecklistIntegrationServiceAsync;

    private final CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking;

    private final CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceFutureStub financeportalCloseChecklistIntegrationServiceFuture;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final PurchaseOrderWorkflowServiceGrpc.PurchaseOrderWorkflowServiceBlockingStub procurementPurchaseOrderWorkflowServiceBlocking;

    private final FinanceportalRepository repository;

    private final Executor rpcExecutor;

    private final SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking;

    public FinanceWorkspaceQueryApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            ChannelRegistry channels,
            DriverHomeServiceGrpc.DriverHomeServiceBlockingStub driverappDriverHomeServiceBlocking,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceStub financeportalCloseChecklistIntegrationServiceAsync,
            CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking,
            CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceFutureStub financeportalCloseChecklistIntegrationServiceFuture,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            PurchaseOrderWorkflowServiceGrpc.PurchaseOrderWorkflowServiceBlockingStub procurementPurchaseOrderWorkflowServiceBlocking,
            FinanceportalRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.channels = channels;
        this.driverappDriverHomeServiceBlocking = driverappDriverHomeServiceBlocking;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.financeportalCloseChecklistIntegrationServiceAsync = financeportalCloseChecklistIntegrationServiceAsync;
        this.financeportalCloseChecklistIntegrationServiceBlocking = financeportalCloseChecklistIntegrationServiceBlocking;
        this.financeportalCloseChecklistIntegrationServiceFuture = financeportalCloseChecklistIntegrationServiceFuture;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.procurementPurchaseOrderWorkflowServiceBlocking = procurementPurchaseOrderWorkflowServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRatingApprovalServiceBlocking = suppliersSupplierRatingApprovalServiceBlocking;
    }

    /**
     * Returns a finance workspace.
     */
    public LookupFinanceWorkspaceResponse lookupFinanceWorkspace(LookupFinanceWorkspaceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupFinanceWorkspace")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "LookupFinanceWorkspace");

        var outbound0 = CloneDriverHomeRequest.newBuilder()
                .setContext(request.getContext())
                .setDriverHome(DriverHome.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.driverapp.v1.DriverHomeService/CloneDriverHome", outbound0);
        var reply0 = driverappDriverHomeServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneDriverHome(command0.payload());

        var outbound1 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input1 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = RelayCloseChecklistMessagesRequest.newBuilder()
                .setChange(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setDueAt(request.getContext().getDeadline())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(RelayCloseChecklistMessagesResponse.getDefaultInstance());
        var input2 = financeportalCloseChecklistIntegrationServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).relayCloseChecklistMessages(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        return LookupFinanceWorkspaceResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getDriverHome().getAssignee().getEmail())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setRequester(reply0.getDriverHome().getRequester())
                        .build())
                .build();
    }

    /**
     * Lists finance workspaces a page at a time.
     */
    public BrowseFinanceWorkspacesResponse browseFinanceWorkspaces(BrowseFinanceWorkspacesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseFinanceWorkspaces")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "BrowseFinanceWorkspaces");

        var outbound0 = TestCloseChecklistConnectionRequest.newBuilder()
                .putAllAnnotations(request.getFilter().getLabelsMap())
                .setContext(request.getContext())
                .setDetails(TestCloseChecklistConnectionRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getFilter().getOwnerId())
                        .setReceivedAt(request.getFilter().getCreatedAfter())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).testCloseChecklistConnection(outbound0), rpcExecutor);

        var outbound1 = ListRetentionPolicyConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicyConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicyConflicts(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseFinanceWorkspaces", request.getPage().getPageToken());
        return BrowseFinanceWorkspacesResponse.newBuilder()
                .addFinanceWorkspaces(FinanceWorkspace.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCloseChecklist().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Counts finance workspaces by status.
     */
    public CountFinanceWorkspacesResponse countFinanceWorkspaces(CountFinanceWorkspacesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountFinanceWorkspaces")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "CountFinanceWorkspaces");

        var outbound0 = AcknowledgeCloseChecklistEventRequest.newBuilder()
                .putAllAnnotations(request.getFilter().getLabelsMap())
                .setContext(request.getContext())
                .setDetails(AcknowledgeCloseChecklistEventRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .build();
        var pending0 = financeportalCloseChecklistIntegrationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).acknowledgeCloseChecklistEvent(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = TestCloseChecklistConnectionRequest.newBuilder()
                .putAllAnnotations(request.getFilter().getLabelsMap())
                .setCloseChecklistId(reply0.getCloseChecklist().getCloseChecklistId())
                .setContext(request.getContext())
                .setDetails(TestCloseChecklistConnectionRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(reply0.getResult().getMessage())
                        .setReceivedAt(reply0.getCloseChecklist().getAudit().getUpdatedAt())
                        .build())
                .build();
        Function<TestCloseChecklistConnectionRequest, TestCloseChecklistConnectionResponse> operation1 = financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::testCloseChecklistConnection;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountFinanceWorkspaces", request.getPage().getPageToken());
        return CountFinanceWorkspacesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns several finance workspaces at once.
     */
    public ResolveFinanceWorkspacesResponse resolveFinanceWorkspaces(ResolveFinanceWorkspacesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveFinanceWorkspaces")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "ResolveFinanceWorkspaces");

        var builder0 = BatchGetFeatureFlagsRequest.newBuilder();
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetFeatureFlags(outbound0);
        return ResolveFinanceWorkspacesResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .build();
    }

    /**
     * Streams every matching finance workspace.
     */
    public ExportFinanceWorkspacesResponse exportFinanceWorkspaces(ExportFinanceWorkspacesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportFinanceWorkspaces")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "ExportFinanceWorkspaces");

        var outbound0 = PauseCloseChecklistWorkflowRequest.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .addLines(CloseChecklistLine.newBuilder()
                                .putAllAttributes(request.getFilter().getLabelsMap())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PauseCloseChecklistWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.financeportal.v1.CloseChecklistWorkflowService");
        var selected0 = CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.pauseCloseChecklistWorkflow(outbound0);

        var outbound1 = CreateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCloseChecklist().getAudit().getEtag())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getCloseChecklist().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        RpcPort<CreateUserRequest, CreateUserResponse> port1 = RpcPort.adapt(identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createUser);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = AcknowledgeCloseChecklistEventRequest.newBuilder()
                .setCloseChecklistId(reply0.getCloseChecklist().getCloseChecklistId())
                .setContext(request.getContext())
                .setDetails(AcknowledgeCloseChecklistEventRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .build();
        var pending2 = financeportalCloseChecklistIntegrationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).acknowledgeCloseChecklistEvent(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportFinanceWorkspaces", request.getFilter().getQuery());
        return ExportFinanceWorkspacesResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getUser().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Summarizes a finance workspace.
     */
    public SummarizeFinanceWorkspacesResponse summarizeFinanceWorkspaces(SummarizeFinanceWorkspacesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeFinanceWorkspaces")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "SummarizeFinanceWorkspaces");

        var outbound0 = SignalPurchaseOrderWorkflowRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SignalPurchaseOrderWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> procurementPurchaseOrderWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalPurchaseOrderWorkflow(outbound0), rpcExecutor);

        var outbound1 = BatchGetUsersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetUsers(outbound1), rpcExecutor);

        var outbound2 = DisconnectCloseChecklistSourceRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(DisconnectCloseChecklistSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).disconnectCloseChecklistSource(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();

        var outbound3 = (CreateFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPurchaseOrder().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createFeatureFlag(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeFinanceWorkspaces", request.getFinanceWorkspaceId());
        return SummarizeFinanceWorkspacesResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a finance workspace.
     */
    public PeekFinanceWorkspaceResponse peekFinanceWorkspace(PeekFinanceWorkspaceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekFinanceWorkspace")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "PeekFinanceWorkspace");

        var outbound0 = RpcSupport.unpack(Any.pack(EscalateSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateSupplierRatingRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()), EscalateSupplierRatingRequest.class);
        var reply0 = suppliersSupplierRatingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateSupplierRating(outbound0);
        return PeekFinanceWorkspaceResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
