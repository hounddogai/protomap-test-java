package com.acme.finance.domains.financeportal;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditEventRequest;
import com.acme.audit.v1.PreviewAuditEventResponse;
import com.acme.audit.v1.RepairRetentionPolicyRequest;
import com.acme.audit.v1.RepairRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyAdminServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdWorkflowServiceGrpc;
import com.acme.customers.v1.PauseHouseholdWorkflowRequest;
import com.acme.customers.v1.PauseHouseholdWorkflowResponse;
import com.acme.edgegateway.v1.RouteBindingServiceGrpc;
import com.acme.edgegateway.v1.WatchRouteBindingsRequest;
import com.acme.edgegateway.v1.WatchRouteBindingsResponse;
import com.acme.fanportal.v1.FanSessionServiceGrpc;
import com.acme.fanportal.v1.ListFanSessionsRequest;
import com.acme.fanportal.v1.ListFanSessionsResponse;
import com.acme.featureflags.v1.BrowseFeatureFlagsRequest;
import com.acme.featureflags.v1.BrowseFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventRequest;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventResponse;
import com.acme.financeportal.v1.ApprovalInboxNotificationServiceGrpc;
import com.acme.financeportal.v1.BrowseCloseChecklistsRequest;
import com.acme.financeportal.v1.BrowseCloseChecklistsResponse;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistQueryServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.ComposeFinanceWorkspacePageRequest;
import com.acme.financeportal.v1.ComposeFinanceWorkspacePageResponse;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FetchFinanceWorkspaceViewRequest;
import com.acme.financeportal.v1.FetchFinanceWorkspaceViewResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceNotificationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceServiceGrpc;
import com.acme.financeportal.v1.GetFinanceWorkspaceSummaryRequest;
import com.acme.financeportal.v1.GetFinanceWorkspaceSummaryResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsResponse;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersRequest;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersResponse;
import com.acme.financeportal.v1.RefreshFinanceWorkspaceRequest;
import com.acme.financeportal.v1.RefreshFinanceWorkspaceResponse;
import com.acme.financeportal.v1.ResolveCloseChecklistsRequest;
import com.acme.financeportal.v1.ResolveCloseChecklistsResponse;
import com.acme.financeportal.v1.RetryCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.RetryCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SearchFinanceWorkspaceCatalogRequest;
import com.acme.financeportal.v1.SearchFinanceWorkspaceCatalogResponse;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.StartFinanceWorkspaceCheckoutRequest;
import com.acme.financeportal.v1.StartFinanceWorkspaceCheckoutResponse;
import com.acme.financeportal.v1.StreamFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.StreamFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.SubmitFinanceWorkspaceFormRequest;
import com.acme.financeportal.v1.SubmitFinanceWorkspaceFormResponse;
import com.acme.financeportal.v1.TrackFinanceWorkspaceRequest;
import com.acme.financeportal.v1.TrackFinanceWorkspaceResponse;
import com.acme.financeportal.v1.UnsubscribeApprovalInboxUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeApprovalInboxUpdatesResponse;
import com.acme.financeportal.v1.UploadFinanceWorkspaceAttachmentsRequest;
import com.acme.financeportal.v1.UploadFinanceWorkspaceAttachmentsResponse;
import com.acme.financeportal.v1.WatchFinanceWorkspacesRequest;
import com.acme.financeportal.v1.WatchFinanceWorkspacesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.TerminatePolicyRequest;
import com.acme.insurance.v1.TerminatePolicyResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingStatusChange;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.ExplainLedgerAccountDecisionRequest;
import com.acme.ledger.v1.ExplainLedgerAccountDecisionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.procurement.v1.EscalateGoodsReceiptRequest;
import com.acme.procurement.v1.EscalateGoodsReceiptResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.RejectGoodsReceiptRequest;
import com.acme.procurement.v1.RejectGoodsReceiptResponse;
import com.acme.procurement.v1.SignalGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.SignalGoodsReceiptWorkflowResponse;
import com.acme.returns.v1.ReturnRequestServiceGrpc;
import com.acme.returns.v1.WatchReturnRequestsRequest;
import com.acme.returns.v1.WatchReturnRequestsResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * The edge API that apps call for finance workspaces. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class FinanceWorkspaceGatewayApplication {
    private static final Logger log = LoggerFactory.getLogger(FinanceWorkspaceGatewayApplication.class);

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final RetentionPolicyAdminServiceGrpc.RetentionPolicyAdminServiceBlockingStub auditRetentionPolicyAdminServiceBlocking;

    private final ChannelRegistry channels;

    private final HouseholdWorkflowServiceGrpc.HouseholdWorkflowServiceBlockingStub customersHouseholdWorkflowServiceBlocking;

    private final RouteBindingServiceGrpc.RouteBindingServiceBlockingStub edgegatewayRouteBindingServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FanSessionServiceGrpc.FanSessionServiceBlockingStub fanportalFanSessionServiceBlocking;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final ApprovalInboxNotificationServiceGrpc.ApprovalInboxNotificationServiceBlockingStub financeportalApprovalInboxNotificationServiceBlocking;

    private final CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking;

    private final CloseChecklistQueryServiceGrpc.CloseChecklistQueryServiceBlockingStub financeportalCloseChecklistQueryServiceBlocking;

    private final CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking;

    private final FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking;

    private final FinanceWorkspaceNotificationServiceGrpc.FinanceWorkspaceNotificationServiceBlockingStub financeportalFinanceWorkspaceNotificationServiceBlocking;

    private final FinanceWorkspaceServiceGrpc.FinanceWorkspaceServiceBlockingStub financeportalFinanceWorkspaceServiceBlocking;

    private final PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final FinanceWorkspaceGatewayRequestMapper mapper;

    private final GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceBlockingStub procurementGoodsReceiptApprovalServiceBlocking;

    private final FinanceportalRepository repository;

    private final ReturnRequestServiceGrpc.ReturnRequestServiceBlockingStub returnsReturnRequestServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierServiceGrpc.SupplierServiceStub suppliersSupplierServiceAsync;

    public FinanceWorkspaceGatewayApplication(
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            RetentionPolicyAdminServiceGrpc.RetentionPolicyAdminServiceBlockingStub auditRetentionPolicyAdminServiceBlocking,
            ChannelRegistry channels,
            HouseholdWorkflowServiceGrpc.HouseholdWorkflowServiceBlockingStub customersHouseholdWorkflowServiceBlocking,
            RouteBindingServiceGrpc.RouteBindingServiceBlockingStub edgegatewayRouteBindingServiceBlocking,
            ApplicationEventPublisher events,
            FanSessionServiceGrpc.FanSessionServiceBlockingStub fanportalFanSessionServiceBlocking,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            ApprovalInboxNotificationServiceGrpc.ApprovalInboxNotificationServiceBlockingStub financeportalApprovalInboxNotificationServiceBlocking,
            CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking,
            CloseChecklistQueryServiceGrpc.CloseChecklistQueryServiceBlockingStub financeportalCloseChecklistQueryServiceBlocking,
            CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking,
            FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking,
            FinanceWorkspaceNotificationServiceGrpc.FinanceWorkspaceNotificationServiceBlockingStub financeportalFinanceWorkspaceNotificationServiceBlocking,
            FinanceWorkspaceServiceGrpc.FinanceWorkspaceServiceBlockingStub financeportalFinanceWorkspaceServiceBlocking,
            PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            FinanceWorkspaceGatewayRequestMapper mapper,
            GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceBlockingStub procurementGoodsReceiptApprovalServiceBlocking,
            FinanceportalRepository repository,
            ReturnRequestServiceGrpc.ReturnRequestServiceBlockingStub returnsReturnRequestServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierServiceGrpc.SupplierServiceStub suppliersSupplierServiceAsync) {
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditRetentionPolicyAdminServiceBlocking = auditRetentionPolicyAdminServiceBlocking;
        this.channels = channels;
        this.customersHouseholdWorkflowServiceBlocking = customersHouseholdWorkflowServiceBlocking;
        this.edgegatewayRouteBindingServiceBlocking = edgegatewayRouteBindingServiceBlocking;
        this.events = events;
        this.fanportalFanSessionServiceBlocking = fanportalFanSessionServiceBlocking;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.financeportalApprovalInboxNotificationServiceBlocking = financeportalApprovalInboxNotificationServiceBlocking;
        this.financeportalCloseChecklistIntegrationServiceBlocking = financeportalCloseChecklistIntegrationServiceBlocking;
        this.financeportalCloseChecklistQueryServiceBlocking = financeportalCloseChecklistQueryServiceBlocking;
        this.financeportalCloseChecklistWorkflowServiceBlocking = financeportalCloseChecklistWorkflowServiceBlocking;
        this.financeportalFinanceWorkspaceIntegrationServiceBlocking = financeportalFinanceWorkspaceIntegrationServiceBlocking;
        this.financeportalFinanceWorkspaceNotificationServiceBlocking = financeportalFinanceWorkspaceNotificationServiceBlocking;
        this.financeportalFinanceWorkspaceServiceBlocking = financeportalFinanceWorkspaceServiceBlocking;
        this.insurancePolicyLifecycleServiceBlocking = insurancePolicyLifecycleServiceBlocking;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.mapper = mapper;
        this.procurementGoodsReceiptApprovalServiceBlocking = procurementGoodsReceiptApprovalServiceBlocking;
        this.repository = repository;
        this.returnsReturnRequestServiceBlocking = returnsReturnRequestServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierServiceAsync = suppliersSupplierServiceAsync;
    }

    public record ComposeFinanceWorkspacePageStage2Requested(SignalGoodsReceiptWorkflowRequest request, CompletableFuture<SignalGoodsReceiptWorkflowResponse> completion, io.grpc.Context context) {}

    /**
     * Fetchs a finance workspace.
     */
    public FetchFinanceWorkspaceViewResponse fetchFinanceWorkspaceView(FetchFinanceWorkspaceViewRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchFinanceWorkspaceView")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getFinanceWorkspace());

        var outbound0 = ResolveCloseChecklistsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> financeportalCloseChecklistQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCloseChecklists(outbound0), rpcExecutor);

        var outbound1 = RejectGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectGoodsReceiptRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getFinanceWorkspace().getCustomer().getDisplayName())
                                .setEmail(request.getFinanceWorkspace().getRequester().getEmail())
                                .build())
                        .build())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(request.getFinanceWorkspace().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> procurementGoodsReceiptApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectGoodsReceipt(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ExplainLedgerAccountDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getGoodsReceipt().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.ledger.v1.LedgerAccountValidationService");
        var serviceName2 = "acme.ledger.v1" + "." + "LedgerAccountValidationService";
        var methodName2 = "Exp" + "lainLedgerAccountDecision";
        var descriptor2 = MethodDescriptor.<ExplainLedgerAccountDecisionRequest, ExplainLedgerAccountDecisionResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainLedgerAccountDecisionRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainLedgerAccountDecisionResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = PreviewAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getFinanceWorkspace().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAuditEvent(value));

        var outbound4 = BatchCreateSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateSuppliersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getFinanceWorkspace().getRequester())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateSuppliersResponse.getDefaultInstance());
        var input4 = suppliersSupplierServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateSuppliers(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(request.getFinanceWorkspace().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFinanceWorkspace().getAudit().getCreatedAt())
                                .setCreatedBy(request.getFinanceWorkspace().getAudit().getCreatedBy())
                                .setEtag(request.getFinanceWorkspace().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input5 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getFinanceWorkspace().getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FetchFinanceWorkspaceView", request.getFinanceWorkspace().getFinanceWorkspaceId());
        return FetchFinanceWorkspaceViewResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply2.getLedgerAccount().getContact().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getGoodsReceipt().getAudit().getUpdatedBy())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setRequester(request.getFinanceWorkspace().getRequester())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Composes a finance workspace.
     */
    public ComposeFinanceWorkspacePageResponse composeFinanceWorkspacePage(ComposeFinanceWorkspacePageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ComposeFinanceWorkspacePage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getFinanceWorkspace());

        var outbound0 = mapper.composeFinanceWorkspacePageStage0(request);
        var reply0 = insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listUnderwritingConflicts(outbound0);

        var outbound1 = BrowseCloseChecklistsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCloseChecklistsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFinanceWorkspace().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getFinanceWorkspace().getCustomer().getSupplierId())
                        .build())
                .build();
        var reply1 = financeportalCloseChecklistQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseCloseChecklists(outbound1);

        var outbound2 = SignalGoodsReceiptWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalGoodsReceiptWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getFinanceWorkspace().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<SignalGoodsReceiptWorkflowResponse>();
        events.publishEvent(new ComposeFinanceWorkspacePageStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = WatchReturnRequestsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getFinanceWorkspace().getCustomer().getEmail())
                        .setName(request.getFinanceWorkspace().getRequester().getName())
                        .setPreferredChannel(request.getFinanceWorkspace().getRequester().getPreferredChannel())
                        .build())
                .build();
        var stream3 = returnsReturnRequestServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchReturnRequests(outbound3);
        var reply3 = WatchReturnRequestsResponse.getDefaultInstance();
        while (stream3.hasNext()) {
            reply3 = stream3.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFinanceWorkspace().getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ComposeFinanceWorkspacePage", request.getFinanceWorkspace().getFinanceWorkspaceId());
        return ComposeFinanceWorkspacePageResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(request.getFinanceWorkspace().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getGoodsReceipt().getAudit().getEtag())
                                .setUpdatedBy(reply2.getGoodsReceipt().getAudit().getUpdatedBy())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Refreshs a finance workspace.
     */
    public RefreshFinanceWorkspaceResponse refreshFinanceWorkspace(RefreshFinanceWorkspaceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RefreshFinanceWorkspace")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getFinanceWorkspace());

        var outbound0 = UnsubscribeApprovalInboxUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeApprovalInboxUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getFinanceWorkspace().getAssignee().getEmail())
                                .setName(request.getFinanceWorkspace().getRequester().getName())
                                .setPhone(request.getFinanceWorkspace().getRequester().getPhone())
                                .setPreferredChannel(request.getFinanceWorkspace().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<UnsubscribeApprovalInboxUpdatesRequest, UnsubscribeApprovalInboxUpdatesResponse>() {
            @Override
            public UnsubscribeApprovalInboxUpdatesResponse apply(UnsubscribeApprovalInboxUpdatesRequest input) {
                return financeportalApprovalInboxNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeApprovalInboxUpdates(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = WatchRouteBindingsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getFinanceWorkspace().getAssignee().getEmail())
                        .setName(request.getFinanceWorkspace().getRequester().getName())
                        .setPhone(request.getFinanceWorkspace().getRequester().getPhone())
                        .setPreferredChannel(request.getFinanceWorkspace().getRequester().getPreferredChannel())
                        .build())
                .build();
        var stream1 = edgegatewayRouteBindingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchRouteBindings(outbound1);
        var reply1 = WatchRouteBindingsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFinanceWorkspace().getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RefreshFinanceWorkspace", request.getFinanceWorkspace().getFinanceWorkspaceId());
        return RefreshFinanceWorkspaceResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setSubject(request.getFinanceWorkspace().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Submits a finance workspace.
     */
    public SubmitFinanceWorkspaceFormResponse submitFinanceWorkspaceForm(SubmitFinanceWorkspaceFormRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitFinanceWorkspaceForm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getFinanceWorkspace());

        var outbound0 = EscalateGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(request.getFinanceWorkspace().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> procurementGoodsReceiptApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateGoodsReceipt(outbound0), rpcExecutor);

        var outbound1 = TerminatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(TerminatePolicyRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getFinanceWorkspace().getAssignee().getDisplayName())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getFinanceWorkspace().getAssignee().getEmail())
                                .setName(request.getFinanceWorkspace().getRequester().getName())
                                .build())
                        .build())
                .setPolicyId(request.getFinanceWorkspace().getSla().getPolicyId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).terminatePolicy(outbound1), rpcExecutor);

        var outbound2 = ConnectFinanceWorkspaceSourceRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ConnectFinanceWorkspaceSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).connectFinanceWorkspaceSource(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getFinanceWorkspace().getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitFinanceWorkspaceForm", request.getFinanceWorkspace().getFinanceWorkspaceId());
        return SubmitFinanceWorkspaceFormResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .putAllCustomFields(request.getFinanceWorkspace().getCustomFieldsMap())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply2.getFinanceWorkspace().getRequester().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Tracks a finance workspace.
     */
    public TrackFinanceWorkspaceResponse trackFinanceWorkspace(TrackFinanceWorkspaceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TrackFinanceWorkspace")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getFinanceWorkspace());

        var outbound0 = SummarizeSearchIndexesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.search.v1.SearchIndexQueryService");
        var selected0 = SearchIndexQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.summarizeSearchIndexes(outbound0);

        var outbound1 = WatchFinanceWorkspacesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setName(request.getFinanceWorkspace().getRequester().getName())
                        .build())
                .build();
        var stream1 = financeportalFinanceWorkspaceServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchFinanceWorkspaces(outbound1);
        var reply1 = WatchFinanceWorkspacesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = (ConnectFinanceWorkspaceSourceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ConnectFinanceWorkspaceSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply2 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).connectFinanceWorkspaceSource(outbound2);

        var outbound3 = CancelCloseChecklistWorkflowRequest.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getFinanceWorkspace().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(switch (reply2.getFinanceWorkspace().getOriginCase()) {
                                    case CUSTOMER -> reply2.getFinanceWorkspace().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .setEmail(reply2.getFinanceWorkspace().getAssignee().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply3 = financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelCloseChecklistWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFinanceWorkspace().getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TrackFinanceWorkspace", request.getFinanceWorkspace().getFinanceWorkspaceId());
        return TrackFinanceWorkspaceResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(reply2.getFinanceWorkspace().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply3.getCloseChecklist().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply3.getCloseChecklist().getAudit().getUpdatedBy())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setSubject(request.getFinanceWorkspace().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Streams the changes of finance workspaces as they happen.
     */
    public StreamFinanceWorkspaceUpdatesResponse streamFinanceWorkspaceUpdates(StreamFinanceWorkspaceUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamFinanceWorkspaceUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "StreamFinanceWorkspaceUpdates");

        var outbound0 = ListFinanceWorkspaceSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFinanceWorkspaceSubscriptionsRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> financeportalFinanceWorkspaceNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFinanceWorkspaceSubscriptions(outbound0), rpcExecutor);

        var outbound1 = BrowseFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseFeatureFlagsRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseFeatureFlags(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamFinanceWorkspaceUpdates", request.getFilter().getQuery());
        return StreamFinanceWorkspaceUpdatesResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Takes a stream of finance workspaces and answers once at the end.
     */
    public UploadFinanceWorkspaceAttachmentsResponse uploadFinanceWorkspaceAttachments(UploadFinanceWorkspaceAttachmentsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UploadFinanceWorkspaceAttachments")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getRecord());
        }

        var futures0 = request.getRecord().getHistoryList().stream()
                .map(element -> ReconcileUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileUnderwritingsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .addHistory(UnderwritingStatusChange.newBuilder()
                                .setChangedAt(element.getChangedAt())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileUnderwritings(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ReconcileUnderwritingsResponse.getDefaultInstance()
                : results0.getLast();

        var results1 = new ArrayList<RetryCloseChecklistWorkflowResponse>();
        for (var element : request.getRecord().getCommentsList()) {
            RetryCloseChecklistWorkflowRequest itemRequest = RetryCloseChecklistWorkflowRequest.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results1.add(financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryCloseChecklistWorkflow(itemRequest));
        }
        var reply1 = results1.isEmpty() ? RetryCloseChecklistWorkflowResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = SignalCloseChecklistWorkflowRequest.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCloseChecklist().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getUnderwriting().getCustomer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.financeportal.v1.CloseChecklistWorkflowService");
        var selected2 = CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.signalCloseChecklistWorkflow(outbound2);

        var outbound3 = RepairRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RepairRetentionPolicyRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(switch (reply0.getUnderwriting().getOriginCase()) {
                                    case CUSTOMER -> reply0.getUnderwriting().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = auditRetentionPolicyAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).repairRetentionPolicy(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UploadFinanceWorkspaceAttachments", request.getHeader().getSource());
        return UploadFinanceWorkspaceAttachmentsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists finance workspaces a page at a time.
     */
    public SearchFinanceWorkspaceCatalogResponse searchFinanceWorkspaceCatalog(SearchFinanceWorkspaceCatalogRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchFinanceWorkspaceCatalog")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getContext().getTraceId(), "SearchFinanceWorkspaceCatalog");

        var outbound0 = PreviewAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSource(request.getFilter().getOwnerId())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var reply0 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAuditEvent(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchFinanceWorkspaceCatalog", request.getPage().getPageToken());
        return SearchFinanceWorkspaceCatalogResponse.newBuilder()
                .addFinanceWorkspaces(FinanceWorkspace.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a finance workspace.
     */
    public GetFinanceWorkspaceSummaryResponse getFinanceWorkspaceSummary(GetFinanceWorkspaceSummaryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFinanceWorkspaceSummary")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FinanceWorkspace", request.getFinanceWorkspaceId(), "GetFinanceWorkspaceSummary");

        var outbound0 = NotifyFinanceWorkspaceWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyFinanceWorkspaceWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(financeportalFinanceWorkspaceNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyFinanceWorkspaceWatchers(value)))
                .collect(Collectors.toList()).getFirst();
        return GetFinanceWorkspaceSummaryResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFinanceWorkspace().getAudit().getCreatedAt())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Starts a finance workspace.
     */
    public StartFinanceWorkspaceCheckoutResponse startFinanceWorkspaceCheckout(StartFinanceWorkspaceCheckoutRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartFinanceWorkspaceCheckout")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFinanceWorkspace(request.getContext().getTenantId(), request.getFinanceWorkspace());

        var results0 = new ArrayList<AcknowledgeCloseChecklistEventResponse>();
        for (var element : request.getFinanceWorkspace().getCommentsList()) {
            AcknowledgeCloseChecklistEventRequest itemRequest = AcknowledgeCloseChecklistEventRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AcknowledgeCloseChecklistEventRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setReceivedAt(element.getCreatedAt())
                        .build())
                .setReason(request.getReason())
                .build();
            results0.add(financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).acknowledgeCloseChecklistEvent(itemRequest));
        }
        var reply0 = results0.isEmpty() ? AcknowledgeCloseChecklistEventResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = PauseHouseholdWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getFinanceWorkspace().getAssignee().getDisplayName())
                                                .setUserId(request.getFinanceWorkspace().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = customersHouseholdWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseHouseholdWorkflow(outbound1);

        var outbound2 = ListFanSessionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFanSessionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFinanceWorkspace().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getCloseChecklist().getCounterparty().getCustomerId())
                        .setRegion(reply0.getCloseChecklist().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(fanportalFanSessionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFanSessions(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getFinanceWorkspace().getFinanceWorkspaceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartFinanceWorkspaceCheckout", request.getFinanceWorkspace().getFinanceWorkspaceId());
        return StartFinanceWorkspaceCheckoutResponse.newBuilder()
                .setFinanceWorkspace(FinanceWorkspace.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getHousehold().getAudit().getUpdatedBy())
                                .build())
                        .setFinanceWorkspaceId(request.getFinanceWorkspaceId())
                        .setPriority(request.getFinanceWorkspace().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }
}
