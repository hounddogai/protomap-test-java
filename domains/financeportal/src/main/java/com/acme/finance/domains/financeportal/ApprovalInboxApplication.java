package com.acme.finance.domains.financeportal;

import com.acme.assets.v1.CancelDepreciationWorkflowRequest;
import com.acme.assets.v1.CancelDepreciationWorkflowResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentBankSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.NormalizeDocumentRequest;
import com.acme.documents.v1.NormalizeDocumentResponse;
import com.acme.edgegateway.v1.RouteBindingQueryServiceGrpc;
import com.acme.edgegateway.v1.SummarizeRouteBindingsRequest;
import com.acme.edgegateway.v1.SummarizeRouteBindingsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxLine;
import com.acme.financeportal.v1.ApprovalInboxLineTax;
import com.acme.financeportal.v1.BatchGetApprovalInboxesRequest;
import com.acme.financeportal.v1.BatchGetApprovalInboxesResponse;
import com.acme.financeportal.v1.CloneApprovalInboxRequest;
import com.acme.financeportal.v1.CloneApprovalInboxResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistNotificationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistQueryServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistRef;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.CreateApprovalInboxRequest;
import com.acme.financeportal.v1.CreateApprovalInboxResponse;
import com.acme.financeportal.v1.DeleteApprovalInboxRequest;
import com.acme.financeportal.v1.DeleteApprovalInboxResponse;
import com.acme.financeportal.v1.ExportCloseChecklistsRequest;
import com.acme.financeportal.v1.ExportCloseChecklistsResponse;
import com.acme.financeportal.v1.FinanceWorkspaceNotificationServiceGrpc;
import com.acme.financeportal.v1.GetApprovalInboxRequest;
import com.acme.financeportal.v1.GetApprovalInboxResponse;
import com.acme.financeportal.v1.ListApprovalInboxSourcesRequest;
import com.acme.financeportal.v1.ListApprovalInboxSourcesResponse;
import com.acme.financeportal.v1.ListApprovalInboxesRequest;
import com.acme.financeportal.v1.ListApprovalInboxesResponse;
import com.acme.financeportal.v1.SearchApprovalInboxesRequest;
import com.acme.financeportal.v1.SearchApprovalInboxesResponse;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SubscribeCloseChecklistUpdatesRequest;
import com.acme.financeportal.v1.SubscribeCloseChecklistUpdatesResponse;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.UpdateApprovalInboxRequest;
import com.acme.financeportal.v1.UpdateApprovalInboxResponse;
import com.acme.financeportal.v1.WatchApprovalInboxesRequest;
import com.acme.financeportal.v1.WatchApprovalInboxesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.CountEInvoiceTotalsRequest;
import com.acme.invoicing.v1.CountEInvoiceTotalsResponse;
import com.acme.invoicing.v1.EInvoiceReportingServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexComponent;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.suppliers.v1.DelegateSupplierContractApprovalRequest;
import com.acme.suppliers.v1.DelegateSupplierContractApprovalResponse;
import com.acme.suppliers.v1.EscalateSupplierContractRequest;
import com.acme.suppliers.v1.EscalateSupplierContractResponse;
import com.acme.suppliers.v1.ListSupplierWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierWorkflowsResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractAttachment;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.acme.venues.v1.UpdateVenueRequest;
import com.acme.venues.v1.UpdateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.acme.webbff.v1.PageCompositionQueryServiceGrpc;
import com.acme.webbff.v1.ResolvePageCompositionsRequest;
import com.acme.webbff.v1.ResolvePageCompositionsResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
 * Manages approval inboxes: creates, reads, changes, and lists them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ApprovalInboxApplication {
    private static final Logger log = LoggerFactory.getLogger(ApprovalInboxApplication.class);

    private final DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final RouteBindingQueryServiceGrpc.RouteBindingQueryServiceBlockingStub edgegatewayRouteBindingQueryServiceBlocking;

    private final ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceBlockingStub financeportalApprovalInboxIntegrationServiceBlocking;

    private final CloseChecklistQueryServiceGrpc.CloseChecklistQueryServiceBlockingStub financeportalCloseChecklistQueryServiceBlocking;

    private final CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking;

    private final FinanceWorkspaceNotificationServiceGrpc.FinanceWorkspaceNotificationServiceBlockingStub financeportalFinanceWorkspaceNotificationServiceBlocking;

    private final EInvoiceReportingServiceGrpc.EInvoiceReportingServiceFutureStub invoicingEInvoiceReportingServiceFuture;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final FinanceportalRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    private final SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking;

    private final SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking;

    private final VenueServiceGrpc.VenueServiceFutureStub venuesVenueServiceFuture;

    public ApprovalInboxApplication(
            DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            ChannelRegistry channels,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            RouteBindingQueryServiceGrpc.RouteBindingQueryServiceBlockingStub edgegatewayRouteBindingQueryServiceBlocking,
            ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceBlockingStub financeportalApprovalInboxIntegrationServiceBlocking,
            CloseChecklistQueryServiceGrpc.CloseChecklistQueryServiceBlockingStub financeportalCloseChecklistQueryServiceBlocking,
            CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking,
            FinanceWorkspaceNotificationServiceGrpc.FinanceWorkspaceNotificationServiceBlockingStub financeportalFinanceWorkspaceNotificationServiceBlocking,
            EInvoiceReportingServiceGrpc.EInvoiceReportingServiceFutureStub invoicingEInvoiceReportingServiceFuture,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            FinanceportalRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking,
            SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking,
            SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking,
            VenueServiceGrpc.VenueServiceFutureStub venuesVenueServiceFuture) {
        this.assetsDepreciationWorkflowServiceBlocking = assetsDepreciationWorkflowServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.channels = channels;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.edgegatewayRouteBindingQueryServiceBlocking = edgegatewayRouteBindingQueryServiceBlocking;
        this.financeportalApprovalInboxIntegrationServiceBlocking = financeportalApprovalInboxIntegrationServiceBlocking;
        this.financeportalCloseChecklistQueryServiceBlocking = financeportalCloseChecklistQueryServiceBlocking;
        this.financeportalCloseChecklistWorkflowServiceBlocking = financeportalCloseChecklistWorkflowServiceBlocking;
        this.financeportalFinanceWorkspaceNotificationServiceBlocking = financeportalFinanceWorkspaceNotificationServiceBlocking;
        this.invoicingEInvoiceReportingServiceFuture = invoicingEInvoiceReportingServiceFuture;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
        this.suppliersSupplierContractApprovalServiceBlocking = suppliersSupplierContractApprovalServiceBlocking;
        this.suppliersSupplierWorkflowServiceBlocking = suppliersSupplierWorkflowServiceBlocking;
        this.venuesVenueServiceFuture = venuesVenueServiceFuture;
    }

    /**
     * Creates an approval inbox.
     */
    public CreateApprovalInboxResponse createApprovalInbox(CreateApprovalInboxRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateApprovalInbox")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveApprovalInbox(request.getContext().getTenantId(), request.getApprovalInbox());

        var outbound0 = NormalizeDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDocument(Document.newBuilder()
                        .setBank(DocumentBankSettlement.newBuilder()
                                .setIban(switch (request.getApprovalInbox().getSettlementCase()) {
                                    case BANK -> request.getApprovalInbox().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getApprovalInbox().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply0 = documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeDocument(outbound0);

        var builder1 = ListSupplierWorkflowsRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(ListSupplierWorkflowsRequest.Filter.newBuilder()
                .setCreatedAfter(request.getApprovalInbox().getAudit().getCreatedAt())
                .putAllLabels(reply0.getDocument().getMetadataMap())
                .setOwnerId(request.getApprovalInbox().getApprovalInboxId())
                .setRegion(request.getApprovalInbox().getBillingAddress().getRegion())
                .build());
        var outbound1 = builder1.build();
        var reply1 = suppliersSupplierWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSupplierWorkflows(outbound1);

        var outbound2 = ExportCloseChecklistsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportCloseChecklistsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getDocument().getAudit().getUpdatedAt())
                        .setOwnerId(request.getApprovalInbox().getCounterparty().getCloseChecklistId())
                        .setRegion(reply0.getDocument().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = financeportalCloseChecklistQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportCloseChecklists(outbound2);
        var reply2 = ExportCloseChecklistsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = SendLedgerAccountTestNoticeRequest.newBuilder()
                .putAllAnnotations(reply1.getResult().getDetailsMap())
                .setContext(request.getContext())
                .setDetails(SendLedgerAccountTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(reply0.getDocument().getMetadataMap())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendLedgerAccountTestNotice(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getApprovalInbox().getCounterparty().getDisplayName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateApprovalInbox", request.getApprovalInbox().getCounterparty().getDisplayName());
        return CreateApprovalInboxResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getLedgerAccount().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(CloseChecklistRef.newBuilder()
                                .setNumber(request.getApprovalInbox().getNumber())
                                .build())
                        .setIssuedAt(request.getApprovalInbox().getIssuedAt())
                        .setNumber(reply0.getDocument().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns an approval inbox.
     */
    public GetApprovalInboxResponse getApprovalInbox(GetApprovalInboxRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetApprovalInbox")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "GetApprovalInbox");
        return GetApprovalInboxResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .build())
                .build();
    }

    /**
     * Changes an approval inbox.
     */
    public UpdateApprovalInboxResponse updateApprovalInbox(UpdateApprovalInboxRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateApprovalInbox")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveApprovalInbox(request.getContext().getTenantId(), request.getApprovalInbox());

        var results0 = new ArrayList<SummarizeRouteBindingsResponse>();
        for (var element : request.getApprovalInbox().getLinesList()) {
            SummarizeRouteBindingsRequest itemRequest = SummarizeRouteBindingsRequest.newBuilder()
                .putAllAnnotations(request.getApprovalInbox().getMetadataMap())
                .setContext(request.getContext())
                .setDetails(SummarizeRouteBindingsRequest.Details.newBuilder()
                        .putAllFilters(element.getAttributesMap())
                        .build())
                .build();
            results0.add(edgegatewayRouteBindingQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeRouteBindings(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SummarizeRouteBindingsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ExportCloseChecklistsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportCloseChecklistsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getApprovalInbox().getDueAt())
                        .putAllLabels(request.getApprovalInbox().getMetadataMap())
                        .setOwnerId(reply0.getReportId())
                        .setRegion(request.getApprovalInbox().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = financeportalCloseChecklistQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportCloseChecklists(outbound1);
        var reply1 = ExportCloseChecklistsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var futures2 = request.getApprovalInbox().getAttachmentsList().stream()
                .map(element -> EscalateSupplierContractRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierContract(SupplierContract.newBuilder()
                        .addAttachments(SupplierContractAttachment.newBuilder()
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> suppliersSupplierContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateSupplierContract(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? EscalateSupplierContractResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getApprovalInbox().getApprovalInboxId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateApprovalInbox", request.getApprovalInbox().getApprovalInboxId());
        return UpdateApprovalInboxResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setBillingAddress(request.getApprovalInbox().getBillingAddress())
                        .setStatus(request.getApprovalInbox().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes an approval inbox.
     */
    public DeleteApprovalInboxResponse deleteApprovalInbox(DeleteApprovalInboxRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteApprovalInbox")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "DeleteApprovalInbox");

        var outbound0 = UnsubscribeFinanceWorkspaceUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeFinanceWorkspaceUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        RpcPort<UnsubscribeFinanceWorkspaceUpdatesRequest, UnsubscribeFinanceWorkspaceUpdatesResponse> port0 = RpcPort.adapt(financeportalFinanceWorkspaceNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::unsubscribeFinanceWorkspaceUpdates);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = DelegateSupplierContractApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(DelegateSupplierContractApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(switch (reply0.getFinanceWorkspace().getOriginCase()) {
                                    case CUSTOMER -> reply0.getFinanceWorkspace().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getFinanceWorkspace().getRequester().getEmail())
                                .setSupplierId(switch (reply0.getFinanceWorkspace().getOriginCase()) {
                                    case CUSTOMER -> reply0.getFinanceWorkspace().getCustomer().getSupplierId();
                                    default -> "";
                                })
                                .build())
                        .setDueAt(reply0.getFinanceWorkspace().getSla().getDueAt())
                        .build())
                .build();
        var reply1 = suppliersSupplierContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateSupplierContractApproval(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(CancelDepreciationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(reply0.getFinanceWorkspace().getRequester().getPhone())
                                        .build())
                                .build())
                        .build())
                .build()), CancelDepreciationWorkflowRequest.class);
        var reply2 = assetsDepreciationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelDepreciationWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getApprovalInboxId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteApprovalInbox", request.getApprovalInboxId());
        return DeleteApprovalInboxResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    public ListApprovalInboxesResponse listApprovalInboxes(ListApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "ListApprovalInboxes");

        var outbound0 = UpdateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = venuesVenueServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).updateVenue(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListApprovalInboxes", request.getPage().getPageToken());
        return ListApprovalInboxesResponse.newBuilder()
                .addApprovalInboxes(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getVenue().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getVenue().getAddress().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns several approval inboxes at once.
     */
    public BatchGetApprovalInboxesResponse batchGetApprovalInboxes(BatchGetApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "BatchGetApprovalInboxes");

        var outbound0 = ResolvePageCompositionsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.webbff.v1.PageCompositionQueryService");
        var selected0 = PageCompositionQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.resolvePageCompositions(outbound0);
        return BatchGetApprovalInboxesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    public SearchApprovalInboxesResponse searchApprovalInboxes(SearchApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "SearchApprovalInboxes");

        var outbound0 = UnsubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.audit.v1.RetentionPolicyNotificationService");
        var serviceName0 = "acme.audit.v1" + "." + "RetentionPolicyNotificationService";
        var methodName0 = "Uns" + "ubscribeRetentionPolicyUpdates";
        var descriptor0 = MethodDescriptor.<UnsubscribeRetentionPolicyUpdatesRequest, UnsubscribeRetentionPolicyUpdatesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(UnsubscribeRetentionPolicyUpdatesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(UnsubscribeRetentionPolicyUpdatesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchApprovalInboxes", request.getPage().getPageToken());
        return SearchApprovalInboxesResponse.newBuilder()
                .addApprovalInboxes(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRetentionPolicy().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of approval inboxes as they happen.
     */
    public WatchApprovalInboxesResponse watchApprovalInboxes(WatchApprovalInboxesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchApprovalInboxes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "WatchApprovalInboxes");

        var outbound0 = SubscribeCloseChecklistUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeCloseChecklistUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setName(request.getSubscriber().getName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.financeportal.v1.CloseChecklistNotificationService");
        var selected0 = CloseChecklistNotificationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.subscribeCloseChecklistUpdates(outbound0);

        var outbound1 = StartCloseChecklistWorkflowRequest.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getCloseChecklist().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter1 = new Function<StartCloseChecklistWorkflowRequest, StartCloseChecklistWorkflowResponse>() {
            @Override
            public StartCloseChecklistWorkflowResponse apply(StartCloseChecklistWorkflowRequest input) {
                return financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startCloseChecklistWorkflow(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = CountEInvoiceTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountEInvoiceTotalsRequest.Filter.newBuilder()
                        .setRegion(reply1.getCloseChecklist().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = invoicingEInvoiceReportingServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).countEInvoiceTotals(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchApprovalInboxes", request.getFilter().getQuery());
        return WatchApprovalInboxesResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCloseChecklist().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getCloseChecklist().getBillingAddress().getPostalCode())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setTotal(reply0.getCloseChecklist().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Creates an approval inbox.
     */
    public CloneApprovalInboxResponse cloneApprovalInbox(CloneApprovalInboxRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneApprovalInbox")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveApprovalInbox(request.getContext().getTenantId(), request.getApprovalInbox());

        var outbound0 = (SendRetentionPolicyTestNoticeRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(SendRetentionPolicyTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getApprovalInbox().getMetadataMap())
                        .build())
                .build()).toBuilder().build();
        var reply0 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendRetentionPolicyTestNotice(outbound0);

        List<UpdateSearchIndexResponse> results1 = request.getApprovalInbox().getLinesList().stream()
                .map(element -> UpdateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getApprovalInbox().getAudit().getCreatedBy())
                                .build())
                        .putComponents("primary", SearchIndexComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSearchIndex(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? UpdateSearchIndexResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ListApprovalInboxSourcesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListApprovalInboxSourcesRequest.Filter.newBuilder()
                        .setRegion(request.getApprovalInbox().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> financeportalApprovalInboxIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listApprovalInboxSources(value));
        log.atDebug().addKeyValue("auditValue", request.getApprovalInbox().getApprovalInboxId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneApprovalInbox", request.getApprovalInbox().getApprovalInboxId());
        return CloneApprovalInboxResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setBillingAddress(request.getApprovalInbox().getBillingAddress())
                        .addLines(ApprovalInboxLine.newBuilder()
                                .setTax(ApprovalInboxLineTax.newBuilder()
                                        .setAmount(reply0.getRetentionPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .setNumber(request.getApprovalInbox().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }
}
