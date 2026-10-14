package com.acme.finance.domains.financeportal;

import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationApprovalServiceGrpc;
import com.acme.accounting.v1.AllocationBlockAction;
import com.acme.accounting.v1.RecallAllocationRequest;
import com.acme.accounting.v1.RecallAllocationResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionRequest;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseSla;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.PauseCollectionCaseWorkflowRequest;
import com.acme.collections.v1.PauseCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.ListAttestationReportsRequest;
import com.acme.compliance.v1.ListAttestationReportsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordWorkflowServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.StartConsentRecordWorkflowRequest;
import com.acme.customers.v1.StartConsentRecordWorkflowResponse;
import com.acme.datahub.v1.ExportWorkspacesRequest;
import com.acme.datahub.v1.ExportWorkspacesResponse;
import com.acme.datahub.v1.WatchWorkspacesRequest;
import com.acme.datahub.v1.WatchWorkspacesResponse;
import com.acme.datahub.v1.WorkspaceQueryServiceGrpc;
import com.acme.datahub.v1.WorkspaceServiceGrpc;
import com.acme.documents.v1.CreateDocumentRequest;
import com.acme.documents.v1.CreateDocumentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.employeeportal.v1.FindPortalHomesRequest;
import com.acme.employeeportal.v1.FindPortalHomesResponse;
import com.acme.employeeportal.v1.PortalHomeQueryServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventRequest;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventResponse;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.ConnectApprovalInboxSourceRequest;
import com.acme.financeportal.v1.ConnectApprovalInboxSourceResponse;
import com.acme.financeportal.v1.ConnectCloseChecklistSourceRequest;
import com.acme.financeportal.v1.ConnectCloseChecklistSourceResponse;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.DisconnectCloseChecklistSourceRequest;
import com.acme.financeportal.v1.DisconnectCloseChecklistSourceResponse;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.IngestCloseChecklistEventsRequest;
import com.acme.financeportal.v1.IngestCloseChecklistEventsResponse;
import com.acme.financeportal.v1.ListCloseChecklistSourcesRequest;
import com.acme.financeportal.v1.ListCloseChecklistSourcesResponse;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesRequest;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesResponse;
import com.acme.financeportal.v1.ReplayApprovalInboxEventsRequest;
import com.acme.financeportal.v1.ReplayApprovalInboxEventsResponse;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsRequest;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsResponse;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.TestApprovalInboxConnectionRequest;
import com.acme.financeportal.v1.TestApprovalInboxConnectionResponse;
import com.acme.financeportal.v1.TestCloseChecklistConnectionRequest;
import com.acme.financeportal.v1.TestCloseChecklistConnectionResponse;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.suppliers.v1.SubmitSupplierContractRequest;
import com.acme.suppliers.v1.SubmitSupplierContractResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractCardSettlement;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Connects external systems that send close checklist events. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class CloseChecklistIntegrationApplication {
    private static final Logger log = LoggerFactory.getLogger(CloseChecklistIntegrationApplication.class);

    private final AllocationApprovalServiceGrpc.AllocationApprovalServiceBlockingStub accountingAllocationApprovalServiceBlocking;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceFutureStub collectionsCollectionCaseWorkflowServiceFuture;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final ConsentRecordWorkflowServiceGrpc.ConsentRecordWorkflowServiceBlockingStub customersConsentRecordWorkflowServiceBlocking;

    private final WorkspaceQueryServiceGrpc.WorkspaceQueryServiceBlockingStub datahubWorkspaceQueryServiceBlocking;

    private final WorkspaceServiceGrpc.WorkspaceServiceBlockingStub datahubWorkspaceServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final PortalHomeQueryServiceGrpc.PortalHomeQueryServiceFutureStub employeeportalPortalHomeQueryServiceFuture;

    private final ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceBlockingStub financeportalApprovalInboxIntegrationServiceBlocking;

    private final ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking;

    private final FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final FinanceportalRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking;

    public CloseChecklistIntegrationApplication(
            AllocationApprovalServiceGrpc.AllocationApprovalServiceBlockingStub accountingAllocationApprovalServiceBlocking,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceFutureStub collectionsCollectionCaseWorkflowServiceFuture,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            ConsentRecordWorkflowServiceGrpc.ConsentRecordWorkflowServiceBlockingStub customersConsentRecordWorkflowServiceBlocking,
            WorkspaceQueryServiceGrpc.WorkspaceQueryServiceBlockingStub datahubWorkspaceQueryServiceBlocking,
            WorkspaceServiceGrpc.WorkspaceServiceBlockingStub datahubWorkspaceServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            PortalHomeQueryServiceGrpc.PortalHomeQueryServiceFutureStub employeeportalPortalHomeQueryServiceFuture,
            ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceBlockingStub financeportalApprovalInboxIntegrationServiceBlocking,
            ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking,
            FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            FinanceportalRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking) {
        this.accountingAllocationApprovalServiceBlocking = accountingAllocationApprovalServiceBlocking;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseWorkflowServiceFuture = collectionsCollectionCaseWorkflowServiceFuture;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.customersConsentRecordWorkflowServiceBlocking = customersConsentRecordWorkflowServiceBlocking;
        this.datahubWorkspaceQueryServiceBlocking = datahubWorkspaceQueryServiceBlocking;
        this.datahubWorkspaceServiceBlocking = datahubWorkspaceServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.employeeportalPortalHomeQueryServiceFuture = employeeportalPortalHomeQueryServiceFuture;
        this.financeportalApprovalInboxIntegrationServiceBlocking = financeportalApprovalInboxIntegrationServiceBlocking;
        this.financeportalApprovalInboxWorkflowServiceBlocking = financeportalApprovalInboxWorkflowServiceBlocking;
        this.financeportalFinanceWorkspaceIntegrationServiceBlocking = financeportalFinanceWorkspaceIntegrationServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractApprovalServiceBlocking = suppliersSupplierContractApprovalServiceBlocking;
    }

    /**
     * Connects a close checklist.
     */
    public ConnectCloseChecklistSourceResponse connectCloseChecklistSource(ConnectCloseChecklistSourceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ConnectCloseChecklistSource")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "ConnectCloseChecklistSource");

        var outbound0 = JsonCodec.roundTrip(RecallAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setBlock(AllocationBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .build())
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build(), RecallAllocationRequest.class);
        var reply0 = accountingAllocationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallAllocation(outbound0);

        var outbound1 = ListAttestationReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAttestationReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getAllocation().getAudit().getUpdatedAt())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAttestationReports(value));

        var outbound2 = ReplayApprovalInboxEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReplayApprovalInboxEventsRequest.Details.newBuilder()
                        .setEndpoint(request.getDetails().getEndpoint())
                        .setEnvelope(Any.pack(request.getContext()))
                        .setReceivedAt(request.getDetails().getReceivedAt())
                        .setSourceSystem(request.getDetails().getSourceSystem())
                        .build())
                .setReason(reply0.getAllocation().getBlock().getReason())
                .build();
        var channel2 = channels.lookup("acme.financeportal.v1.ApprovalInboxIntegrationService");
        var selected2 = ApprovalInboxIntegrationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.replayApprovalInboxEvents(outbound2);

        var outbound3 = TestApprovalInboxConnectionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(TestApprovalInboxConnectionRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .setReason(reply0.getAllocation().getBlock().getReason())
                .build();
        var channel3 = channels.lookup("acme.financeportal.v1.ApprovalInboxIntegrationService");
        var serviceName3 = "acme.financeportal.v1" + "." + "ApprovalInboxIntegrationService";
        var methodName3 = "Tes" + "tApprovalInboxConnection";
        var descriptor3 = MethodDescriptor.<TestApprovalInboxConnectionRequest, TestApprovalInboxConnectionResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(TestApprovalInboxConnectionRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(TestApprovalInboxConnectionResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getMessageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ConnectCloseChecklistSource", request.getDetails().getMessageId());
        return ConnectCloseChecklistSourceResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply3.getApprovalInbox().getCounterparty().getDisplayName())
                                .build())
                        .setDueAt(reply2.getApprovalInbox().getDueAt())
                        .build())
                .setOutcome(ConnectCloseChecklistSourceResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Disconnects a close checklist.
     */
    public DisconnectCloseChecklistSourceResponse disconnectCloseChecklistSource(DisconnectCloseChecklistSourceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DisconnectCloseChecklistSource")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "DisconnectCloseChecklistSource");

        var outbound0 = ConnectApprovalInboxSourceRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ConnectApprovalInboxSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getDetails().getMessageId())
                        .setReceivedAt(request.getDetails().getReceivedAt())
                        .setSourceSystem(request.getDetails().getSourceSystem())
                        .build())
                .build();
        var adapter0 = new Function<ConnectApprovalInboxSourceRequest, ConnectApprovalInboxSourceResponse>() {
            @Override
            public ConnectApprovalInboxSourceResponse apply(ConnectApprovalInboxSourceRequest input) {
                return financeportalApprovalInboxIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).connectApprovalInboxSource(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = SubmitSupplierContractRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setCard(SupplierContractCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getApprovalInbox().getSettlementCase()) {
                                    case CARD -> reply0.getApprovalInbox().getCard().getLast4();
                                    default -> "";
                                })
                                .setNetwork(switch (reply0.getApprovalInbox().getSettlementCase()) {
                                    case CARD -> reply0.getApprovalInbox().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = suppliersSupplierContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitSupplierContract(outbound1);

        var outbound2 = FindPortalHomesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPortalHomesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getApprovalInbox().getAudit().getUpdatedAt())
                        .setRegion(reply1.getSupplierContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = employeeportalPortalHomeQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).findPortalHomes(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DisconnectCloseChecklistSource", request.getDetails().getEndpoint());
        return DisconnectCloseChecklistSourceResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .build())
                .setOutcome(DisconnectCloseChecklistSourceResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Takes a stream of close checklists and answers once at the end.
     */
    public IngestCloseChecklistEventsResponse ingestCloseChecklistEvents(IngestCloseChecklistEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "IngestCloseChecklistEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveCloseChecklist(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = PauseApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getRecord().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getRecord().getBillingAddress().getPostalCode())
                                .setRegion(request.getRecord().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseApprovalInboxWorkflow(value)).orElseThrow();

        var outbound1 = ReplayFinanceWorkspaceEventsRequest.newBuilder()
                .putAllAnnotations(request.getRecord().getMetadataMap())
                .setContext(request.getContext())
                .setDetails(ReplayFinanceWorkspaceEventsRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(reply0.getOutcome().getMessage())
                        .setReceivedAt(reply0.getApprovalInbox().getAudit().getUpdatedAt())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.financeportal.v1.FinanceWorkspaceIntegrationService/ReplayFinanceWorkspaceEvents", outbound1);
        var reply1 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).replayFinanceWorkspaceEvents(command1.payload());

        var outbound2 = ExplainAccessPolicyDecisionRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getFinanceWorkspace().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getCounterparty().getEmail();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainAccessPolicyDecision(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "IngestCloseChecklistEvents", request.getHeader().getSource());
        return IngestCloseChecklistEventsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Replays a close checklist.
     */
    public ReplayCloseChecklistEventsResponse replayCloseChecklistEvents(ReplayCloseChecklistEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReplayCloseChecklistEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "ReplayCloseChecklistEvents");

        var outbound0 = ConnectFinanceWorkspaceSourceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ConnectFinanceWorkspaceSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getDetails().getMessageId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).connectFinanceWorkspaceSource(outbound0), rpcExecutor);

        var outbound1 = ScheduleFiscalPeriodExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFiscalPeriodExport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReplayCloseChecklistEvents", request.getDetails().getEndpoint());
        return ReplayCloseChecklistEventsResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getFinanceWorkspace().getAudit().getCreatedBy())
                                .build())
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getFinanceWorkspace().getCustomer().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Acknowledges a close checklist.
     */
    public AcknowledgeCloseChecklistEventResponse acknowledgeCloseChecklistEvent(AcknowledgeCloseChecklistEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AcknowledgeCloseChecklistEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "AcknowledgeCloseChecklistEvent");

        var outbound0 = CreateDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getDetails().getReceivedAt())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createDocument(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = DisconnectFinanceWorkspaceSourceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(DisconnectFinanceWorkspaceSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getDetails().getMessageId())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).disconnectFinanceWorkspaceSource(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = BeanCopies.copy(ReplayFinanceWorkspaceEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReplayFinanceWorkspaceEventsRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getDetails().getMessageId())
                        .build())
                .build(), ReplayFinanceWorkspaceEventsRequest.class);
        var reply2 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).replayFinanceWorkspaceEvents(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AcknowledgeCloseChecklistEvent", request.getDetails().getEndpoint());
        return AcknowledgeCloseChecklistEventResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setDueAt(reply1.getFinanceWorkspace().getSla().getDueAt())
                        .build())
                .setOutcome(AcknowledgeCloseChecklistEventResponse.Outcome.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of close checklists with a peer.
     */
    public RelayCloseChecklistMessagesResponse relayCloseChecklistMessages(RelayCloseChecklistMessagesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RelayCloseChecklistMessages")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveCloseChecklist(request.getContext().getTenantId(), request.getChange());
        }

        var outbound0 = AcknowledgeFinanceWorkspaceEventRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AcknowledgeFinanceWorkspaceEventRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setReceivedAt(request.getChange().getAudit().getUpdatedAt())
                        .build())
                .build();
        final class Stage0Adapter {
            AcknowledgeFinanceWorkspaceEventResponse fetch(AcknowledgeFinanceWorkspaceEventRequest input) {
                return financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).acknowledgeFinanceWorkspaceEvent(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = PauseCollectionCaseWorkflowRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getFinanceWorkspace().getRequester().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getFinanceWorkspace().getAudit().getEtag())
                                .build())
                        .setSla(CollectionCaseSla.newBuilder()
                                .setBreached(reply0.getFinanceWorkspace().getSla().getBreached())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = collectionsCollectionCaseWorkflowServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).pauseCollectionCaseWorkflow(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getChange().getCloseChecklistId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RelayCloseChecklistMessages", request.getChange().getCloseChecklistId());
        return RelayCloseChecklistMessagesResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setBillingAddress(request.getChange().getBillingAddress())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getFinanceWorkspace().getAssignee().getEmail())
                                .build())
                        .setDueAt(reply0.getFinanceWorkspace().getSla().getDueAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setSequence(request.getAck().getSequence())
                .build();
    }

    /**
     * Lists close checklists a page at a time.
     */
    public ListCloseChecklistSourcesResponse listCloseChecklistSources(ListCloseChecklistSourcesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListCloseChecklistSources")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getContext().getTraceId(), "ListCloseChecklistSources");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListCloseChecklistSources", request.getPage().getPageToken());
        return ListCloseChecklistSourcesResponse.getDefaultInstance();
    }

    /**
     * Tests a close checklist.
     */
    public TestCloseChecklistConnectionResponse testCloseChecklistConnection(TestCloseChecklistConnectionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TestCloseChecklistConnection")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "TestCloseChecklistConnection");

        var outbound0 = ExportWorkspacesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportWorkspacesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getReceivedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getMessageId())
                        .build())
                .build();
        var stream0 = datahubWorkspaceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportWorkspaces(outbound0);
        var reply0 = ExportWorkspacesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = WatchWorkspacesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchWorkspacesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getReceivedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream1 = datahubWorkspaceServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchWorkspaces(outbound1);
        var reply1 = WatchWorkspacesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = StartConsentRecordWorkflowRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(StartConsentRecordWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> customersConsentRecordWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startConsentRecordWorkflow(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TestCloseChecklistConnection", request.getDetails().getEndpoint());
        return TestCloseChecklistConnectionResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .build())
                .setOutcome(TestCloseChecklistConnectionResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .build();
    }
}
