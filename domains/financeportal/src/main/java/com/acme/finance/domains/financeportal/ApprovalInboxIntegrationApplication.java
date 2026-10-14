package com.acme.finance.domains.financeportal;

import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.EscalateBudgetRequest;
import com.acme.budgeting.v1.EscalateBudgetResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegmentApprovalServiceGrpc;
import com.acme.customers.v1.ListPendingCustomerSegmentsRequest;
import com.acme.customers.v1.ListPendingCustomerSegmentsResponse;
import com.acme.customers.v1.PreferenceReportingServiceGrpc;
import com.acme.customers.v1.SchedulePreferenceReportRequest;
import com.acme.customers.v1.SchedulePreferenceReportResponse;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistBankSettlement;
import com.acme.financeportal.v1.CloseChecklistCardSettlement;
import com.acme.financeportal.v1.CloseChecklistRef;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.ConnectApprovalInboxSourceRequest;
import com.acme.financeportal.v1.ConnectApprovalInboxSourceResponse;
import com.acme.financeportal.v1.DisconnectApprovalInboxSourceRequest;
import com.acme.financeportal.v1.DisconnectApprovalInboxSourceResponse;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.IngestApprovalInboxEventsRequest;
import com.acme.financeportal.v1.IngestApprovalInboxEventsResponse;
import com.acme.financeportal.v1.InspectApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.InspectApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ListApprovalInboxSourcesRequest;
import com.acme.financeportal.v1.ListApprovalInboxSourcesResponse;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsRequest;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsResponse;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesRequest;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesResponse;
import com.acme.financeportal.v1.ReplayApprovalInboxEventsRequest;
import com.acme.financeportal.v1.ReplayApprovalInboxEventsResponse;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.TestApprovalInboxConnectionRequest;
import com.acme.financeportal.v1.TestApprovalInboxConnectionResponse;
import com.acme.financeportal.v1.TestFinanceWorkspaceConnectionRequest;
import com.acme.financeportal.v1.TestFinanceWorkspaceConnectionResponse;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.AdvancePayStubWorkflowRequest;
import com.acme.payroll.v1.AdvancePayStubWorkflowResponse;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.risk.v1.StartRiskScenarioWorkflowRequest;
import com.acme.risk.v1.StartRiskScenarioWorkflowResponse;
import com.acme.webbff.v1.BatchGetPageCompositionsRequest;
import com.acme.webbff.v1.BatchGetPageCompositionsResponse;
import com.acme.webbff.v1.PageCompositionServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Connects external systems that send approval inbox events. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class ApprovalInboxIntegrationApplication {
    private static final Logger log = LoggerFactory.getLogger(ApprovalInboxIntegrationApplication.class);

    private final BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerSegmentApprovalServiceGrpc.CustomerSegmentApprovalServiceBlockingStub customersCustomerSegmentApprovalServiceBlocking;

    private final PreferenceReportingServiceGrpc.PreferenceReportingServiceBlockingStub customersPreferenceReportingServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking;

    private final FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking;

    private final ApprovalInboxIntegrationRequestMapper mapper;

    private final PayStubWorkflowServiceGrpc.PayStubWorkflowServiceBlockingStub payrollPayStubWorkflowServiceBlocking;

    private final FinanceportalRepository repository;

    private final RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking;

    private final PageCompositionServiceGrpc.PageCompositionServiceBlockingStub webbffPageCompositionServiceBlocking;

    public ApprovalInboxIntegrationApplication(
            BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking,
            ChannelRegistry channels,
            CustomerSegmentApprovalServiceGrpc.CustomerSegmentApprovalServiceBlockingStub customersCustomerSegmentApprovalServiceBlocking,
            PreferenceReportingServiceGrpc.PreferenceReportingServiceBlockingStub customersPreferenceReportingServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking,
            FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking,
            ApprovalInboxIntegrationRequestMapper mapper,
            PayStubWorkflowServiceGrpc.PayStubWorkflowServiceBlockingStub payrollPayStubWorkflowServiceBlocking,
            FinanceportalRepository repository,
            RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking,
            PageCompositionServiceGrpc.PageCompositionServiceBlockingStub webbffPageCompositionServiceBlocking) {
        this.budgetingBudgetApprovalServiceBlocking = budgetingBudgetApprovalServiceBlocking;
        this.channels = channels;
        this.customersCustomerSegmentApprovalServiceBlocking = customersCustomerSegmentApprovalServiceBlocking;
        this.customersPreferenceReportingServiceBlocking = customersPreferenceReportingServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.financeportalCloseChecklistWorkflowServiceBlocking = financeportalCloseChecklistWorkflowServiceBlocking;
        this.financeportalFinanceWorkspaceIntegrationServiceBlocking = financeportalFinanceWorkspaceIntegrationServiceBlocking;
        this.mapper = mapper;
        this.payrollPayStubWorkflowServiceBlocking = payrollPayStubWorkflowServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioWorkflowServiceBlocking = riskRiskScenarioWorkflowServiceBlocking;
        this.webbffPageCompositionServiceBlocking = webbffPageCompositionServiceBlocking;
    }

    /**
     * Connects an approval inbox.
     */
    public ConnectApprovalInboxSourceResponse connectApprovalInboxSource(ConnectApprovalInboxSourceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ConnectApprovalInboxSource")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "ConnectApprovalInboxSource");

        var outbound0 = BatchGetPageCompositionsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.retryRead(() -> webbffPageCompositionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetPageCompositions(outbound0));

        var outbound1 = DisconnectFinanceWorkspaceSourceRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(DisconnectFinanceWorkspaceSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.financeportal.v1.FinanceWorkspaceIntegrationService/DisconnectFinanceWorkspaceSource", outbound1);
        var reply1 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).disconnectFinanceWorkspaceSource(command1.payload());

        var outbound2 = ResumeCloseChecklistWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResumeCloseChecklistWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPhone(reply1.getFinanceWorkspace().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.financeportal.v1.CloseChecklistWorkflowService/ResumeCloseChecklistWorkflow", outbound2);
        var reply2 = financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeCloseChecklistWorkflow(command2.payload());
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ConnectApprovalInboxSource", request.getDetails().getEndpoint());
        return ConnectApprovalInboxSourceResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .build())
                .setOutcome(ConnectApprovalInboxSourceResponse.Outcome.newBuilder()
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Disconnects an approval inbox.
     */
    public DisconnectApprovalInboxSourceResponse disconnectApprovalInboxSource(DisconnectApprovalInboxSourceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DisconnectApprovalInboxSource")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "DisconnectApprovalInboxSource");

        var outbound0 = PreviewDocumentRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewDocument(value));

        var outbound1 = JsonCodec.roundTrip(ListCloseChecklistWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCloseChecklistWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getDocument().getIssuedAt())
                        .putAllLabels(reply0.getDocument().getMetadataMap())
                        .setRegion(reply0.getDocument().getBillingAddress().getRegion())
                        .build())
                .build(), ListCloseChecklistWorkflowsRequest.class);
        var reply1 = financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCloseChecklistWorkflows(outbound1);

        var outbound2 = InspectApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInboxId(request.getApprovalInboxId())
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.financeportal.v1.ApprovalInboxWorkflowService");
        var serviceName2 = "acme.financeportal.v1" + "." + "ApprovalInboxWorkflowService";
        var methodName2 = "Ins" + "pectApprovalInboxWorkflow";
        var descriptor2 = MethodDescriptor.<InspectApprovalInboxWorkflowRequest, InspectApprovalInboxWorkflowResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(InspectApprovalInboxWorkflowRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(InspectApprovalInboxWorkflowResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DisconnectApprovalInboxSource", request.getDetails().getEndpoint());
        return DisconnectApprovalInboxSourceResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getApprovalInbox().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Takes a stream of approval inboxes and answers once at the end.
     */
    public IngestApprovalInboxEventsResponse ingestApprovalInboxEvents(IngestApprovalInboxEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "IngestApprovalInboxEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveApprovalInbox(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = SignalCloseChecklistWorkflowRequest.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setBank(CloseChecklistBankSettlement.newBuilder()
                                .setAccountHolder(request.getRecord().getBank().getAccountHolder())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getRecord().getBillingAddress().getCountryCode())
                                .addAllLines(request.getRecord().getBillingAddress().getLinesList())
                                .setLocality(request.getRecord().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            SignalCloseChecklistWorkflowResponse fetch(SignalCloseChecklistWorkflowRequest input) {
                return financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalCloseChecklistWorkflow(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = StartCloseChecklistWorkflowRequest.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRecord().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getCloseChecklist().getBillingAddress().getRecipient())
                                .build())
                        .setCard(CloseChecklistCardSettlement.newBuilder()
                                .setHolderName(switch (request.getRecord().getSettlementCase()) {
                                    case CARD -> request.getRecord().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startCloseChecklistWorkflow(outbound1);

        var outbound2 = CancelCloseChecklistWorkflowRequest.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCard(CloseChecklistCardSettlement.newBuilder()
                                .setHolderName(switch (request.getRecord().getSettlementCase()) {
                                    case CARD -> request.getRecord().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelCloseChecklistWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "IngestApprovalInboxEvents", request.getHeader().getSource());
        return IngestApprovalInboxEventsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Replays an approval inbox.
     */
    public ReplayApprovalInboxEventsResponse replayApprovalInboxEvents(ReplayApprovalInboxEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReplayApprovalInboxEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "ReplayApprovalInboxEvents");

        var outbound0 = SchedulePreferenceReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        Function<SchedulePreferenceReportRequest, SchedulePreferenceReportResponse> operation0 = customersPreferenceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::schedulePreferenceReport;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = ListPendingCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingCustomerSegmentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getReceivedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getDetails().getMessageId())
                        .build())
                .build();
        final class Stage1Adapter {
            ListPendingCustomerSegmentsResponse fetch(ListPendingCustomerSegmentsRequest input) {
                return customersCustomerSegmentApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingCustomerSegments(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = mapper.replayApprovalInboxEventsStage2(request);
        var reply2 = financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseCloseChecklistWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getEndpoint()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReplayApprovalInboxEvents", request.getDetails().getEndpoint());
        return ReplayApprovalInboxEventsResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getCloseChecklist().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply2.getCloseChecklist().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setOutcome(ReplayApprovalInboxEventsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of approval inboxes with a peer.
     */
    public RelayApprovalInboxMessagesResponse relayApprovalInboxMessages(RelayApprovalInboxMessagesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RelayApprovalInboxMessages")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveApprovalInbox(request.getContext().getTenantId(), request.getChange());
        }

        var outbound0 = TestFinanceWorkspaceConnectionRequest.newBuilder()
                .putAllAnnotations(request.getChange().getMetadataMap())
                .setContext(request.getContext())
                .setDetails(TestFinanceWorkspaceConnectionRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(switch (request.getFrameCase()) {
                            case CHANGE -> request.getChange().getCounterparty().getCloseChecklistId();
                            default -> "";
                        })
                        .setReceivedAt(request.getChange().getAudit().getCreatedAt())
                        .build())
                .build();
        var reply0 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).testFinanceWorkspaceConnection(outbound0);

        var outbound1 = EscalateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setCard(BudgetCardFunding.newBuilder()
                                .setToken(request.getChange().getCard().getToken())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EscalateBudgetRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply0.getFinanceWorkspace().getRequester().getEmail())
                                .setUserId(reply0.getFinanceWorkspace().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            EscalateBudgetResponse fetch(EscalateBudgetRequest input) {
                return budgetingBudgetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateBudget(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = (StartRiskScenarioWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(StartRiskScenarioWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply1.getBudget().getContact().getEmail())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFinanceWorkspace().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = riskRiskScenarioWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startRiskScenarioWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getChange().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RelayApprovalInboxMessages", request.getChange().getNumber());
        return RelayApprovalInboxMessagesResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getChange().getApprovalInboxId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFinanceWorkspace().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getBudget().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(request.getChange().getBillingAddress())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    public ListApprovalInboxSourcesResponse listApprovalInboxSources(ListApprovalInboxSourcesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListApprovalInboxSources")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "ListApprovalInboxSources");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListApprovalInboxSources", request.getPage().getPageToken());
        return ListApprovalInboxSourcesResponse.getDefaultInstance();
    }

    /**
     * Tests an approval inbox.
     */
    public TestApprovalInboxConnectionResponse testApprovalInboxConnection(TestApprovalInboxConnectionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TestApprovalInboxConnection")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "TestApprovalInboxConnection");

        var outbound0 = AcknowledgeFinanceWorkspaceEventRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AcknowledgeFinanceWorkspaceEventRequest.Details.newBuilder()
                        .setEndpoint(request.getDetails().getEndpoint())
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getDetails().getMessageId())
                        .setSourceSystem(request.getDetails().getSourceSystem())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).acknowledgeFinanceWorkspaceEvent(value));

        var outbound1 = PauseCloseChecklistWorkflowRequest.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getFinanceWorkspace().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PauseCloseChecklistWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply0.getFinanceWorkspace().getAssignee().getEmail())
                                .setPhone(reply0.getFinanceWorkspace().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(financeportalCloseChecklistWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseCloseChecklistWorkflow(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = AdvancePayStubWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvancePayStubWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getFinanceWorkspace().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(reply1.getCloseChecklist().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<AdvancePayStubWorkflowRequest, AdvancePayStubWorkflowResponse> operation2 = payrollPayStubWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::advancePayStubWorkflow;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getReason()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TestApprovalInboxConnection", request.getReason());
        return TestApprovalInboxConnectionResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getPayStub().getAudit().getCreatedAt())
                                .setEtag(reply0.getFinanceWorkspace().getAudit().getEtag())
                                .build())
                        .setCounterparty(CloseChecklistRef.newBuilder()
                                .setCloseChecklistId(reply1.getCloseChecklist().getCloseChecklistId())
                                .build())
                        .build())
                .setOutcome(TestApprovalInboxConnectionResponse.Outcome.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }
}
