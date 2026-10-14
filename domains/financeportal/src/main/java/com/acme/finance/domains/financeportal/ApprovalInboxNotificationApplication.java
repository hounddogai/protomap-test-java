package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.LintAuditTrailsRequest;
import com.acme.audit.v1.LintAuditTrailsResponse;
import com.acme.audit.v1.ListAuditEventSubscriptionsRequest;
import com.acme.audit.v1.ListAuditEventSubscriptionsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventRequest;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistCardSettlement;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistRef;
import com.acme.financeportal.v1.ConnectCloseChecklistSourceRequest;
import com.acme.financeportal.v1.ConnectCloseChecklistSourceResponse;
import com.acme.financeportal.v1.DisconnectApprovalInboxSourceRequest;
import com.acme.financeportal.v1.DisconnectApprovalInboxSourceResponse;
import com.acme.financeportal.v1.GetApprovalInboxPreferencesRequest;
import com.acme.financeportal.v1.GetApprovalInboxPreferencesResponse;
import com.acme.financeportal.v1.ListApprovalInboxSubscriptionsRequest;
import com.acme.financeportal.v1.ListApprovalInboxSubscriptionsResponse;
import com.acme.financeportal.v1.ListCloseChecklistSourcesRequest;
import com.acme.financeportal.v1.ListCloseChecklistSourcesResponse;
import com.acme.financeportal.v1.NotifyApprovalInboxWatchersRequest;
import com.acme.financeportal.v1.NotifyApprovalInboxWatchersResponse;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesRequest;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesResponse;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsRequest;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsResponse;
import com.acme.financeportal.v1.SendApprovalInboxTestNoticeRequest;
import com.acme.financeportal.v1.SendApprovalInboxTestNoticeResponse;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.StreamApprovalInboxNoticesRequest;
import com.acme.financeportal.v1.StreamApprovalInboxNoticesResponse;
import com.acme.financeportal.v1.SubscribeApprovalInboxUpdatesRequest;
import com.acme.financeportal.v1.SubscribeApprovalInboxUpdatesResponse;
import com.acme.financeportal.v1.TestApprovalInboxConnectionRequest;
import com.acme.financeportal.v1.TestApprovalInboxConnectionResponse;
import com.acme.financeportal.v1.TestCloseChecklistConnectionRequest;
import com.acme.financeportal.v1.TestCloseChecklistConnectionResponse;
import com.acme.financeportal.v1.UnsubscribeApprovalInboxUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeApprovalInboxUpdatesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.PauseCoverageWorkflowRequest;
import com.acme.insurance.v1.PauseCoverageWorkflowResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferLine;
import com.acme.treasury.v1.CommentFxHedgeApprovalRequest;
import com.acme.treasury.v1.CommentFxHedgeApprovalResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeApprovalServiceGrpc;
import com.acme.treasury.v1.SubmitBankTransferRequest;
import com.acme.treasury.v1.SubmitBankTransferResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Tells the people who follow an approval inbox what changed. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class ApprovalInboxNotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(ApprovalInboxNotificationApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceBlockingStub financeportalApprovalInboxIntegrationServiceBlocking;

    private final ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking;

    private final CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceStub financeportalCloseChecklistIntegrationServiceAsync;

    private final CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking;

    private final CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final FinanceportalRepository repository;

    private final FxHedgeApprovalServiceGrpc.FxHedgeApprovalServiceBlockingStub treasuryFxHedgeApprovalServiceBlocking;

    public ApprovalInboxNotificationApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceBlockingStub financeportalApprovalInboxIntegrationServiceBlocking,
            ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking,
            CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceStub financeportalCloseChecklistIntegrationServiceAsync,
            CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking,
            CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            FinanceportalRepository repository,
            FxHedgeApprovalServiceGrpc.FxHedgeApprovalServiceBlockingStub treasuryFxHedgeApprovalServiceBlocking) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.financeportalApprovalInboxIntegrationServiceBlocking = financeportalApprovalInboxIntegrationServiceBlocking;
        this.financeportalApprovalInboxWorkflowServiceBlocking = financeportalApprovalInboxWorkflowServiceBlocking;
        this.financeportalCloseChecklistIntegrationServiceAsync = financeportalCloseChecklistIntegrationServiceAsync;
        this.financeportalCloseChecklistIntegrationServiceBlocking = financeportalCloseChecklistIntegrationServiceBlocking;
        this.insuranceCoverageWorkflowServiceBlocking = insuranceCoverageWorkflowServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.repository = repository;
        this.treasuryFxHedgeApprovalServiceBlocking = treasuryFxHedgeApprovalServiceBlocking;
    }

    public record GetApprovalInboxPreferencesStage0Requested(ListCloseChecklistSourcesRequest request, CompletableFuture<ListCloseChecklistSourcesResponse> completion, io.grpc.Context context) {}

    /**
     * Subscribes an approval inbox.
     */
    public SubscribeApprovalInboxUpdatesResponse subscribeApprovalInboxUpdates(SubscribeApprovalInboxUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubscribeApprovalInboxUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "SubscribeApprovalInboxUpdates");

        var builder0 = TestCloseChecklistConnectionRequest.newBuilder();
        builder0.putAllAnnotations(request.getAnnotationsMap());
        builder0.setContext(request.getContext());
        builder0.setDetails(TestCloseChecklistConnectionRequest.Details.newBuilder()
                .setEnvelope(Any.pack(request.getContext()))
                .setMessageId(request.getDetails().getTemplateId())
                .build());
        builder0.setReason(request.getReason());
        var outbound0 = builder0.build();
        var reply0 = financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).testCloseChecklistConnection(outbound0);

        var outbound1 = RecordRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCloseChecklist().getAudit().getCreatedBy())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getCloseChecklist().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordRetentionPolicyVersion(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = TestApprovalInboxConnectionRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setApprovalInboxId(request.getApprovalInboxId())
                .setContext(request.getContext())
                .setDetails(TestApprovalInboxConnectionRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(reply0.getOutcome().getMessage())
                        .build())
                .setReason(switch (reply1.getRetentionPolicy().getActionCase()) {
                    case BLOCK -> reply1.getRetentionPolicy().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply2 = financeportalApprovalInboxIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).testApprovalInboxConnection(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubscribeApprovalInboxUpdates", request.getDetails().getRecipient().getEmail());
        return SubscribeApprovalInboxUpdatesResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getApprovalInbox().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getCloseChecklist().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getApprovalInbox().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setOutcome(SubscribeApprovalInboxUpdatesResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Unsubscribes an approval inbox.
     */
    public UnsubscribeApprovalInboxUpdatesResponse unsubscribeApprovalInboxUpdates(UnsubscribeApprovalInboxUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnsubscribeApprovalInboxUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "UnsubscribeApprovalInboxUpdates");

        var outbound0 = ReplayCloseChecklistEventsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ReplayCloseChecklistEventsRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getDetails().getTemplateId())
                        .setReceivedAt(request.getContext().getDeadline())
                        .build())
                .setReason(request.getReason())
                .build();
        Function<ReplayCloseChecklistEventsRequest, ReplayCloseChecklistEventsResponse> operation0 = financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::replayCloseChecklistEvents;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(AcknowledgeCloseChecklistEventRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setCloseChecklistId(reply0.getCloseChecklist().getCloseChecklistId())
                .setContext(request.getContext())
                .setDetails(AcknowledgeCloseChecklistEventRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setReceivedAt(reply0.getCloseChecklist().getAudit().getCreatedAt())
                        .build())
                .setReason(request.getReason())
                .build()), AcknowledgeCloseChecklistEventRequest.class);
        var reply1 = financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).acknowledgeCloseChecklistEvent(outbound1);

        var outbound2 = LintAuditTrailsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCloseChecklist().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getCloseChecklist().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAuditTrails(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnsubscribeApprovalInboxUpdates", request.getDetails().getRecipient().getEmail());
        return UnsubscribeApprovalInboxUpdatesResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setBillingAddress(reply1.getCloseChecklist().getBillingAddress())
                        .setCounterparty(CloseChecklistRef.newBuilder()
                                .setCloseChecklistId(reply0.getCloseChecklist().getCloseChecklistId())
                                .build())
                        .build())
                .setOutcome(UnsubscribeApprovalInboxUpdatesResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Notifys an approval inbox.
     */
    public NotifyApprovalInboxWatchersResponse notifyApprovalInboxWatchers(NotifyApprovalInboxWatchersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NotifyApprovalInboxWatchers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "NotifyApprovalInboxWatchers");

        var outbound0 = PauseCoverageWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PauseCoverageWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setName(request.getDetails().getRecipient().getName())
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .build();
        RpcPort<PauseCoverageWorkflowRequest, PauseCoverageWorkflowResponse> port0 = RpcPort.adapt(insuranceCoverageWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::pauseCoverageWorkflow);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = SubmitBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .addLines(BankTransferLine.newBuilder()
                                .setAmount(reply0.getCoverage().getDiscount().getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SubmitBankTransferRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(reply0.getCoverage().getOwner().getUserId())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.treasury.v1.BankTransferApprovalService");
        var serviceName1 = "acme.treasury.v1" + "." + "BankTransferApprovalService";
        var methodName1 = "Sub" + "mitBankTransfer";
        var descriptor1 = MethodDescriptor.<SubmitBankTransferRequest, SubmitBankTransferResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(SubmitBankTransferRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SubmitBankTransferResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = SignalApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setCounterparty(CloseChecklistRef.newBuilder()
                                .setDisplayName(reply0.getCoverage().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter2 = new Function<SignalApprovalInboxWorkflowRequest, SignalApprovalInboxWorkflowResponse>() {
            @Override
            public SignalApprovalInboxWorkflowResponse apply(SignalApprovalInboxWorkflowRequest input) {
                return financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalApprovalInboxWorkflow(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NotifyApprovalInboxWatchers", request.getDetails().getRecipient().getPhone());
        return NotifyApprovalInboxWatchersResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setBillingAddress(reply1.getBankTransfer().getBillingAddress())
                        .putAllMetadata(reply1.getBankTransfer().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    public ListApprovalInboxSubscriptionsResponse listApprovalInboxSubscriptions(ListApprovalInboxSubscriptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListApprovalInboxSubscriptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "ListApprovalInboxSubscriptions");

        var outbound0 = ListCloseChecklistSourcesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCloseChecklistSourcesRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build();
        final class Stage0Adapter {
            ListCloseChecklistSourcesResponse fetch(ListCloseChecklistSourcesRequest input) {
                return financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCloseChecklistSources(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListApprovalInboxSubscriptions", request.getFilter().getQuery());
        return ListApprovalInboxSubscriptionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Sends an approval inbox.
     */
    public SendApprovalInboxTestNoticeResponse sendApprovalInboxTestNotice(SendApprovalInboxTestNoticeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SendApprovalInboxTestNotice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "SendApprovalInboxTestNotice");

        var outbound0 = DisconnectApprovalInboxSourceRequest.newBuilder()
                .setApprovalInboxId(request.getApprovalInboxId())
                .setContext(request.getContext())
                .setDetails(DisconnectApprovalInboxSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .build())
                .build();
        var reply0 = financeportalApprovalInboxIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).disconnectApprovalInboxSource(outbound0);

        var outbound1 = ListAuditEventSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventSubscriptionsRequest.Filter.newBuilder()
                        .setRegion(reply0.getApprovalInbox().getBillingAddress().getRegion())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.audit.v1.AuditEventNotificationService/ListAuditEventSubscriptions", outbound1);
        var reply1 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEventSubscriptions(command1.payload());

        var outbound2 = ConnectCloseChecklistSourceRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setCloseChecklistId(reply0.getApprovalInbox().getCounterparty().getCloseChecklistId())
                .setContext(request.getContext())
                .setDetails(ConnectCloseChecklistSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(reply0.getResult().getMessage())
                        .build())
                .build();
        var reply2 = financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).connectCloseChecklistSource(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SendApprovalInboxTestNotice", request.getDetails().getRecipient().getEmail());
        return SendApprovalInboxTestNoticeResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .setIssuedAt(reply2.getCloseChecklist().getIssuedAt())
                        .build())
                .setOutcome(SendApprovalInboxTestNoticeResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns an approval inbox.
     */
    public GetApprovalInboxPreferencesResponse getApprovalInboxPreferences(GetApprovalInboxPreferencesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetApprovalInboxPreferences")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getApprovalInboxId(), "GetApprovalInboxPreferences");

        var outbound0 = ListCloseChecklistSourcesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCloseChecklistSourcesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getApprovalInboxId())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ListCloseChecklistSourcesResponse>();
        events.publishEvent(new GetApprovalInboxPreferencesStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return GetApprovalInboxPreferencesResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setApprovalInboxId(request.getApprovalInboxId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Streams the changes of approval inboxes as they happen.
     */
    public StreamApprovalInboxNoticesResponse streamApprovalInboxNotices(StreamApprovalInboxNoticesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamApprovalInboxNotices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ApprovalInbox", request.getContext().getTraceId(), "StreamApprovalInboxNotices");

        var outbound0 = CommentFxHedgeApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentFxHedgeApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval", outbound0);
        var reply0 = treasuryFxHedgeApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentFxHedgeApproval(command0.payload());

        var outbound1 = RelayCloseChecklistMessagesRequest.newBuilder()
                .setChange(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .setCard(CloseChecklistCardSettlement.newBuilder()
                                .setLast4(reply0.getFxHedge().getCard().getLast4())
                                .setToken(reply0.getFxHedge().getCard().getToken())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getFxHedge().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var collector1 = new StreamCollector<>(RelayCloseChecklistMessagesResponse.getDefaultInstance());
        var input1 = financeportalCloseChecklistIntegrationServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).relayCloseChecklistMessages(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = RpcSupport.unpack(Any.pack(RecordLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setCard(LedgerAccountCardFunding.newBuilder()
                                .setHolderName(reply0.getFxHedge().getCard().getHolderName())
                                .setNetwork(reply0.getFxHedge().getCard().getNetwork())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setDisplayName(reply0.getFxHedge().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build()), RecordLedgerAccountVersionRequest.class);
        var reply2 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordLedgerAccountVersion(outbound2);

        var outbound3 = ConnectCloseChecklistSourceRequest.newBuilder()
                .putAllAnnotations(reply0.getResult().getDetailsMap())
                .setContext(request.getContext())
                .setDetails(ConnectCloseChecklistSourceRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(reply0.getOutcome().getMessage())
                        .setReceivedAt(reply2.getLedgerAccount().getAudit().getUpdatedAt())
                        .build())
                .build();
        var reply3 = financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).connectCloseChecklistSource(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamApprovalInboxNotices", request.getFilter().getQuery());
        return StreamApprovalInboxNoticesResponse.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply3.getCloseChecklist().getBillingAddress().getRecipient())
                                .setRegion(reply0.getFxHedge().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
