package com.acme.finance.domains.collections;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RetryAuditEventExportRequest;
import com.acme.audit.v1.RetryAuditEventExportResponse;
import com.acme.audit.v1.SendAuditEventTestNoticeRequest;
import com.acme.audit.v1.SendAuditEventTestNoticeResponse;
import com.acme.audit.v1.SendAuditTrailTestNoticeRequest;
import com.acme.audit.v1.SendAuditTrailTestNoticeResponse;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.ListBankAccountWorkflowsRequest;
import com.acme.banking.v1.ListBankAccountWorkflowsResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.collections.v1.ApprovePaymentPromiseRequest;
import com.acme.collections.v1.ApprovePaymentPromiseResponse;
import com.acme.collections.v1.ApproveWriteOffRequest;
import com.acme.collections.v1.ApproveWriteOffResponse;
import com.acme.collections.v1.BrowseCollectionCasesRequest;
import com.acme.collections.v1.BrowseCollectionCasesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseComment;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.CommentWriteOffApprovalRequest;
import com.acme.collections.v1.CommentWriteOffApprovalResponse;
import com.acme.collections.v1.EscalateCollectionCaseRequest;
import com.acme.collections.v1.EscalateCollectionCaseResponse;
import com.acme.collections.v1.EscalateWriteOffRequest;
import com.acme.collections.v1.EscalateWriteOffResponse;
import com.acme.collections.v1.GetCollectionCaseApprovalStatusRequest;
import com.acme.collections.v1.GetCollectionCaseApprovalStatusResponse;
import com.acme.collections.v1.ListPendingCollectionCasesRequest;
import com.acme.collections.v1.ListPendingCollectionCasesResponse;
import com.acme.collections.v1.ListPendingWriteOffsRequest;
import com.acme.collections.v1.ListPendingWriteOffsResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.RecallWriteOffRequest;
import com.acme.collections.v1.RecallWriteOffResponse;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.collections.v1.RejectWriteOffRequest;
import com.acme.collections.v1.RejectWriteOffResponse;
import com.acme.collections.v1.StartCollectionCaseWorkflowRequest;
import com.acme.collections.v1.StartCollectionCaseWorkflowResponse;
import com.acme.collections.v1.SubmitCollectionCaseRequest;
import com.acme.collections.v1.SubmitCollectionCaseResponse;
import com.acme.collections.v1.SubmitWriteOffRequest;
import com.acme.collections.v1.SubmitWriteOffResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.RecallClaimRequest;
import com.acme.insurance.v1.RecallClaimResponse;
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
 * Routes write offs through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class WriteOffApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(WriteOffApprovalApplication.class);

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking;

    private final CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceFutureStub collectionsCollectionCaseApprovalServiceFuture;

    private final CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking;

    private final CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking;

    private final WriteOffApprovalRequestMapper mapper;

    private final CollectionsRepository repository;

    public WriteOffApprovalApplication(
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking,
            CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceFutureStub collectionsCollectionCaseApprovalServiceFuture,
            CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking,
            CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            ApplicationEventPublisher events,
            ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking,
            WriteOffApprovalRequestMapper mapper,
            CollectionsRepository repository) {
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.bankingBankAccountWorkflowServiceBlocking = bankingBankAccountWorkflowServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseApprovalServiceBlocking = collectionsCollectionCaseApprovalServiceBlocking;
        this.collectionsCollectionCaseApprovalServiceFuture = collectionsCollectionCaseApprovalServiceFuture;
        this.collectionsCollectionCaseQueryServiceBlocking = collectionsCollectionCaseQueryServiceBlocking;
        this.collectionsCollectionCaseWorkflowServiceBlocking = collectionsCollectionCaseWorkflowServiceBlocking;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.events = events;
        this.insuranceClaimApprovalServiceBlocking = insuranceClaimApprovalServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    public record ListPendingWriteOffsStage1Requested(EscalateStandingOrderRequest request, CompletableFuture<EscalateStandingOrderResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a write off.
     */
    public SubmitWriteOffResponse submitWriteOff(SubmitWriteOffRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitWriteOff")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = RecallClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getWriteOff().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insuranceClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallClaim(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = StartCollectionCaseWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(StartCollectionCaseWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        Function<StartCollectionCaseWorkflowRequest, StartCollectionCaseWorkflowResponse> operation1 = collectionsCollectionCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::startCollectionCaseWorkflow;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitWriteOff", request.getWriteOff().getWriteOffId());
        return SubmitWriteOffResponse.newBuilder()
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCollectionCase().getAudit().getEtag())
                                .setUpdatedBy(reply0.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(request.getWriteOff().getCounterparty())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Approves a write off.
     */
    public ApproveWriteOffResponse approveWriteOff(ApproveWriteOffRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveWriteOff")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = EscalateCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setCustomer(PaymentPromiseRef.newBuilder()
                                .setDisplayName(request.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .setPriority(request.getWriteOff().getCounterparty().getPriority())
                        .build())
                .setContext(request.getContext())
                .setDetails(EscalateCollectionCaseRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            EscalateCollectionCaseResponse fetch(EscalateCollectionCaseRequest input) {
                return collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateCollectionCase(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveWriteOff", request.getWriteOff().getWriteOffId());
        return ApproveWriteOffResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setNotes(request.getWriteOff().getNotes())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Rejects a write off.
     */
    public RejectWriteOffResponse rejectWriteOff(RejectWriteOffRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectWriteOff")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = BeanCopies.copy(RejectCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getWriteOff().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), RejectCollectionCaseRequest.class);
        var reply0 = collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectCollectionCase(outbound0);

        var outbound1 = RetryAuditEventExportRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(switch (reply0.getCollectionCase().getOriginCase()) {
                                    case CUSTOMER -> reply0.getCollectionCase().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RetryAuditEventExportRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setName(reply0.getCollectionCase().getRequester().getName())
                                .setPreferredChannel(reply0.getCollectionCase().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryAuditEventExport(outbound1);

        var outbound2 = mapper.rejectWriteOffStage2(request);
        var reply2 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendAuditEventTestNotice(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectWriteOff", request.getWriteOff().getWriteOffId());
        return RejectWriteOffResponse.newBuilder()
                .setOutcome(RejectWriteOffResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCollectionCase().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setDisplayName(reply0.getCollectionCase().getCustomer().getDisplayName())
                                .build())
                        .setIssuedAt(request.getWriteOff().getIssuedAt())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Escalates a write off.
     */
    public EscalateWriteOffResponse escalateWriteOff(EscalateWriteOffRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateWriteOff")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = SubmitCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getWriteOff().getAudit().getCreatedBy())
                                .build())
                        .addComments(CollectionCaseComment.newBuilder()
                                .setCreatedAt(request.getWriteOff().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(PaymentPromiseRef.newBuilder()
                                .setDisplayName(request.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .setPriority(request.getWriteOff().getCounterparty().getPriority())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = collectionsCollectionCaseApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).submitCollectionCase(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = StreamAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getOutcome().getReferenceId())
                        .setRegion(request.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventExport(outbound1);
        var reply1 = StreamAuditEventExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateWriteOff", request.getWriteOff().getWriteOffId());
        return EscalateWriteOffResponse.newBuilder()
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCollectionCase().getAudit().getCreatedBy())
                                .build())
                        .setStatus(request.getWriteOff().getStatus())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Lists write offs a page at a time.
     */
    public ListPendingWriteOffsResponse listPendingWriteOffs(ListPendingWriteOffsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingWriteOffs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "WriteOff", request.getContext().getTraceId(), "ListPendingWriteOffs");

        var outbound0 = BrowseCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCollectionCasesRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> collectionsCollectionCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseCollectionCases(outbound0));

        var outbound1 = EscalateStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<EscalateStandingOrderResponse>();
        events.publishEvent(new ListPendingWriteOffsStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingWriteOffs", request.getPage().getPageToken());
        return ListPendingWriteOffsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .addWriteOffs(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getStandingOrder().getLocation().getPostalCode())
                                .setRegion(reply1.getStandingOrder().getLocation().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Recalls a write off.
     */
    public RecallWriteOffResponse recallWriteOff(RecallWriteOffRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallWriteOff")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = ValidateWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getWriteOff().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getWriteOff().getBillingAddress().getPostalCode())
                                .build())
                        .setCard(WriteOffCardSettlement.newBuilder()
                                .setToken(switch (request.getWriteOff().getSettlementCase()) {
                                    case CARD -> request.getWriteOff().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateWriteOff(outbound0);

        var outbound1 = BeanCopies.copy(ListPendingCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingCollectionCasesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getWriteOff().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getWriteOff().getMetadataMap())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .setRegion(request.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build(), ListPendingCollectionCasesRequest.class);
        var reply1 = collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingCollectionCases(outbound1);

        var outbound2 = SendAuditTrailTestNoticeRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SendAuditTrailTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel2 = channels.lookup("acme.audit.v1.AuditTrailNotificationService");
        var serviceName2 = "acme.audit.v1" + "." + "AuditTrailNotificationService";
        var methodName2 = "Sen" + "dAuditTrailTestNotice";
        var descriptor2 = MethodDescriptor.<SendAuditTrailTestNoticeRequest, SendAuditTrailTestNoticeResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(SendAuditTrailTestNoticeRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SendAuditTrailTestNoticeResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = ApprovePaymentPromiseRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getWriteOff().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        Function<ApprovePaymentPromiseRequest, ApprovePaymentPromiseResponse> operation3 = collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::approvePaymentPromise;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallWriteOff", request.getWriteOff().getWriteOffId());
        return RecallWriteOffResponse.newBuilder()
                .setOutcome(RecallWriteOffResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply3.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getWriteOff().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply2.getAuditTrail().getBillingAddress().getRecipient())
                                .setRegion(reply3.getPaymentPromise().getLocation().getRegion())
                                .build())
                        .setIssuedAt(request.getWriteOff().getIssuedAt())
                        .setTotal(reply0.getWriteOff().getTotal())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Comments a write off.
     */
    public CommentWriteOffApprovalResponse commentWriteOffApproval(CommentWriteOffApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentWriteOffApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = EscalateCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setCustomer(PaymentPromiseRef.newBuilder()
                                .setDisplayName(request.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.collections.v1.CollectionCaseApprovalService");
        var serviceName0 = "acme.collections.v1" + "." + "CollectionCaseApprovalService";
        var methodName0 = "Esc" + "alateCollectionCase";
        var descriptor0 = MethodDescriptor.<EscalateCollectionCaseRequest, EscalateCollectionCaseResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(EscalateCollectionCaseRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EscalateCollectionCaseResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = mapper.commentWriteOffApprovalStage1(request);
        var reply1 = collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getCollectionCaseApprovalStatus(outbound1);

        var outbound2 = ListBankAccountWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBankAccountWorkflowsRequest.Filter.newBuilder()
                        .setRegion(request.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<ListBankAccountWorkflowsRequest, ListBankAccountWorkflowsResponse> port2 = RpcPort.adapt(bankingBankAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listBankAccountWorkflows);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentWriteOffApproval", request.getWriteOff().getWriteOffId());
        return CommentWriteOffApprovalResponse.newBuilder()
                .setOutcome(CommentWriteOffApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setNotes(request.getWriteOff().getNotes())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }
}
