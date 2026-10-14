package com.acme.finance.domains.billing;

import com.acme.audit.v1.BatchGetAuditEventsRequest;
import com.acme.audit.v1.BatchGetAuditEventsResponse;
import com.acme.billing.v1.ActivateDunningRequest;
import com.acme.billing.v1.ActivateDunningResponse;
import com.acme.billing.v1.BillingAccountRef;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.billing.v1.BrowseInvoicesRequest;
import com.acme.billing.v1.BrowseInvoicesResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningComment;
import com.acme.billing.v1.EstimateCreditNoteRequest;
import com.acme.billing.v1.EstimateCreditNoteResponse;
import com.acme.billing.v1.EstimateDunningRequest;
import com.acme.billing.v1.EstimateDunningResponse;
import com.acme.billing.v1.ExpireDunningRequest;
import com.acme.billing.v1.ExpireDunningResponse;
import com.acme.billing.v1.FetchDunningReportRequest;
import com.acme.billing.v1.FetchDunningReportResponse;
import com.acme.billing.v1.GetDunningLifecycleRequest;
import com.acme.billing.v1.GetDunningLifecycleResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.ListBillingAccountsRequest;
import com.acme.billing.v1.ListBillingAccountsResponse;
import com.acme.billing.v1.PreviewInvoiceRequest;
import com.acme.billing.v1.PreviewInvoiceResponse;
import com.acme.billing.v1.ReinstateDunningRequest;
import com.acme.billing.v1.ReinstateDunningResponse;
import com.acme.billing.v1.RenewDunningRequest;
import com.acme.billing.v1.RenewDunningResponse;
import com.acme.billing.v1.ScheduleDunningExpiryRequest;
import com.acme.billing.v1.ScheduleDunningExpiryResponse;
import com.acme.billing.v1.SuspendDunningRequest;
import com.acme.billing.v1.SuspendDunningResponse;
import com.acme.billing.v1.TerminateDunningRequest;
import com.acme.billing.v1.TerminateDunningResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.customers.v1.CheckPreferenceEligibilityRequest;
import com.acme.customers.v1.CheckPreferenceEligibilityResponse;
import com.acme.customers.v1.CountCustomersRequest;
import com.acme.customers.v1.CountCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.QuoteCustomerSegmentRequest;
import com.acme.customers.v1.QuoteCustomerSegmentResponse;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentLifecycleServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.risk.v1.ResumeExposureWorkflowRequest;
import com.acme.risk.v1.ResumeExposureWorkflowResponse;
import com.acme.search.v1.NormalizeSearchIndexRequest;
import com.acme.search.v1.NormalizeSearchIndexResponse;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexComponent;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Moves a dunning between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class DunningLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(DunningLifecycleApplication.class);

    private final BillingAccountServiceGrpc.BillingAccountServiceBlockingStub billingBillingAccountServiceBlocking;

    private final CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking;

    private final InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking;

    private final InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceFutureStub customersCustomerQueryServiceFuture;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceFutureStub customersCustomerSegmentCalculationServiceFuture;

    private final ApplicationEventPublisher events;

    private final RepaymentLifecycleServiceGrpc.RepaymentLifecycleServiceBlockingStub lendingRepaymentLifecycleServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceFutureStub searchSearchIndexValidationServiceFuture;

    public DunningLifecycleApplication(
            BillingAccountServiceGrpc.BillingAccountServiceBlockingStub billingBillingAccountServiceBlocking,
            CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking,
            InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking,
            InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking,
            ChannelRegistry channels,
            CustomerQueryServiceGrpc.CustomerQueryServiceFutureStub customersCustomerQueryServiceFuture,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceFutureStub customersCustomerSegmentCalculationServiceFuture,
            ApplicationEventPublisher events,
            RepaymentLifecycleServiceGrpc.RepaymentLifecycleServiceBlockingStub lendingRepaymentLifecycleServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceFutureStub searchSearchIndexValidationServiceFuture) {
        this.billingBillingAccountServiceBlocking = billingBillingAccountServiceBlocking;
        this.billingCreditNoteCalculationServiceBlocking = billingCreditNoteCalculationServiceBlocking;
        this.billingInvoiceQueryServiceBlocking = billingInvoiceQueryServiceBlocking;
        this.billingInvoiceValidationServiceBlocking = billingInvoiceValidationServiceBlocking;
        this.channels = channels;
        this.customersCustomerQueryServiceFuture = customersCustomerQueryServiceFuture;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceFuture = customersCustomerSegmentCalculationServiceFuture;
        this.events = events;
        this.lendingRepaymentLifecycleServiceBlocking = lendingRepaymentLifecycleServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.searchSearchIndexValidationServiceFuture = searchSearchIndexValidationServiceFuture;
    }

    public record ActivateDunningStage0Requested(BatchGetAuditEventsRequest request, CompletableFuture<BatchGetAuditEventsResponse> completion, io.grpc.Context context) {}

    public record TerminateDunningStage0Requested(FetchDunningReportRequest request, CompletableFuture<FetchDunningReportResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a dunning.
     */
    public ActivateDunningResponse activateDunning(ActivateDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "ActivateDunning");

        var outbound0 = BatchGetAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<BatchGetAuditEventsResponse>();
        events.publishEvent(new ActivateDunningStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = RecalculateCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = customersCustomerSegmentCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomerSegments(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = ScheduleRepaymentExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingRepaymentLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleRepaymentExpiry(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateDunning", request.getDetails().getReasonCode());
        return ActivateDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .addComments(DunningComment.newBuilder()
                                .setCreatedAt(reply2.getRepayment().getAudit().getCreatedAt())
                                .build())
                        .setDunningId(request.getDunningId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getCustomerSegment().getOwner().getEmail())
                                .build())
                        .build())
                .setOutcome(ActivateDunningResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Suspends a dunning.
     */
    public SuspendDunningResponse suspendDunning(SuspendDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "SuspendDunning");

        var outbound0 = ResumeExposureWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResumeExposureWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.ExposureWorkflowService");
        var serviceName0 = "acme.risk.v1" + "." + "ExposureWorkflowService";
        var methodName0 = "Res" + "umeExposureWorkflow";
        var descriptor0 = MethodDescriptor.<ResumeExposureWorkflowRequest, ResumeExposureWorkflowResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ResumeExposureWorkflowRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ResumeExposureWorkflowResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = CreateNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        RpcPort<CreateNotificationRequest, CreateNotificationResponse> port1 = RpcPort.adapt(notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createNotification);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendDunning", request.getDetails().getReasonCode());
        return SuspendDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setDunningId(request.getDunningId())
                        .build())
                .setOutcome(SuspendDunningResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a dunning.
     */
    public ReinstateDunningResponse reinstateDunning(ReinstateDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "ReinstateDunning");

        var outbound0 = PreviewSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.search.v1.SearchIndexValidationService");
        var serviceName0 = "acme.search.v1" + "." + "SearchIndexValidationService";
        var methodName0 = "Pre" + "viewSearchIndex";
        var descriptor0 = MethodDescriptor.<PreviewSearchIndexRequest, PreviewSearchIndexResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(PreviewSearchIndexRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PreviewSearchIndexResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = NormalizeSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .putComponents("primary", SearchIndexComponent.newBuilder()
                                .setQuantity(reply0.getSearchIndex().getComponentsOrDefault("primary", SearchIndexComponent.getDefaultInstance()).getQuantity())
                                .build())
                        .build())
                .build();
        var pending1 = searchSearchIndexValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeSearchIndex(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateDunning", request.getDetails().getReasonCode());
        return ReinstateDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getSearchIndex().getAudit().getUpdatedBy())
                                .build())
                        .setDunningId(request.getDunningId())
                        .build())
                .setOutcome(ReinstateDunningResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates a dunning.
     */
    public TerminateDunningResponse terminateDunning(TerminateDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "TerminateDunning");

        var outbound0 = FetchDunningReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDunningId(request.getDunningId())
                .build();
        var completion0 = new CompletableFuture<FetchDunningReportResponse>();
        events.publishEvent(new TerminateDunningStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = EstimateCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getDunning().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getDunning().getAssignee().getDisplayName())
                                .build())
                        .setDueAt(reply0.getDunning().getSla().getDueAt())
                        .build())
                .build();
        final class Stage1Adapter {
            EstimateCreditNoteResponse fetch(EstimateCreditNoteRequest input) {
                return billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCreditNote(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateDunning", request.getDetails().getReasonCode());
        return TerminateDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .addComments(DunningComment.newBuilder()
                                .setCreatedAt(reply0.getDunning().getAudit().getCreatedAt())
                                .build())
                        .setDunningId(request.getDunningId())
                        .build())
                .setOutcome(TerminateDunningResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Renews a dunning.
     */
    public RenewDunningResponse renewDunning(RenewDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "RenewDunning");

        var outbound0 = CountCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = customersCustomerQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).countCustomers(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = EstimateDunningRequest.newBuilder()
                .setContext(request.getContext())
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setCustomer(BillingAccountRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.billing.v1.DunningCalculationService");
        var serviceName1 = "acme.billing.v1" + "." + "DunningCalculationService";
        var methodName1 = "Est" + "imateDunning";
        var descriptor1 = MethodDescriptor.<EstimateDunningRequest, EstimateDunningResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(EstimateDunningRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EstimateDunningResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewDunning", request.getDetails().getReasonCode());
        return RenewDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getDunning().getAssignee().getDisplayName())
                                .build())
                        .setDunningId(request.getDunningId())
                        .build())
                .setOutcome(RenewDunningResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Expires a dunning.
     */
    public ExpireDunningResponse expireDunning(ExpireDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "ExpireDunning");

        var outbound0 = ListBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBillingAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> billingBillingAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listBillingAccounts(outbound0), rpcExecutor);

        var outbound1 = QuoteCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setBlock(CustomerSegmentBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCustomerSegment(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireDunning", request.getDetails().getReasonCode());
        return ExpireDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setDunningId(request.getDunningId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getCustomerSegment().getOwner().getEmail())
                                .build())
                        .build())
                .setOutcome(ExpireDunningResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a dunning.
     */
    public GetDunningLifecycleResponse getDunningLifecycle(GetDunningLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDunningLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "GetDunningLifecycle");

        var outbound0 = CheckPreferenceEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckPreferenceEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.PreferenceValidationService");
        var selected0 = PreferenceValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.checkPreferenceEligibility(outbound0);
        return GetDunningLifecycleResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPreference().getAudit().getCreatedAt())
                                .build())
                        .setDunningId(request.getDunningId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Schedules a dunning.
     */
    public ScheduleDunningExpiryResponse scheduleDunningExpiry(ScheduleDunningExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleDunningExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = (BrowseInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseInvoicesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDunning().getAudit().getCreatedAt())
                        .putAllLabels(request.getDunning().getCustomFieldsMap())
                        .setOwnerId(request.getDunning().getCustomer().getBillingAccountId())
                        .build())
                .build()).toBuilder().build();
        var reply0 = billingInvoiceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseInvoices(outbound0);

        var outbound1 = NormalizeSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeSearchIndexRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDunning().getAssignee().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getDunning().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDunning().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.search.v1.SearchIndexValidationService/NormalizeSearchIndex", outbound1);
        var reply1 = searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeSearchIndex(command1.payload());

        var outbound2 = PreviewInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getDunning().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(switch (request.getDunning().getOriginCase()) {
                                    case CUSTOMER -> request.getDunning().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewInvoice(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleDunningExpiry", request.getDunning().getDunningId());
        return ScheduleDunningExpiryResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAssignee(request.getDunning().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getSearchIndex().getAudit().getEtag())
                                .build())
                        .setDunningId(request.getDunningId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }
}
