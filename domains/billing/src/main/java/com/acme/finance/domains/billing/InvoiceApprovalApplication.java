package com.acme.finance.domains.billing;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.PreviewAccessPolicyRequest;
import com.acme.authorization.v1.PreviewAccessPolicyResponse;
import com.acme.billing.v1.ApproveInvoiceRequest;
import com.acme.billing.v1.ApproveInvoiceResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountLimit;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CheckBillingAccountEligibilityRequest;
import com.acme.billing.v1.CheckBillingAccountEligibilityResponse;
import com.acme.billing.v1.CommentInvoiceApprovalRequest;
import com.acme.billing.v1.CommentInvoiceApprovalResponse;
import com.acme.billing.v1.DelegateBillingAccountApprovalRequest;
import com.acme.billing.v1.DelegateBillingAccountApprovalResponse;
import com.acme.billing.v1.DelegateInvoiceApprovalRequest;
import com.acme.billing.v1.DelegateInvoiceApprovalResponse;
import com.acme.billing.v1.EscalateInvoiceRequest;
import com.acme.billing.v1.EscalateInvoiceResponse;
import com.acme.billing.v1.GetInvoiceApprovalStatusRequest;
import com.acme.billing.v1.GetInvoiceApprovalStatusResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceCardSettlement;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.RecalculateInvoicesRequest;
import com.acme.billing.v1.RecalculateInvoicesResponse;
import com.acme.billing.v1.RecallInvoiceRequest;
import com.acme.billing.v1.RecallInvoiceResponse;
import com.acme.billing.v1.RejectInvoiceRequest;
import com.acme.billing.v1.RejectInvoiceResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.SubmitInvoiceRequest;
import com.acme.billing.v1.SubmitInvoiceResponse;
import com.acme.billing.v1.SuggestInvoicesRequest;
import com.acme.billing.v1.SuggestInvoicesResponse;
import com.acme.budgeting.v1.EscalateVarianceRequest;
import com.acme.budgeting.v1.EscalateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ApproveEInvoiceRequest;
import com.acme.invoicing.v1.ApproveEInvoiceResponse;
import com.acme.invoicing.v1.EInvoiceApprovalServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryBankSettlement;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.ledger.v1.ValidateJournalEntryRequest;
import com.acme.ledger.v1.ValidateJournalEntryResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesComponent;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.ValidateMetricSeriesRequest;
import com.acme.metrics.v1.ValidateMetricSeriesResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.ScoreNotificationRequest;
import com.acme.notifications.v1.ScoreNotificationResponse;
import com.acme.scheduling.v1.CreateShiftRequest;
import com.acme.scheduling.v1.CreateShiftResponse;
import com.acme.scheduling.v1.Shift;
import com.acme.scheduling.v1.ShiftParticipant;
import com.acme.scheduling.v1.ShiftServiceGrpc;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingWorkflowServiceGrpc;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes invoices through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class InvoiceApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceApprovalApplication.class);

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking;

    private final BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking;

    private final InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking;

    private final InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking;

    private final VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final EInvoiceApprovalServiceGrpc.EInvoiceApprovalServiceBlockingStub invoicingEInvoiceApprovalServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final ShiftServiceGrpc.ShiftServiceBlockingStub schedulingShiftServiceBlocking;

    private final SupplierRatingWorkflowServiceGrpc.SupplierRatingWorkflowServiceBlockingStub suppliersSupplierRatingWorkflowServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public InvoiceApprovalApplication(
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking,
            BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking,
            InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking,
            InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking,
            VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking,
            ChannelRegistry channels,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            ApplicationEventPublisher events,
            EInvoiceApprovalServiceGrpc.EInvoiceApprovalServiceBlockingStub invoicingEInvoiceApprovalServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            ShiftServiceGrpc.ShiftServiceBlockingStub schedulingShiftServiceBlocking,
            SupplierRatingWorkflowServiceGrpc.SupplierRatingWorkflowServiceBlockingStub suppliersSupplierRatingWorkflowServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.billingBillingAccountApprovalServiceBlocking = billingBillingAccountApprovalServiceBlocking;
        this.billingBillingAccountValidationServiceBlocking = billingBillingAccountValidationServiceBlocking;
        this.billingInvoiceCalculationServiceBlocking = billingInvoiceCalculationServiceBlocking;
        this.billingInvoiceQueryServiceBlocking = billingInvoiceQueryServiceBlocking;
        this.budgetingVarianceApprovalServiceBlocking = budgetingVarianceApprovalServiceBlocking;
        this.channels = channels;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.events = events;
        this.invoicingEInvoiceApprovalServiceBlocking = invoicingEInvoiceApprovalServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.schedulingShiftServiceBlocking = schedulingShiftServiceBlocking;
        this.suppliersSupplierRatingWorkflowServiceBlocking = suppliersSupplierRatingWorkflowServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    public record ApproveInvoiceStage2Requested(DelegateBillingAccountApprovalRequest request, CompletableFuture<DelegateBillingAccountApprovalResponse> completion, io.grpc.Context context) {}

    public record EscalateInvoiceStage2Requested(FindUsersRequest request, CompletableFuture<FindUsersResponse> completion, io.grpc.Context context) {}

    /**
     * Submits an invoice.
     */
    public SubmitInvoiceResponse submitInvoice(SubmitInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = StartSupplierRatingWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getInvoice().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getInvoice().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setCard(SupplierRatingCardSettlement.newBuilder()
                                .setToken(request.getInvoice().getCard().getToken())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(suppliersSupplierRatingWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startSupplierRatingWorkflow(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CreateShiftRequest.newBuilder()
                .setContext(request.getContext())
                .setShift(Shift.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getInvoice().getCounterparty().getDisplayName())
                                .build())
                        .addParticipants(ShiftParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(schedulingShiftServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createShift(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitInvoice", request.getInvoice().getInvoiceId());
        return SubmitInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getShift().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getSupplierRating().getBillingAddress().getRegion())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .setTotal(request.getInvoice().getTotal())
                        .build())
                .setOutcome(SubmitInvoiceResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves an invoice.
     */
    public ApproveInvoiceResponse approveInvoice(ApproveInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var futures0 = request.getInvoice().getLinesList().stream()
                .map(element -> SummarizeVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizeVenuesRequest.Details.newBuilder()
                        .putAllFilters(element.getAttributesMap())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeVenues(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? SummarizeVenuesResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ScoreNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreNotificationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getInvoice().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getInvoice().getCounterparty().getDisplayName())
                                .setEmail(request.getInvoice().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.notifications.v1.NotificationValidationService");
        var serviceName1 = "acme.notifications.v1" + "." + "NotificationValidationService";
        var methodName1 = "Sco" + "reNotification";
        var descriptor1 = MethodDescriptor.<ScoreNotificationRequest, ScoreNotificationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ScoreNotificationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScoreNotificationResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = DelegateBillingAccountApprovalRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getNotification().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion2 = new CompletableFuture<DelegateBillingAccountApprovalResponse>();
        events.publishEvent(new ApproveInvoiceStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = ApproveEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApproveEInvoiceRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply2.getBillingAccount().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> invoicingEInvoiceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveEInvoice(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveInvoice", request.getInvoice().getInvoiceId());
        return ApproveInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getNotification().getAudit().getEtag())
                                .build())
                        .setBillingAddress(request.getInvoice().getBillingAddress())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .setOutcome(ApproveInvoiceResponse.Outcome.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Rejects an invoice.
     */
    public RejectInvoiceResponse rejectInvoice(RejectInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = BeanCopies.copy(LintCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getInvoice().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build(), LintCustomersRequest.class);
        var reply0 = customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomers(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectInvoice", request.getInvoice().getInvoiceId());
        return RejectInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getCustomer().getPrimaryAddress().getCountryCode())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .setStatus(request.getInvoice().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates an invoice.
     */
    public EscalateInvoiceResponse escalateInvoice(EscalateInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = ValidateJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setBank(JournalEntryBankSettlement.newBuilder()
                                .setIban(switch (request.getInvoice().getSettlementCase()) {
                                    case BANK -> request.getInvoice().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getInvoice().getBillingAddress().getLocality())
                                .setRegion(request.getInvoice().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateJournalEntry(outbound0);

        var futures1 = request.getInvoice().getLinesList().stream()
                .map(element -> ValidateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(request.getInvoice().getBillingAddress().getLocality())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getInvoice().getAudit().getUpdatedAt())
                                .build())
                        .putComponents("primary", MetricSeriesComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMetricSeries(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ValidateMetricSeriesResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = FindUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindUsersRequest.Filter.newBuilder()
                        .setRegion(request.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion2 = new CompletableFuture<FindUsersResponse>();
        events.publishEvent(new EscalateInvoiceStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = SuggestInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestInvoicesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getJournalEntry().getMetadataMap())
                        .setOwnerId(reply1.getMetricSeries().getComponentsOrDefault("primary", MetricSeriesComponent.getDefaultInstance()).getComponentId())
                        .setRegion(reply0.getJournalEntry().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> billingInvoiceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestInvoices(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateInvoice", request.getInvoice().getInvoiceId());
        return EscalateInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setInvoiceId(request.getInvoiceId())
                        .setStatus(request.getInvoice().getStatus())
                        .build())
                .setOutcome(EscalateInvoiceResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates an invoice.
     */
    public DelegateInvoiceApprovalResponse delegateInvoiceApproval(DelegateInvoiceApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateInvoiceApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var builder0 = EscalateVarianceRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setVariance(Variance.newBuilder()
                .setRequester(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = budgetingVarianceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateVariance(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateInvoiceApproval", request.getInvoice().getInvoiceId());
        return DelegateInvoiceApprovalResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setCounterparty(request.getInvoice().getCounterparty())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .setOutcome(DelegateInvoiceApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls an invoice.
     */
    public RecallInvoiceResponse recallInvoice(RecallInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = ScoreBillingAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreBillingAccountRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getInvoice().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        RpcPort<ScoreBillingAccountRequest, ScoreBillingAccountResponse> port0 = RpcPort.adapt(billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreBillingAccount);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = JsonCodec.roundTrip(SendPostingBatchTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendPostingBatchTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getInvoice().getCounterparty().getEmail())
                                .setName(reply0.getBillingAccount().getContact().getName())
                                .setPhone(reply0.getBillingAccount().getContact().getPhone())
                                .setPreferredChannel(reply0.getBillingAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), SendPostingBatchTestNoticeRequest.class);
        var reply1 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendPostingBatchTestNotice(outbound1);

        var outbound2 = DelegateBillingAccountApprovalRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setCard(BillingAccountCardFunding.newBuilder()
                                .setLast4(reply0.getBillingAccount().getCard().getLast4())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = billingBillingAccountApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateBillingAccountApproval(outbound2);

        var outbound3 = PreviewAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getInvoice().getAudit().getEtag())
                                .setUpdatedBy(reply1.getPostingBatch().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAccessPolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getBillingAccount().getContact().getEmail())
                                .build())
                        .build())
                .build();
        Function<PreviewAccessPolicyRequest, PreviewAccessPolicyResponse> operation3 = authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewAccessPolicy;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallInvoice", request.getInvoice().getInvoiceId());
        return RecallInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getPostingBatch().getBillingAddress().getPostalCode())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .setNotes(request.getInvoice().getNotes())
                        .setNumber(reply0.getBillingAccount().getHolder().getNumber())
                        .build())
                .setOutcome(RecallInvoiceResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns an invoice.
     */
    public GetInvoiceApprovalStatusResponse getInvoiceApprovalStatus(GetInvoiceApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetInvoiceApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getInvoiceId(), "GetInvoiceApprovalStatus");
        return GetInvoiceApprovalStatusResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .build();
    }

    /**
     * Comments an invoice.
     */
    public CommentInvoiceApprovalResponse commentInvoiceApproval(CommentInvoiceApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentInvoiceApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = RecalculateInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getInvoice().getBillingAddress().getLocality())
                                .build())
                        .setCard(InvoiceCardSettlement.newBuilder()
                                .setNetwork(switch (request.getInvoice().getSettlementCase()) {
                                    case CARD -> request.getInvoice().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateInvoices(outbound0);

        var results1 = new ArrayList<CheckBillingAccountEligibilityResponse>();
        for (var element : request.getInvoice().getLinesList()) {
            CheckBillingAccountEligibilityRequest itemRequest = CheckBillingAccountEligibilityRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setCard(BillingAccountCardFunding.newBuilder()
                                .setLast4(reply0.getInvoice().getCard().getLast4())
                                .setNetwork(request.getInvoice().getCard().getNetwork())
                                .build())
                        .addLimits(BillingAccountLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckBillingAccountEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getInvoice().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
            results1.add(billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkBillingAccountEligibility(itemRequest));
        }
        var reply1 = results1.isEmpty() ? CheckBillingAccountEligibilityResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentInvoiceApproval", request.getInvoice().getInvoiceId());
        return CommentInvoiceApprovalResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setInvoiceId(request.getInvoiceId())
                        .setIssuedAt(reply0.getInvoice().getIssuedAt())
                        .build())
                .setOutcome(CommentInvoiceApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }
}
