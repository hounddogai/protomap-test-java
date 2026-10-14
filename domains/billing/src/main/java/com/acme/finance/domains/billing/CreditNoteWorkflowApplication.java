package com.acme.finance.domains.billing;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.billing.v1.AdvanceCreditNoteWorkflowRequest;
import com.acme.billing.v1.AdvanceCreditNoteWorkflowResponse;
import com.acme.billing.v1.BatchGetBillingAccountsRequest;
import com.acme.billing.v1.BatchGetBillingAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountLimit;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.billing.v1.BillingAccountWorkflowServiceGrpc;
import com.acme.billing.v1.CancelCreditNoteWorkflowRequest;
import com.acme.billing.v1.CancelCreditNoteWorkflowResponse;
import com.acme.billing.v1.CommentCreditNoteApprovalRequest;
import com.acme.billing.v1.CommentCreditNoteApprovalResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteApprovalServiceGrpc;
import com.acme.billing.v1.CreditNoteAttachment;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.EscalateBillingAccountRequest;
import com.acme.billing.v1.EscalateBillingAccountResponse;
import com.acme.billing.v1.GetCreditNoteRatesRequest;
import com.acme.billing.v1.GetCreditNoteRatesResponse;
import com.acme.billing.v1.GetCreditNoteRulesRequest;
import com.acme.billing.v1.GetCreditNoteRulesResponse;
import com.acme.billing.v1.InspectCreditNoteWorkflowRequest;
import com.acme.billing.v1.InspectCreditNoteWorkflowResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceWorkflowServiceGrpc;
import com.acme.billing.v1.ListCreditNoteWorkflowsRequest;
import com.acme.billing.v1.ListCreditNoteWorkflowsResponse;
import com.acme.billing.v1.ListPendingPaymentTermsRequest;
import com.acme.billing.v1.ListPendingPaymentTermsResponse;
import com.acme.billing.v1.PauseCreditNoteWorkflowRequest;
import com.acme.billing.v1.PauseCreditNoteWorkflowResponse;
import com.acme.billing.v1.PauseInvoiceWorkflowRequest;
import com.acme.billing.v1.PauseInvoiceWorkflowResponse;
import com.acme.billing.v1.PaymentTermApprovalServiceGrpc;
import com.acme.billing.v1.RejectCreditNoteRequest;
import com.acme.billing.v1.RejectCreditNoteResponse;
import com.acme.billing.v1.ResumeCreditNoteWorkflowRequest;
import com.acme.billing.v1.ResumeCreditNoteWorkflowResponse;
import com.acme.billing.v1.ResumeInvoiceWorkflowRequest;
import com.acme.billing.v1.ResumeInvoiceWorkflowResponse;
import com.acme.billing.v1.RetryBillingAccountWorkflowRequest;
import com.acme.billing.v1.RetryBillingAccountWorkflowResponse;
import com.acme.billing.v1.RetryCreditNoteWorkflowRequest;
import com.acme.billing.v1.RetryCreditNoteWorkflowResponse;
import com.acme.billing.v1.SignalCreditNoteWorkflowRequest;
import com.acme.billing.v1.SignalCreditNoteWorkflowResponse;
import com.acme.billing.v1.StartCreditNoteWorkflowRequest;
import com.acme.billing.v1.StartCreditNoteWorkflowResponse;
import com.acme.billing.v1.SubmitBillingAccountRequest;
import com.acme.billing.v1.SubmitBillingAccountResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BatchCreateCustomersRequest;
import com.acme.customers.v1.BatchCreateCustomersResponse;
import com.acme.customers.v1.BatchGetCustomersRequest;
import com.acme.customers.v1.BatchGetCustomersResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CancelCollateralWorkflowRequest;
import com.acme.lending.v1.CancelCollateralWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentLifecycleServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.messaging.v1.CalculateConversationRequest;
import com.acme.messaging.v1.CalculateConversationResponse;
import com.acme.messaging.v1.Conversation;
import com.acme.messaging.v1.ConversationCalculationServiceGrpc;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCardSettlement;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.RetryLiquidityForecastImportRequest;
import com.acme.treasury.v1.RetryLiquidityForecastImportResponse;
import java.util.ArrayList;
import java.util.List;
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
 * Runs the long-lived workflow that takes a credit note from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class CreditNoteWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(CreditNoteWorkflowApplication.class);

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking;

    private final BillingAccountServiceGrpc.BillingAccountServiceBlockingStub billingBillingAccountServiceBlocking;

    private final BillingAccountWorkflowServiceGrpc.BillingAccountWorkflowServiceBlockingStub billingBillingAccountWorkflowServiceBlocking;

    private final CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceBlockingStub billingCreditNoteApprovalServiceBlocking;

    private final CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking;

    private final InvoiceWorkflowServiceGrpc.InvoiceWorkflowServiceBlockingStub billingInvoiceWorkflowServiceBlocking;

    private final PaymentTermApprovalServiceGrpc.PaymentTermApprovalServiceBlockingStub billingPaymentTermApprovalServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceStub customersCustomerServiceAsync;

    private final ApplicationEventPublisher events;

    private final CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking;

    private final RepaymentLifecycleServiceGrpc.RepaymentLifecycleServiceBlockingStub lendingRepaymentLifecycleServiceBlocking;

    private final CreditNoteWorkflowRequestMapper mapper;

    private final ConversationCalculationServiceGrpc.ConversationCalculationServiceBlockingStub messagingConversationCalculationServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking;

    public CreditNoteWorkflowApplication(
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking,
            BillingAccountServiceGrpc.BillingAccountServiceBlockingStub billingBillingAccountServiceBlocking,
            BillingAccountWorkflowServiceGrpc.BillingAccountWorkflowServiceBlockingStub billingBillingAccountWorkflowServiceBlocking,
            CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceBlockingStub billingCreditNoteApprovalServiceBlocking,
            CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking,
            InvoiceWorkflowServiceGrpc.InvoiceWorkflowServiceBlockingStub billingInvoiceWorkflowServiceBlocking,
            PaymentTermApprovalServiceGrpc.PaymentTermApprovalServiceBlockingStub billingPaymentTermApprovalServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerServiceGrpc.CustomerServiceStub customersCustomerServiceAsync,
            ApplicationEventPublisher events,
            CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking,
            RepaymentLifecycleServiceGrpc.RepaymentLifecycleServiceBlockingStub lendingRepaymentLifecycleServiceBlocking,
            CreditNoteWorkflowRequestMapper mapper,
            ConversationCalculationServiceGrpc.ConversationCalculationServiceBlockingStub messagingConversationCalculationServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking) {
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.billingBillingAccountApprovalServiceBlocking = billingBillingAccountApprovalServiceBlocking;
        this.billingBillingAccountServiceBlocking = billingBillingAccountServiceBlocking;
        this.billingBillingAccountWorkflowServiceBlocking = billingBillingAccountWorkflowServiceBlocking;
        this.billingCreditNoteApprovalServiceBlocking = billingCreditNoteApprovalServiceBlocking;
        this.billingCreditNoteValidationServiceBlocking = billingCreditNoteValidationServiceBlocking;
        this.billingInvoiceWorkflowServiceBlocking = billingInvoiceWorkflowServiceBlocking;
        this.billingPaymentTermApprovalServiceBlocking = billingPaymentTermApprovalServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerServiceAsync = customersCustomerServiceAsync;
        this.events = events;
        this.lendingCollateralWorkflowServiceBlocking = lendingCollateralWorkflowServiceBlocking;
        this.lendingRepaymentLifecycleServiceBlocking = lendingRepaymentLifecycleServiceBlocking;
        this.mapper = mapper;
        this.messagingConversationCalculationServiceBlocking = messagingConversationCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.treasuryLiquidityForecastImportServiceBlocking = treasuryLiquidityForecastImportServiceBlocking;
    }

    public record PauseCreditNoteWorkflowStage1Requested(GetCreditNoteRatesRequest request, CompletableFuture<GetCreditNoteRatesResponse> completion, io.grpc.Context context) {}

    public record InspectCreditNoteWorkflowStage0Requested(BatchGetCustomersRequest request, CompletableFuture<BatchGetCustomersResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a credit note.
     */
    public StartCreditNoteWorkflowResponse startCreditNoteWorkflow(StartCreditNoteWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartCreditNoteWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = PreviewAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getCreditNote().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getCreditNote().getBillingAddress().getLinesList())
                                .setLocality(request.getCreditNote().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAuditTrailRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAuditTrail(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartCreditNoteWorkflow", request.getCreditNote().getCreditNoteId());
        return StartCreditNoteWorkflowResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .setNotes(request.getCreditNote().getNotes())
                        .build())
                .setOutcome(StartCreditNoteWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances a credit note.
     */
    public AdvanceCreditNoteWorkflowResponse advanceCreditNoteWorkflow(AdvanceCreditNoteWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceCreditNoteWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        List<PauseInvoiceWorkflowResponse> results0 = request.getCreditNote().getLinesList().stream()
                .map(element -> PauseInvoiceWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .addLines(InvoiceLine.newBuilder()
                                .setTax(InvoiceLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(billingInvoiceWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseInvoiceWorkflow(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? PauseInvoiceWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(CalculateConversationRequest.newBuilder()
                .setContext(request.getContext())
                .setConversation(Conversation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getCreditNote().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build(), CalculateConversationRequest.class);
        var reply1 = messagingConversationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateConversation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceCreditNoteWorkflow", request.getCreditNote().getCreditNoteId());
        return AdvanceCreditNoteWorkflowResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(request.getCreditNote().getBillingAddress())
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .setOutcome(AdvanceCreditNoteWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a credit note.
     */
    public CancelCreditNoteWorkflowResponse cancelCreditNoteWorkflow(CancelCreditNoteWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelCreditNoteWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = ResolveCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCustomers(outbound0), rpcExecutor);

        var outbound1 = EscalateBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCreditNote().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> billingBillingAccountApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateBillingAccount(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        List<RetryBillingAccountWorkflowResponse> results2 = request.getCreditNote().getLinesList().stream()
                .map(element -> RetryBillingAccountWorkflowRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCreditNote().getAudit().getCreatedAt())
                                .build())
                        .setBalance(element.getTax().getAmount())
                        .setCard(BillingAccountCardFunding.newBuilder()
                                .setHolderName(reply1.getBillingAccount().getCard().getHolderName())
                                .build())
                        .addLimits(BillingAccountLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(billingBillingAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryBillingAccountWorkflow(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? RetryBillingAccountWorkflowResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = RetryLiquidityForecastImportRequest.newBuilder()
                .setContext(request.getContext())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setCard(LiquidityForecastCardSettlement.newBuilder()
                                .setNetwork(switch (reply2.getBillingAccount().getFundingCase()) {
                                    case CARD -> reply2.getBillingAccount().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryLiquidityForecastImport(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelCreditNoteWorkflow", request.getCreditNote().getCreditNoteId());
        return CancelCreditNoteWorkflowResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply3.getLiquidityForecast().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply3.getLiquidityForecast().getBillingAddress().getLinesList())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .setStatus(request.getCreditNote().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Retrys a credit note.
     */
    public RetryCreditNoteWorkflowResponse retryCreditNoteWorkflow(RetryCreditNoteWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryCreditNoteWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var results0 = new ArrayList<SubmitBillingAccountResponse>();
        for (var element : request.getCreditNote().getLinesList()) {
            SubmitBillingAccountRequest itemRequest = SubmitBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCreditNote().getAudit().getCreatedBy())
                                .build())
                        .setBalance(element.getTax().getAmount())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(request.getCreditNote().getCounterparty().getDisplayName())
                                .build())
                        .addLimits(BillingAccountLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(billingBillingAccountApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitBillingAccount(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SubmitBillingAccountResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = mapper.retryCreditNoteWorkflowStage1(request, reply0);
        var reply1 = billingPaymentTermApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingPaymentTerms(outbound1);

        var outbound2 = RejectCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCreditNote().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.billing.v1.CreditNoteApprovalService/RejectCreditNote", outbound2);
        var reply2 = billingCreditNoteApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectCreditNote(command2.payload());

        List<CancelCollateralWorkflowResponse> results3 = request.getCreditNote().getLinesList().stream()
                .map(element -> CancelCollateralWorkflowRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getBillingAccount().getAudit().getCreatedBy())
                                .build())
                        .putComponents("primary", CollateralComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(lendingCollateralWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelCollateralWorkflow(value)))
                .collect(Collectors.toList());
        var reply3 = results3.isEmpty() ? CancelCollateralWorkflowResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryCreditNoteWorkflow", request.getCreditNote().getCreditNoteId());
        return RetryCreditNoteWorkflowResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getCreditNote().getAudit().getEtag())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .putAllMetadata(reply2.getCreditNote().getMetadataMap())
                        .setNotes(request.getCreditNote().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Pauses a credit note.
     */
    public PauseCreditNoteWorkflowResponse pauseCreditNoteWorkflow(PauseCreditNoteWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseCreditNoteWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = (GetCreditNoteRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNoteId(request.getCreditNote().getCreditNoteId())
                .build()).toBuilder().build();
        var reply0 = billingCreditNoteValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getCreditNoteRules(outbound0);

        var outbound1 = GetCreditNoteRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNoteId(request.getCreditNote().getCreditNoteId())
                .build();
        var completion1 = new CompletableFuture<GetCreditNoteRatesResponse>();
        events.publishEvent(new PauseCreditNoteWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        List<CommentCreditNoteApprovalResponse> results2 = request.getCreditNote().getAttachmentsList().stream()
                .map(element -> CommentCreditNoteApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .addAttachments(CreditNoteAttachment.newBuilder()
                                .setFileName(element.getFileName())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(billingCreditNoteApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentCreditNoteApproval(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? CommentCreditNoteApprovalResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseCreditNoteWorkflow", request.getCreditNote().getNumber());
        return PauseCreditNoteWorkflowResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .setStatus(request.getCreditNote().getStatus())
                        .setTotal(reply0.getCreditNote().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Resumes a credit note.
     */
    public ResumeCreditNoteWorkflowResponse resumeCreditNoteWorkflow(ResumeCreditNoteWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeCreditNoteWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = ScheduleRepaymentExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleRepaymentExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getCreditNote().getBillingAddress().getRegion())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingRepaymentLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleRepaymentExpiry(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CommentCreditNoteApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getCreditNote().getBillingAddress().getPostalCode())
                                .build())
                        .setCard(CreditNoteCardSettlement.newBuilder()
                                .setNetwork(switch (request.getCreditNote().getSettlementCase()) {
                                    case CARD -> request.getCreditNote().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getRepayment().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setDetails(CommentCreditNoteApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getCreditNote().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = billingCreditNoteApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentCreditNoteApproval(outbound1);

        var results2 = new ArrayList<RejectCreditNoteResponse>();
        for (var element : request.getCreditNote().getAttachmentsList()) {
            RejectCreditNoteRequest itemRequest = RejectCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .addAttachments(CreditNoteAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build();
            results2.add(billingCreditNoteApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectCreditNote(itemRequest));
        }
        var reply2 = results2.isEmpty() ? RejectCreditNoteResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeCreditNoteWorkflow", request.getCreditNote().getCreditNoteId());
        return ResumeCreditNoteWorkflowResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRepayment().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getRepayment().getLocation().getLocality())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .setIssuedAt(request.getCreditNote().getIssuedAt())
                        .setNotes(reply1.getCreditNote().getNotes())
                        .build())
                .build();
    }

    /**
     * Returns a credit note.
     */
    public InspectCreditNoteWorkflowResponse inspectCreditNoteWorkflow(InspectCreditNoteWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectCreditNoteWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "InspectCreditNoteWorkflow");

        var outbound0 = BatchGetCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<BatchGetCustomersResponse>();
        events.publishEvent(new InspectCreditNoteWorkflowStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return InspectCreditNoteWorkflowResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists credit notes a page at a time.
     */
    public ListCreditNoteWorkflowsResponse listCreditNoteWorkflows(ListCreditNoteWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListCreditNoteWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getContext().getTraceId(), "ListCreditNoteWorkflows");

        var outbound0 = BeanCopies.copy(RecalculateCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setDetails(RecalculateCustomerSegmentsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), RecalculateCustomerSegmentsRequest.class);
        var reply0 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomerSegments(outbound0);

        var outbound1 = ResumeInvoiceWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCustomerSegment().getAudit().getCreatedAt())
                                .setEtag(reply0.getCustomerSegment().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getCustomerSegment().getOwner().getEmail())
                                .build())
                        .addLines(InvoiceLine.newBuilder()
                                .setAmount(reply0.getCustomerSegment().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var reply1 = billingInvoiceWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeInvoiceWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListCreditNoteWorkflows", request.getPage().getPageToken());
        return ListCreditNoteWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Signals a credit note.
     */
    public SignalCreditNoteWorkflowResponse signalCreditNoteWorkflow(SignalCreditNoteWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalCreditNoteWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = BatchCreateCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setRecord(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCreditNote().getAudit().getEtag())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setLocality(request.getCreditNote().getBillingAddress().getLocality())
                                .setPostalCode(request.getCreditNote().getBillingAddress().getPostalCode())
                                .setRecipient(request.getCreditNote().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(BatchCreateCustomersResponse.getDefaultInstance());
        var input0 = customersCustomerServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCustomers(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();

        var outbound1 = (BatchGetBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply1 = billingBillingAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetBillingAccounts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalCreditNoteWorkflow", request.getCreditNote().getCreditNoteId());
        return SignalCreditNoteWorkflowResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .setIssuedAt(request.getCreditNote().getIssuedAt())
                        .build())
                .setOutcome(SignalCreditNoteWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
