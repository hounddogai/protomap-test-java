package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.ListAuditEventExportsRequest;
import com.acme.audit.v1.ListAuditEventExportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseWorkflowServiceGrpc;
import com.acme.contracts.v1.PauseContractClauseWorkflowRequest;
import com.acme.contracts.v1.PauseContractClauseWorkflowResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.MileageClaimWorkflowServiceGrpc;
import com.acme.expenses.v1.PauseReceiptWorkflowRequest;
import com.acme.expenses.v1.PauseReceiptWorkflowResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.SignalMileageClaimWorkflowRequest;
import com.acme.expenses.v1.SignalMileageClaimWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ActivateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ActivateInvoiceTemplateResponse;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.ExpireInvoiceTemplateRequest;
import com.acme.invoicing.v1.ExpireInvoiceTemplateResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateLifecycleRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateLifecycleResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateRequest;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateResponse;
import com.acme.invoicing.v1.ReinstateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ReinstateInvoiceTemplateResponse;
import com.acme.invoicing.v1.RenewInvoiceTemplateRequest;
import com.acme.invoicing.v1.RenewInvoiceTemplateResponse;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesResponse;
import com.acme.invoicing.v1.ScheduleInvoiceTemplateExpiryRequest;
import com.acme.invoicing.v1.ScheduleInvoiceTemplateExpiryResponse;
import com.acme.invoicing.v1.SuspendInvoiceTemplateRequest;
import com.acme.invoicing.v1.SuspendInvoiceTemplateResponse;
import com.acme.invoicing.v1.TerminateInvoiceTemplateRequest;
import com.acme.invoicing.v1.TerminateInvoiceTemplateResponse;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.PreviewLedgerAccountRequest;
import com.acme.ledger.v1.PreviewLedgerAccountResponse;
import com.acme.ledger.v1.ValidateLedgerAccountRequest;
import com.acme.ledger.v1.ValidateLedgerAccountResponse;
import com.acme.ticketing.v1.SuggestTicketsRequest;
import com.acme.ticketing.v1.SuggestTicketsResponse;
import com.acme.ticketing.v1.TicketQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Moves an invoice template between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class InvoiceTemplateLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceTemplateLifecycleApplication.class);

    private final ChannelRegistry channels;

    private final ContractClauseWorkflowServiceGrpc.ContractClauseWorkflowServiceBlockingStub contractsContractClauseWorkflowServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final MileageClaimWorkflowServiceGrpc.MileageClaimWorkflowServiceBlockingStub expensesMileageClaimWorkflowServiceBlocking;

    private final ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking;

    private final InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking;

    private final InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final InvoiceTemplateLifecycleRequestMapper mapper;

    private final InvoicingRepository repository;

    private final Executor rpcExecutor;

    private final TicketQueryServiceGrpc.TicketQueryServiceFutureStub ticketingTicketQueryServiceFuture;

    public InvoiceTemplateLifecycleApplication(
            ChannelRegistry channels,
            ContractClauseWorkflowServiceGrpc.ContractClauseWorkflowServiceBlockingStub contractsContractClauseWorkflowServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            ApplicationEventPublisher events,
            MileageClaimWorkflowServiceGrpc.MileageClaimWorkflowServiceBlockingStub expensesMileageClaimWorkflowServiceBlocking,
            ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking,
            InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking,
            InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            InvoiceTemplateLifecycleRequestMapper mapper,
            InvoicingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            TicketQueryServiceGrpc.TicketQueryServiceFutureStub ticketingTicketQueryServiceFuture) {
        this.channels = channels;
        this.contractsContractClauseWorkflowServiceBlocking = contractsContractClauseWorkflowServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.events = events;
        this.expensesMileageClaimWorkflowServiceBlocking = expensesMileageClaimWorkflowServiceBlocking;
        this.expensesReceiptWorkflowServiceBlocking = expensesReceiptWorkflowServiceBlocking;
        this.invoicingInvoiceBatchCalculationServiceBlocking = invoicingInvoiceBatchCalculationServiceBlocking;
        this.invoicingInvoiceTemplateValidationServiceBlocking = invoicingInvoiceTemplateValidationServiceBlocking;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.ticketingTicketQueryServiceFuture = ticketingTicketQueryServiceFuture;
    }

    public record ExpireInvoiceTemplateStage0Requested(ResolveInvoiceTemplatesRequest request, CompletableFuture<ResolveInvoiceTemplatesResponse> completion, io.grpc.Context context) {}

    /**
     * Activates an invoice template.
     */
    public ActivateInvoiceTemplateResponse activateInvoiceTemplate(ActivateInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "ActivateInvoiceTemplate");

        var outbound0 = NormalizeConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeConsentRecordRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeConsentRecord(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateInvoiceTemplate", request.getDetails().getReasonCode());
        return ActivateInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setOutcome(ActivateInvoiceTemplateResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends an invoice template.
     */
    public SuspendInvoiceTemplateResponse suspendInvoiceTemplate(SuspendInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "SuspendInvoiceTemplate");

        var outbound0 = PauseReceiptWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PauseReceiptWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> expensesReceiptWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseReceiptWorkflow(value)).orElseThrow();

        var outbound1 = ValidateLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getReceipt().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getReceipt().getAudit().getUpdatedBy())
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setNumber(reply0.getReceipt().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var reply1 = ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateLedgerAccount(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendInvoiceTemplate", request.getDetails().getReasonCode());
        return SuspendInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setDueAt(reply0.getReceipt().getDueAt())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setOutcome(SuspendInvoiceTemplateResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Reinstates an invoice template.
     */
    public ReinstateInvoiceTemplateResponse reinstateInvoiceTemplate(ReinstateInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "ReinstateInvoiceTemplate");

        var outbound0 = BrowseLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseLedgerAccounts(outbound0), rpcExecutor);

        var outbound1 = ResolveCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCustomers(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateInvoiceTemplate", request.getDetails().getReasonCode());
        return ReinstateInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setOutcome(ReinstateInvoiceTemplateResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates an invoice template.
     */
    public TerminateInvoiceTemplateResponse terminateInvoiceTemplate(TerminateInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "TerminateInvoiceTemplate");

        var outbound0 = ExplainInvoiceBatchCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainInvoiceBatchCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainInvoiceBatchCalculationRequest, ExplainInvoiceBatchCalculationResponse> port0 = RpcPort.adapt(invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainInvoiceBatchCalculation);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ListAuditEventExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventExportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getInvoiceBatch().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getInvoiceBatch().getMetadataMap())
                        .setOwnerId(request.getInvoiceTemplateId())
                        .setRegion(reply0.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.audit.v1.AuditEventExportService");
        var selected1 = AuditEventExportServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.listAuditEventExports(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateInvoiceTemplate", request.getDetails().getReasonCode());
        return TerminateInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setOutcome(TerminateInvoiceTemplateResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Renews an invoice template.
     */
    public RenewInvoiceTemplateResponse renewInvoiceTemplate(RenewInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "RenewInvoiceTemplate");

        var outbound0 = NormalizeInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeInvoiceTemplateRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        Function<NormalizeInvoiceTemplateRequest, NormalizeInvoiceTemplateResponse> operation0 = invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeInvoiceTemplate;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewInvoiceTemplate", request.getDetails().getReasonCode());
        return RenewInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getInvoiceTemplate().getBillingAddress().getLocality())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setOutcome(RenewInvoiceTemplateResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Expires an invoice template.
     */
    public ExpireInvoiceTemplateResponse expireInvoiceTemplate(ExpireInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "ExpireInvoiceTemplate");

        var outbound0 = ResolveInvoiceTemplatesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<ResolveInvoiceTemplatesResponse>();
        events.publishEvent(new ExpireInvoiceTemplateStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = mapper.expireInvoiceTemplateStage1(request);
        var reply1 = invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateInvoiceBatch(outbound1);

        var outbound2 = PreviewLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getInvoiceBatch().getAudit().getEtag())
                                .build())
                        .setBank(LedgerAccountBankFunding.newBuilder()
                                .setIban(switch (reply1.getInvoiceBatch().getSettlementCase()) {
                                    case BANK -> reply1.getInvoiceBatch().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewLedgerAccount(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireInvoiceTemplate", request.getDetails().getReasonCode());
        return ExpireInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getInvoiceBatch().getAudit().getCreatedBy())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setNumber(reply2.getLedgerAccount().getHolder().getNumber())
                        .build())
                .setOutcome(ExpireInvoiceTemplateResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an invoice template.
     */
    public GetInvoiceTemplateLifecycleResponse getInvoiceTemplateLifecycle(GetInvoiceTemplateLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetInvoiceTemplateLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "GetInvoiceTemplateLifecycle");

        var outbound0 = PreviewPreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewPreferenceRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<PreviewPreferenceRequest, PreviewPreferenceResponse>() {
            @Override
            public PreviewPreferenceResponse apply(PreviewPreferenceRequest input) {
                return customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPreference(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return GetInvoiceTemplateLifecycleResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getPreference().getAudit().getUpdatedBy())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .build();
    }

    /**
     * Schedules an invoice template.
     */
    public ScheduleInvoiceTemplateExpiryResponse scheduleInvoiceTemplateExpiry(ScheduleInvoiceTemplateExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleInvoiceTemplateExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = SuggestTicketsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestTicketsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceTemplate().getAudit().getCreatedAt())
                        .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = ticketingTicketQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).suggestTickets(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = PauseContractClauseWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setContractClause(ContractClause.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getInvoiceTemplate().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> contractsContractClauseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseContractClauseWorkflow(value)).orElseThrow();

        var outbound2 = mapper.scheduleInvoiceTemplateExpiryStage2(request);
        var reply2 = expensesMileageClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalMileageClaimWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleInvoiceTemplateExpiry", request.getInvoiceTemplate().getInvoiceTemplateId());
        return ScheduleInvoiceTemplateExpiryResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getContractClause().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getMileageClaim().getBillingAddress().getPostalCode())
                                .build())
                        .setDueAt(request.getInvoiceTemplate().getDueAt())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setOutcome(ScheduleInvoiceTemplateExpiryResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
