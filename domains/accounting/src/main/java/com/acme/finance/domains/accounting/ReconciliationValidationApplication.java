package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.CalculateReconciliationRequest;
import com.acme.accounting.v1.CalculateReconciliationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CheckAccrualEligibilityRequest;
import com.acme.accounting.v1.CheckAccrualEligibilityResponse;
import com.acme.accounting.v1.CheckReconciliationEligibilityRequest;
import com.acme.accounting.v1.CheckReconciliationEligibilityResponse;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.EstimateAllocationRequest;
import com.acme.accounting.v1.EstimateAllocationResponse;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionRequest;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionResponse;
import com.acme.accounting.v1.ExplainReconciliationDecisionRequest;
import com.acme.accounting.v1.ExplainReconciliationDecisionResponse;
import com.acme.accounting.v1.GetReconciliationRulesRequest;
import com.acme.accounting.v1.GetReconciliationRulesResponse;
import com.acme.accounting.v1.LintReconciliationsRequest;
import com.acme.accounting.v1.LintReconciliationsResponse;
import com.acme.accounting.v1.NormalizeChartOfAccountsRequest;
import com.acme.accounting.v1.NormalizeChartOfAccountsResponse;
import com.acme.accounting.v1.NormalizeReconciliationRequest;
import com.acme.accounting.v1.NormalizeReconciliationResponse;
import com.acme.accounting.v1.PreviewReconciliationRequest;
import com.acme.accounting.v1.PreviewReconciliationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationComment;
import com.acme.accounting.v1.ReconciliationSla;
import com.acme.accounting.v1.ScoreReconciliationRequest;
import com.acme.accounting.v1.ScoreReconciliationResponse;
import com.acme.accounting.v1.SimulateReconciliationRequest;
import com.acme.accounting.v1.SimulateReconciliationResponse;
import com.acme.accounting.v1.ValidateChartOfAccountsRequest;
import com.acme.accounting.v1.ValidateChartOfAccountsResponse;
import com.acme.accounting.v1.ValidateReconciliationRequest;
import com.acme.accounting.v1.ValidateReconciliationResponse;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSla;
import com.acme.assets.v1.QuoteDisposalRequest;
import com.acme.assets.v1.QuoteDisposalResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.SummarizeBankAccountsRequest;
import com.acme.banking.v1.SummarizeBankAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.PreviewBillingAccountRequest;
import com.acme.billing.v1.PreviewBillingAccountResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.dispatch.v1.CreateDispatchOrderRequest;
import com.acme.dispatch.v1.CreateDispatchOrderResponse;
import com.acme.dispatch.v1.DispatchOrder;
import com.acme.dispatch.v1.DispatchOrderBankSettlement;
import com.acme.dispatch.v1.DispatchOrderServiceGrpc;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.LintDocumentsRequest;
import com.acme.documents.v1.LintDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.localization.v1.ResolveTranslationKeysRequest;
import com.acme.localization.v1.ResolveTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyQueryServiceGrpc;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionResponse;
import com.acme.procurement.v1.NormalizeRfqRequest;
import com.acme.procurement.v1.NormalizeRfqResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import java.util.List;
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
 * Checks reconciliations against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ReconciliationValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ReconciliationValidationApplication.class);

    private final AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking;

    private final AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking;

    private final ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking;

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking;

    private final BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final DispatchOrderServiceGrpc.DispatchOrderServiceBlockingStub dispatchDispatchOrderServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceBlockingStub localizationTranslationKeyQueryServiceBlocking;

    private final ReconciliationValidationRequestMapper mapper;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking;

    private final RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking;

    private final AccountingRepository repository;

    public ReconciliationValidationApplication(
            AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking,
            AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking,
            ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking,
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking,
            BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking,
            ChannelRegistry channels,
            DispatchOrderServiceGrpc.DispatchOrderServiceBlockingStub dispatchDispatchOrderServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            ApplicationEventPublisher events,
            TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceBlockingStub localizationTranslationKeyQueryServiceBlocking,
            ReconciliationValidationRequestMapper mapper,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking,
            RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking,
            AccountingRepository repository) {
        this.accountingAccrualValidationServiceBlocking = accountingAccrualValidationServiceBlocking;
        this.accountingAllocationCalculationServiceBlocking = accountingAllocationCalculationServiceBlocking;
        this.accountingChartOfAccountsValidationServiceBlocking = accountingChartOfAccountsValidationServiceBlocking;
        this.accountingReconciliationCalculationServiceBlocking = accountingReconciliationCalculationServiceBlocking;
        this.billingBillingAccountValidationServiceBlocking = billingBillingAccountValidationServiceBlocking;
        this.channels = channels;
        this.dispatchDispatchOrderServiceBlocking = dispatchDispatchOrderServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.events = events;
        this.localizationTranslationKeyQueryServiceBlocking = localizationTranslationKeyQueryServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.procurementPurchaseOrderValidationServiceBlocking = procurementPurchaseOrderValidationServiceBlocking;
        this.procurementRfqValidationServiceBlocking = procurementRfqValidationServiceBlocking;
        this.repository = repository;
    }

    public record NormalizeReconciliationStage2Requested(QuoteDisposalRequest request, CompletableFuture<QuoteDisposalResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a reconciliation.
     */
    public ValidateReconciliationResponse validateReconciliation(ValidateReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = mapper.validateReconciliationStage0(request);
        var reply0 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createNotification(outbound0);

        var outbound1 = SummarizeBankAccountsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeBankAccountsRequest.Details.newBuilder()
                        .putAllFilters(reply0.getNotification().getCustomFieldsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel1 = channels.lookup("acme.banking.v1.BankAccountQueryService");
        var selected1 = BankAccountQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.summarizeBankAccounts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateReconciliation", request.getReconciliation().getReconciliationId());
        return ValidateReconciliationResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getNotification().getRequester().getEmail())
                                .build())
                        .setSubject(request.getReconciliation().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Previews a reconciliation.
     */
    public PreviewReconciliationResponse previewReconciliation(PreviewReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        List<LintDocumentsResponse> results0 = request.getReconciliation().getHistoryList().stream()
                .map(element -> LintDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintDocumentsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getReconciliation().getAssignee().getEmail())
                                .setPhone(request.getReconciliation().getRequester().getPhone())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getChangedAt())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintDocuments(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? LintDocumentsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CreateDispatchOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDispatchOrder(DispatchOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getDocument().getAudit().getUpdatedAt())
                                .build())
                        .setBank(DispatchOrderBankSettlement.newBuilder()
                                .setBic(reply0.getDocument().getBank().getBic())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<CreateDispatchOrderRequest, CreateDispatchOrderResponse>() {
            @Override
            public CreateDispatchOrderResponse apply(CreateDispatchOrderRequest input) {
                return dispatchDispatchOrderServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createDispatchOrder(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        List<NormalizeChartOfAccountsResponse> results2 = request.getReconciliation().getCommentsList().stream()
                .map(element -> NormalizeChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(element.getCreatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeChartOfAccounts(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? NormalizeChartOfAccountsResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewReconciliation", request.getReconciliation().getReconciliationId());
        return PreviewReconciliationResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .putAllCustomFields(request.getReconciliation().getCustomFieldsMap())
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks a reconciliation.
     */
    public CheckReconciliationEligibilityResponse checkReconciliationEligibility(CheckReconciliationEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckReconciliationEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = PreviewBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getReconciliation().getRequester().getPhone())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewBillingAccountRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.billing.v1.BillingAccountValidationService/PreviewBillingAccount", outbound0);
        var reply0 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBillingAccount(command0.payload());

        var outbound1 = NormalizeRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeRfqRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getReconciliation().getRequester().getPhone())
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply0.getBillingAccount().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply1 = procurementRfqValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRfq(outbound1);

        var outbound2 = (CompareAllocationOptionsRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getRfq().getAssignee().getDisplayName())
                                .setEmail(reply1.getRfq().getCustomer().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply2 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAllocationOptions(outbound2);

        var outbound3 = SimulateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setCustomer(ChartOfAccountsRef.newBuilder()
                                .setDisplayName(switch (reply1.getRfq().getOriginCase()) {
                                    case CUSTOMER -> reply1.getRfq().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply3 = accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateReconciliation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckReconciliationEligibility", request.getReconciliation().getReconciliationId());
        return CheckReconciliationEligibilityResponse.newBuilder()
                .setOutcome(CheckReconciliationEligibilityResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getAllocation().getAudit().getCreatedBy())
                                .build())
                        .setPriority(request.getReconciliation().getPriority())
                        .setReconciliationId(request.getReconciliationId())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(reply0.getBillingAccount().getContact().getName())
                                .setPreferredChannel(reply0.getBillingAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
    }

    /**
     * Normalizes a reconciliation.
     */
    public NormalizeReconciliationResponse normalizeReconciliation(NormalizeReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = ExplainPurchaseOrderDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getReconciliation().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> procurementPurchaseOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPurchaseOrderDecision(value));

        var outbound1 = ValidateChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getPurchaseOrder().getAudit().getCreatedAt())
                                                .build())
                                        .addChildren(ChartOfAccounts.newBuilder()
                                                .setChartOfAccountsId(switch (request.getReconciliation().getOriginCase()) {
                                                    case CUSTOMER -> request.getReconciliation().getCustomer().getChartOfAccountsId();
                                                    default -> "";
                                                })
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateChartOfAccounts(outbound1);

        var outbound2 = QuoteDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteDisposalRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(reply0.getPurchaseOrder().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getReconciliation().getAudit().getCreatedAt())
                                .setUpdatedBy(reply1.getChartOfAccounts().getAudit().getUpdatedBy())
                                .build())
                        .setSla(DisposalSla.newBuilder()
                                .setDueAt(request.getReconciliation().getSla().getDueAt())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<QuoteDisposalResponse>();
        events.publishEvent(new NormalizeReconciliationStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = ResolveTranslationKeysRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        RpcPort<ResolveTranslationKeysRequest, ResolveTranslationKeysResponse> port3 = RpcPort.adapt(localizationTranslationKeyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolveTranslationKeys);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeReconciliation", request.getReconciliation().getReconciliationId());
        return NormalizeReconciliationResponse.newBuilder()
                .setOutcome(NormalizeReconciliationResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getChartOfAccounts().getManager().getDisplayName())
                                .build())
                        .addComments(ReconciliationComment.newBuilder()
                                .setCreatedAt(reply0.getPurchaseOrder().getAudit().getCreatedAt())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .setSla(ReconciliationSla.newBuilder()
                                .setBreached(reply2.getDisposal().getSla().getBreached())
                                .build())
                        .setSubject(request.getReconciliation().getSubject())
                        .build())
                .build();
    }

    /**
     * Explains a reconciliation.
     */
    public ExplainReconciliationDecisionResponse explainReconciliationDecision(ExplainReconciliationDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainReconciliationDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        List<ExplainChartOfAccountsDecisionResponse> results0 = request.getReconciliation().getCommentsList().stream()
                .map(element -> ExplainChartOfAccountsDecisionRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(element.getCreatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(element.getAuthorEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainChartOfAccountsDecision(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ExplainChartOfAccountsDecisionResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainReconciliationDecision", request.getReconciliation().getReconciliationId());
        return ExplainReconciliationDecisionResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .setRequester(request.getReconciliation().getRequester())
                        .build())
                .build();
    }

    /**
     * Scores a reconciliation.
     */
    public ScoreReconciliationResponse scoreReconciliation(ScoreReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = CompareAllocationOptionsRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getReconciliation().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getReconciliation().getAssignee().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<CompareAllocationOptionsRequest, CompareAllocationOptionsResponse> port0 = RpcPort.adapt(accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::compareAllocationOptions);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = CalculateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getAllocation().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<CalculateReconciliationRequest, CalculateReconciliationResponse>() {
            @Override
            public CalculateReconciliationResponse apply(CalculateReconciliationRequest input) {
                return accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateReconciliation(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreReconciliation", request.getReconciliation().getReconciliationId());
        return ScoreReconciliationResponse.newBuilder()
                .setBreakdown(ScoreReconciliationResponse.Breakdown.newBuilder()
                        .addLines(ScoreReconciliationResponse.Breakdown.Line.newBuilder()
                                .setAmount(reply0.getAllocation().getDiscount().getAmount())
                                .build())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getAllocation().getOwner().getUserId())
                                .build())
                        .putAllCustomFields(request.getReconciliation().getCustomFieldsMap())
                        .setReconciliationId(request.getReconciliationId())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getReconciliation().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lints a reconciliation.
     */
    public LintReconciliationsResponse lintReconciliations(LintReconciliationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintReconciliations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        List<CheckAccrualEligibilityResponse> results0 = request.getReconciliation().getCommentsList().stream()
                .map(element -> CheckAccrualEligibilityRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckAccrualEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAccrualEligibility(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CheckAccrualEligibilityResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintReconciliations", request.getReconciliation().getReconciliationId());
        return LintReconciliationsResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .setSubject(request.getReconciliation().getSubject())
                        .build())
                .build();
    }

    /**
     * Returns a reconciliation.
     */
    public GetReconciliationRulesResponse getReconciliationRules(GetReconciliationRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetReconciliationRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Reconciliation", request.getReconciliationId(), "GetReconciliationRules");

        var outbound0 = EstimateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAllocation(value));
        return GetReconciliationRulesResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getAllocation().getOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getAllocation().getAudit().getEtag())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .build();
    }
}
