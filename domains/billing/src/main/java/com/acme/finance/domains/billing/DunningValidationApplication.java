package com.acme.finance.domains.billing;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CheckDunningEligibilityRequest;
import com.acme.billing.v1.CheckDunningEligibilityResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteBankSettlement;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningCalculationServiceGrpc;
import com.acme.billing.v1.DunningComment;
import com.acme.billing.v1.DunningSla;
import com.acme.billing.v1.EstimateCreditNoteRequest;
import com.acme.billing.v1.EstimateCreditNoteResponse;
import com.acme.billing.v1.EstimateDunningRequest;
import com.acme.billing.v1.EstimateDunningResponse;
import com.acme.billing.v1.ExplainDunningDecisionRequest;
import com.acme.billing.v1.ExplainDunningDecisionResponse;
import com.acme.billing.v1.ExplainPaymentTermDecisionRequest;
import com.acme.billing.v1.ExplainPaymentTermDecisionResponse;
import com.acme.billing.v1.GetDunningRulesRequest;
import com.acme.billing.v1.GetDunningRulesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LintDunningsRequest;
import com.acme.billing.v1.LintDunningsResponse;
import com.acme.billing.v1.NormalizeBillingAccountRequest;
import com.acme.billing.v1.NormalizeBillingAccountResponse;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.NormalizeDunningRequest;
import com.acme.billing.v1.NormalizeDunningResponse;
import com.acme.billing.v1.PreviewDunningRequest;
import com.acme.billing.v1.PreviewDunningResponse;
import com.acme.billing.v1.QuoteDunningRequest;
import com.acme.billing.v1.QuoteDunningResponse;
import com.acme.billing.v1.ScoreDunningRequest;
import com.acme.billing.v1.ScoreDunningResponse;
import com.acme.billing.v1.ScoreInvoiceRequest;
import com.acme.billing.v1.ScoreInvoiceResponse;
import com.acme.billing.v1.SimulateInvoiceRequest;
import com.acme.billing.v1.SimulateInvoiceResponse;
import com.acme.billing.v1.ValidateDunningRequest;
import com.acme.billing.v1.ValidateDunningResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptCardSettlement;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ValidateReceiptRequest;
import com.acme.expenses.v1.ValidateReceiptResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountPoliciesRequest;
import com.acme.insurance.v1.CountPoliciesResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.localization.v1.ExportTranslationKeysRequest;
import com.acme.localization.v1.ExportTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyQueryServiceGrpc;
import com.acme.metrics.v1.LintMetricSeriesesRequest;
import com.acme.metrics.v1.LintMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.risk.v1.CalculateStressTestRequest;
import com.acme.risk.v1.CalculateStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks dunnings against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class DunningValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(DunningValidationApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking;

    private final CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking;

    private final CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking;

    private final DunningCalculationServiceGrpc.DunningCalculationServiceBlockingStub billingDunningCalculationServiceBlocking;

    private final InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking;

    private final InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync;

    private final ApplicationEventPublisher events;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceFutureStub insurancePolicyQueryServiceFuture;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceBlockingStub localizationTranslationKeyQueryServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final BillingRepository repository;

    private final StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking;

    public DunningValidationApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking,
            CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking,
            CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking,
            DunningCalculationServiceGrpc.DunningCalculationServiceBlockingStub billingDunningCalculationServiceBlocking,
            InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking,
            InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking,
            ChannelRegistry channels,
            ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync,
            ApplicationEventPublisher events,
            PolicyQueryServiceGrpc.PolicyQueryServiceFutureStub insurancePolicyQueryServiceFuture,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceBlockingStub localizationTranslationKeyQueryServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            BillingRepository repository,
            StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.billingBillingAccountValidationServiceBlocking = billingBillingAccountValidationServiceBlocking;
        this.billingCreditNoteCalculationServiceBlocking = billingCreditNoteCalculationServiceBlocking;
        this.billingCreditNoteValidationServiceBlocking = billingCreditNoteValidationServiceBlocking;
        this.billingDunningCalculationServiceBlocking = billingDunningCalculationServiceBlocking;
        this.billingInvoiceCalculationServiceBlocking = billingInvoiceCalculationServiceBlocking;
        this.billingInvoiceValidationServiceBlocking = billingInvoiceValidationServiceBlocking;
        this.channels = channels;
        this.contractsContractServiceAsync = contractsContractServiceAsync;
        this.events = events;
        this.insurancePolicyQueryServiceFuture = insurancePolicyQueryServiceFuture;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.localizationTranslationKeyQueryServiceBlocking = localizationTranslationKeyQueryServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.repository = repository;
        this.riskStressTestCalculationServiceBlocking = riskStressTestCalculationServiceBlocking;
    }

    public record GetDunningRulesStage0Requested(ExplainPaymentTermDecisionRequest request, CompletableFuture<ExplainPaymentTermDecisionResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a dunning.
     */
    public ValidateDunningResponse validateDunning(ValidateDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var results0 = new ArrayList<ExportTranslationKeysResponse>();
        for (var element : request.getDunning().getHistoryList()) {
            ExportTranslationKeysRequest itemRequest = ExportTranslationKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportTranslationKeysRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getChangedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDunning().getDunningId())
                        .build())
                .build();
            var responses = localizationTranslationKeyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportTranslationKeys(itemRequest);
            responses.forEachRemaining(results0::add);
        }
        var reply0 = results0.isEmpty() ? ExportTranslationKeysResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = BatchCreateContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateContractsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateContractsResponse.getDefaultInstance());
        var input1 = contractsContractServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateContracts(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDunning().getAudit().getCreatedAt())
                                .setCreatedBy(request.getDunning().getAudit().getCreatedBy())
                                .setEtag(request.getDunning().getAudit().getEtag())
                                .setUpdatedAt(request.getDunning().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getDunning().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateDunning", request.getDunning().getSubject());
        return ValidateDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAssignee(request.getDunning().getAssignee())
                        .setDunningId(request.getDunningId())
                        .build())
                .build();
    }

    /**
     * Previews a dunning.
     */
    public PreviewDunningResponse previewDunning(PreviewDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = LintMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintMetricSeriesesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDunning().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            LintMetricSeriesesResponse fetch(LintMetricSeriesesRequest input) {
                return metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintMetricSerieses(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewDunning", request.getDunning().getDunningId());
        return PreviewDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getMetricSeries().getOwner().getDisplayName())
                                .build())
                        .putAllCustomFields(request.getDunning().getCustomFieldsMap())
                        .setDunningId(request.getDunningId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Checks a dunning.
     */
    public CheckDunningEligibilityResponse checkDunningEligibility(CheckDunningEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckDunningEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = ScoreInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getDunning().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getDunning().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(switch (request.getDunning().getOriginCase()) {
                                    case CUSTOMER -> request.getDunning().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .setEmail(request.getDunning().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreInvoice(outbound0);

        var outbound1 = ValidateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateReceiptRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getInvoice().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getDunning().getAudit().getUpdatedAt())
                                .build())
                        .setCard(ReceiptCardSettlement.newBuilder()
                                .setHolderName(reply0.getInvoice().getCard().getHolderName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.expenses.v1.ReceiptValidationService");
        var selected1 = ReceiptValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.validateReceipt(outbound1);

        var results2 = new ArrayList<EstimateCreditNoteResponse>();
        for (var element : request.getDunning().getCommentsList()) {
            EstimateCreditNoteRequest itemRequest = EstimateCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setBank(CreditNoteBankSettlement.newBuilder()
                                .setAccountHolder(reply1.getReceipt().getBank().getAccountHolder())
                                .build())
                        .build())
                .build();
            results2.add(billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCreditNote(itemRequest));
        }
        var reply2 = results2.isEmpty() ? EstimateCreditNoteResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = CalculateStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getCreditNote().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<CalculateStressTestRequest, CalculateStressTestResponse>() {
            @Override
            public CalculateStressTestResponse apply(CalculateStressTestRequest input) {
                return riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateStressTest(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckDunningEligibility", request.getDunning().getDunningId());
        return CheckDunningEligibilityResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getInvoice().getAudit().getEtag())
                                .setUpdatedAt(reply3.getStressTest().getAudit().getUpdatedAt())
                                .build())
                        .putAllCustomFields(request.getDunning().getCustomFieldsMap())
                        .setDunningId(request.getDunningId())
                        .build())
                .setOutcome(CheckDunningEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Normalizes a dunning.
     */
    public NormalizeDunningResponse normalizeDunning(NormalizeDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var builder0 = NormalizeCreditNoteRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setCreditNote(CreditNote.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(request.getDunning().getAudit().getUpdatedAt())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = billingCreditNoteValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCreditNote(outbound0);

        var outbound1 = NormalizeBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getDunning().getAudit().getUpdatedBy())
                                .build())
                        .setBank(BillingAccountBankFunding.newBuilder()
                                .setBic(switch (reply0.getCreditNote().getSettlementCase()) {
                                    case BANK -> reply0.getCreditNote().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getCreditNote().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeBillingAccount(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeDunning", request.getDunning().getDunningId());
        return NormalizeDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .addComments(DunningComment.newBuilder()
                                .setCreatedAt(reply0.getCreditNote().getAudit().getCreatedAt())
                                .build())
                        .putAllCustomFields(request.getDunning().getCustomFieldsMap())
                        .setDunningId(request.getDunningId())
                        .build())
                .setOutcome(NormalizeDunningResponse.Outcome.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a dunning.
     */
    public ExplainDunningDecisionResponse explainDunningDecision(ExplainDunningDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainDunningDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = ExplainTaxWithholdingDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainTaxWithholdingDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDunning().getRequester().getEmail())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDunning().getAssignee().getEmail())
                                .setUserId(request.getDunning().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.TaxWithholdingValidationService");
        var selected0 = TaxWithholdingValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.explainTaxWithholdingDecision(outbound0);

        var results1 = new ArrayList<QuoteDunningResponse>();
        for (var element : request.getDunning().getCommentsList()) {
            QuoteDunningRequest itemRequest = QuoteDunningRequest.newBuilder()
                .setContext(request.getContext())
                .setDunning(Dunning.newBuilder()
                        .addComments(DunningComment.newBuilder()
                                .setAuthorEmail(element.getAuthorEmail())
                                .setCommentId(element.getCommentId())
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build();
            results1.add(billingDunningCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteDunning(itemRequest));
        }
        var reply1 = results1.isEmpty() ? QuoteDunningResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = LintJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintJournalEntriesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply1.getDunning().getRequester().getPhone())
                                .build())
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getTaxWithholding().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setCurrencyCode(switch (request.getDunning().getOriginCase()) {
                                    case CUSTOMER -> request.getDunning().getCustomer().getCurrencyCode();
                                    default -> "";
                                })
                                .build())
                        .addLines(JournalEntryLine.newBuilder()
                                .setTax(JournalEntryLineTax.newBuilder()
                                        .setAmount(reply0.getTaxWithholding().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintJournalEntries(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainDunningDecision", request.getDunning().getDunningId());
        return ExplainDunningDecisionResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getJournalEntry().getAudit().getCreatedBy())
                                .build())
                        .addComments(DunningComment.newBuilder()
                                .setCreatedAt(reply0.getTaxWithholding().getAudit().getCreatedAt())
                                .build())
                        .setDunningId(request.getDunningId())
                        .setStatus(request.getDunning().getStatus())
                        .build())
                .setOutcome(ExplainDunningDecisionResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a dunning.
     */
    public ScoreDunningResponse scoreDunning(ScoreDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = BeanCopies.copy(EstimateDunningRequest.newBuilder()
                .setContext(request.getContext())
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDunning().getAssignee().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDunning().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDunning().getAssignee().getEmail())
                                .build())
                        .setSla(DunningSla.newBuilder()
                                .setBreached(request.getDunning().getSla().getBreached())
                                .build())
                        .build())
                .build(), EstimateDunningRequest.class);
        var reply0 = billingDunningCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateDunning(outbound0);

        var outbound1 = SimulateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getDunning().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getDunning().getAudit().getCreatedBy())
                                .setEtag(request.getDunning().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            SimulateInvoiceResponse fetch(SimulateInvoiceRequest input) {
                return billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateInvoice(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreDunning", request.getDunning().getDunningId());
        return ScoreDunningResponse.newBuilder()
                .setBreakdown(ScoreDunningResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                        .build())
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getInvoice().getCounterparty().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getInvoice().getAudit().getEtag())
                                .build())
                        .setDunningId(request.getDunningId())
                        .setSubject(request.getDunning().getSubject())
                        .build())
                .build();
    }

    /**
     * Lints a dunning.
     */
    public LintDunningsResponse lintDunnings(LintDunningsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintDunnings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = CountPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDunning().getSla().getDueAt())
                        .setOwnerId(request.getDunning().getAssignee().getUserId())
                        .build())
                .build();
        var pending0 = insurancePolicyQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).countPolicies(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintDunnings", request.getDunning().getDunningId());
        return LintDunningsResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .putAllCustomFields(request.getDunning().getCustomFieldsMap())
                        .setDunningId(request.getDunningId())
                        .build())
                .setOutcome(LintDunningsResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a dunning.
     */
    public GetDunningRulesResponse getDunningRules(GetDunningRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDunningRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "GetDunningRules");

        var outbound0 = ExplainPaymentTermDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPaymentTermDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ExplainPaymentTermDecisionResponse>();
        events.publishEvent(new GetDunningRulesStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return GetDunningRulesResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setDunningId(request.getDunningId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getPaymentTerm().getOwner().getEmail())
                                .build())
                        .build())
                .build();
    }
}
