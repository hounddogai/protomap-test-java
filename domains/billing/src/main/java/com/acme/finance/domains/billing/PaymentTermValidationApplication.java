package com.acme.finance.domains.billing;

import com.acme.billing.v1.CheckPaymentTermEligibilityRequest;
import com.acme.billing.v1.CheckPaymentTermEligibilityResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.CreditNoteLineTax;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.ExplainCreditNoteCalculationRequest;
import com.acme.billing.v1.ExplainCreditNoteCalculationResponse;
import com.acme.billing.v1.ExplainCreditNoteDecisionRequest;
import com.acme.billing.v1.ExplainCreditNoteDecisionResponse;
import com.acme.billing.v1.ExplainPaymentTermCalculationRequest;
import com.acme.billing.v1.ExplainPaymentTermCalculationResponse;
import com.acme.billing.v1.ExplainPaymentTermDecisionRequest;
import com.acme.billing.v1.ExplainPaymentTermDecisionResponse;
import com.acme.billing.v1.GetPaymentTermRulesRequest;
import com.acme.billing.v1.GetPaymentTermRulesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceCardSettlement;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LintInvoicesRequest;
import com.acme.billing.v1.LintInvoicesResponse;
import com.acme.billing.v1.LintPaymentTermsRequest;
import com.acme.billing.v1.LintPaymentTermsResponse;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.PaymentTermDiscountAction;
import com.acme.billing.v1.PreviewPaymentTermRequest;
import com.acme.billing.v1.PreviewPaymentTermResponse;
import com.acme.billing.v1.RecalculatePaymentTermsRequest;
import com.acme.billing.v1.RecalculatePaymentTermsResponse;
import com.acme.billing.v1.ScorePaymentTermRequest;
import com.acme.billing.v1.ScorePaymentTermResponse;
import com.acme.billing.v1.ValidateInvoiceRequest;
import com.acme.billing.v1.ValidateInvoiceResponse;
import com.acme.billing.v1.ValidatePaymentTermRequest;
import com.acme.billing.v1.ValidatePaymentTermResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagCondition;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.travel.v1.SuggestTripsRequest;
import com.acme.travel.v1.SuggestTripsResponse;
import com.acme.travel.v1.TripQueryServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.PreviewCashPositionRequest;
import com.acme.treasury.v1.PreviewCashPositionResponse;
import com.acme.treasury.v1.SimulateBankTransferRequest;
import com.acme.treasury.v1.SimulateBankTransferResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks payment terms against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PaymentTermValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(PaymentTermValidationApplication.class);

    private final InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking;

    private final PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final TripQueryServiceGrpc.TripQueryServiceBlockingStub travelTripQueryServiceBlocking;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking;

    public PaymentTermValidationApplication(
            InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking,
            PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            TripQueryServiceGrpc.TripQueryServiceBlockingStub travelTripQueryServiceBlocking,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking) {
        this.billingInvoiceValidationServiceBlocking = billingInvoiceValidationServiceBlocking;
        this.billingPaymentTermCalculationServiceBlocking = billingPaymentTermCalculationServiceBlocking;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.travelTripQueryServiceBlocking = travelTripQueryServiceBlocking;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryCashPositionValidationServiceBlocking = treasuryCashPositionValidationServiceBlocking;
    }

    public record ScorePaymentTermStage0Requested(RecalculatePaymentTermsRequest request, CompletableFuture<RecalculatePaymentTermsResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a payment term.
     */
    public ValidatePaymentTermResponse validatePaymentTerm(ValidatePaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidatePaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = LintInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintInvoicesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPaymentTerm().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getPaymentTerm().getOwner().getDisplayName())
                                .build())
                        .addLines(InvoiceLine.newBuilder()
                                .setTax(InvoiceLineTax.newBuilder()
                                        .setAmount(request.getPaymentTerm().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintInvoices(outbound0);

        var outbound1 = ExplainCreditNoteCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPaymentTerm().getAudit().getEtag())
                                .setUpdatedAt(reply0.getInvoice().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getInvoice().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.billing.v1.CreditNoteCalculationService");
        var selected1 = CreditNoteCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.explainCreditNoteCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidatePaymentTerm", request.getPaymentTerm().getPaymentTermId());
        return ValidatePaymentTermResponse.newBuilder()
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getInvoice().getAudit().getCreatedAt())
                                .build())
                        .setOwner(request.getPaymentTerm().getOwner())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .build();
    }

    /**
     * Previews a payment term.
     */
    public PreviewPaymentTermResponse previewPaymentTerm(PreviewPaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewPaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = PreviewCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPaymentTerm().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewCashPositionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getPaymentTerm().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewCashPositionRequest, PreviewCashPositionResponse> port0 = RpcPort.adapt(treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewCashPosition);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ValidateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setCard(InvoiceCardSettlement.newBuilder()
                                .setToken(reply0.getCashPosition().getCard().getToken())
                                .build())
                        .build())
                .build();
        var reply1 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateInvoice(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewPaymentTerm", request.getPaymentTerm().getPaymentTermId());
        return PreviewPaymentTermResponse.newBuilder()
                .setOutcome(PreviewPaymentTermResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setPaymentTermId(request.getPaymentTermId())
                        .setPriority(request.getPaymentTerm().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks a payment term.
     */
    public CheckPaymentTermEligibilityResponse checkPaymentTermEligibility(CheckPaymentTermEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckPaymentTermEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = CompareBudgetOptionsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getPaymentTerm().getOwner().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(request.getPaymentTerm().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareBudgetOptions(outbound0);

        var futures1 = request.getPaymentTerm().getConditionsList().stream()
                .map(element -> NormalizeFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .addConditions(FeatureFlagCondition.newBuilder()
                                .setNegate(element.getNegate())
                                .setValue(element.getValue())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeFeatureFlag(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? NormalizeFeatureFlagResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckPaymentTermEligibility", request.getPaymentTerm().getPaymentTermId());
        return CheckPaymentTermEligibilityResponse.newBuilder()
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setEffective(request.getPaymentTerm().getEffective())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Explains a payment term.
     */
    public ExplainPaymentTermDecisionResponse explainPaymentTermDecision(ExplainPaymentTermDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPaymentTermDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = RpcSupport.unpack(Any.pack(ExplainPaymentTermCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPaymentTermCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPaymentTerm().getAudit().getUpdatedBy())
                                .build())
                        .setDiscount(PaymentTermDiscountAction.newBuilder()
                                .setAmount(request.getPaymentTerm().getDiscount().getAmount())
                                .build())
                        .build())
                .build()), ExplainPaymentTermCalculationRequest.class);
        var reply0 = billingPaymentTermCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPaymentTermCalculation(outbound0);

        var outbound1 = PreviewSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getPaymentTerm().getOwner().getUserId())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.search.v1.SearchIndexValidationService/PreviewSearchIndex", outbound1);
        var reply1 = searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSearchIndex(command1.payload());

        var outbound2 = SuggestTripsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestTripsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPaymentTerm().getAudit().getUpdatedAt())
                        .setOwnerId(request.getPaymentTerm().getOwner().getUserId())
                        .setRegion(switch (reply1.getSearchIndex().getLocatorCase()) {
                            case ADDRESS -> reply1.getSearchIndex().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply2 = travelTripQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestTrips(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPaymentTermDecision", request.getPaymentTerm().getPaymentTermId());
        return ExplainPaymentTermDecisionResponse.newBuilder()
                .setOutcome(ExplainPaymentTermDecisionResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setName(reply1.getSearchIndex().getName())
                        .putAllParameters(reply0.getPaymentTerm().getParametersMap())
                        .setPaymentTermId(request.getPaymentTermId())
                        .setVersion(request.getPaymentTerm().getVersion())
                        .build())
                .build();
    }

    /**
     * Scores a payment term.
     */
    public ScorePaymentTermResponse scorePaymentTerm(ScorePaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScorePaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = RecalculatePaymentTermsRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPaymentTerm().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getPaymentTerm().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<RecalculatePaymentTermsResponse>();
        events.publishEvent(new ScorePaymentTermStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = ExplainCreditNoteDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .addLines(CreditNoteLine.newBuilder()
                                .setTax(CreditNoteLineTax.newBuilder()
                                        .setAmount(reply0.getPaymentTerm().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.billing.v1.CreditNoteValidationService");
        var selected1 = CreditNoteValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.explainCreditNoteDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScorePaymentTerm", request.getPaymentTerm().getPaymentTermId());
        return ScorePaymentTermResponse.newBuilder()
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getCreditNote().getAudit().getCreatedBy())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .setVersion(request.getPaymentTerm().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints a payment term.
     */
    public LintPaymentTermsResponse lintPaymentTerms(LintPaymentTermsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintPaymentTerms")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = NormalizeCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeCreditNoteRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.billing.v1.CreditNoteValidationService");
        var serviceName0 = "acme.billing.v1" + "." + "CreditNoteValidationService";
        var methodName0 = "Nor" + "malizeCreditNote";
        var descriptor0 = MethodDescriptor.<NormalizeCreditNoteRequest, NormalizeCreditNoteResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(NormalizeCreditNoteRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(NormalizeCreditNoteResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(SimulateBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCreditNote().getAudit().getCreatedBy())
                                .setEtag(request.getPaymentTerm().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateBankTransferRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getCreditNote().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build()), SimulateBankTransferRequest.class);
        var reply1 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateBankTransfer(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintPaymentTerms", request.getPaymentTerm().getPaymentTermId());
        return LintPaymentTermsResponse.newBuilder()
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getBankTransfer().getAudit().getCreatedBy())
                                .build())
                        .setName(request.getPaymentTerm().getName())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getBankTransfer().getCounterparty().getEmail())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a payment term.
     */
    public GetPaymentTermRulesResponse getPaymentTermRules(GetPaymentTermRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPaymentTermRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentTerm", request.getPaymentTermId(), "GetPaymentTermRules");
        return GetPaymentTermRulesResponse.newBuilder()
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .build();
    }
}
