package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.ResolveAuditTrailsRequest;
import com.acme.audit.v1.ResolveAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.NormalizeComplianceCaseRequest;
import com.acme.compliance.v1.NormalizeComplianceCaseResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.BatchCreateExpenseReportsRequest;
import com.acme.expenses.v1.BatchCreateExpenseReportsResponse;
import com.acme.expenses.v1.ExpenseReportServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.insurance.v1.ComparePremiumOptionsRequest;
import com.acme.insurance.v1.ComparePremiumOptionsResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumLineTax;
import com.acme.invoicing.v1.CheckEInvoiceEligibilityRequest;
import com.acme.invoicing.v1.CheckEInvoiceEligibilityResponse;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityResponse;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityResponse;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceAttachment;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.ExplainEInvoiceDecisionRequest;
import com.acme.invoicing.v1.ExplainEInvoiceDecisionResponse;
import com.acme.invoicing.v1.GetEInvoiceRulesRequest;
import com.acme.invoicing.v1.GetEInvoiceRulesResponse;
import com.acme.invoicing.v1.GetInvoiceBatchRatesRequest;
import com.acme.invoicing.v1.GetInvoiceBatchRatesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchBankSettlement;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCardSettlement;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateBankSettlement;
import com.acme.invoicing.v1.InvoiceTemplateCardSettlement;
import com.acme.invoicing.v1.InvoiceTemplateLine;
import com.acme.invoicing.v1.InvoiceTemplateLineTax;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.LintEInvoicesRequest;
import com.acme.invoicing.v1.LintEInvoicesResponse;
import com.acme.invoicing.v1.LintInvoiceTemplatesRequest;
import com.acme.invoicing.v1.LintInvoiceTemplatesResponse;
import com.acme.invoicing.v1.NormalizeEInvoiceRequest;
import com.acme.invoicing.v1.NormalizeEInvoiceResponse;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateRequest;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateResponse;
import com.acme.invoicing.v1.PreviewEInvoiceRequest;
import com.acme.invoicing.v1.PreviewEInvoiceResponse;
import com.acme.invoicing.v1.PreviewInvoiceTemplateRequest;
import com.acme.invoicing.v1.PreviewInvoiceTemplateResponse;
import com.acme.invoicing.v1.QuoteInvoiceBatchRequest;
import com.acme.invoicing.v1.QuoteInvoiceBatchResponse;
import com.acme.invoicing.v1.ScoreEInvoiceRequest;
import com.acme.invoicing.v1.ScoreEInvoiceResponse;
import com.acme.invoicing.v1.ScoreInvoiceBatchRequest;
import com.acme.invoicing.v1.ScoreInvoiceBatchResponse;
import com.acme.invoicing.v1.ScoreInvoiceTemplateRequest;
import com.acme.invoicing.v1.ScoreInvoiceTemplateResponse;
import com.acme.invoicing.v1.SimulateEInvoiceRequest;
import com.acme.invoicing.v1.SimulateEInvoiceResponse;
import com.acme.invoicing.v1.ValidateEInvoiceRequest;
import com.acme.invoicing.v1.ValidateEInvoiceResponse;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.ResolveBankTransfersRequest;
import com.acme.treasury.v1.ResolveBankTransfersResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Checks e invoices against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class EInvoiceValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(EInvoiceValidationApplication.class);

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final AuditTrailQueryServiceGrpc.AuditTrailQueryServiceFutureStub auditAuditTrailQueryServiceFuture;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExpenseReportServiceGrpc.ExpenseReportServiceStub expensesExpenseReportServiceAsync;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking;

    private final InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking;

    private final InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking;

    private final InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final InvoicingRepository repository;

    private final Executor rpcExecutor;

    private final BankTransferQueryServiceGrpc.BankTransferQueryServiceFutureStub treasuryBankTransferQueryServiceFuture;

    public EInvoiceValidationApplication(
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            AuditTrailQueryServiceGrpc.AuditTrailQueryServiceFutureStub auditAuditTrailQueryServiceFuture,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            ApplicationEventPublisher events,
            ExpenseReportServiceGrpc.ExpenseReportServiceStub expensesExpenseReportServiceAsync,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking,
            InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking,
            InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking,
            InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            InvoicingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            BankTransferQueryServiceGrpc.BankTransferQueryServiceFutureStub treasuryBankTransferQueryServiceFuture) {
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.auditAuditTrailQueryServiceFuture = auditAuditTrailQueryServiceFuture;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.events = events;
        this.expensesExpenseReportServiceAsync = expensesExpenseReportServiceAsync;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.invoicingEInvoiceCalculationServiceBlocking = invoicingEInvoiceCalculationServiceBlocking;
        this.invoicingInvoiceBatchCalculationServiceBlocking = invoicingInvoiceBatchCalculationServiceBlocking;
        this.invoicingInvoiceBatchValidationServiceBlocking = invoicingInvoiceBatchValidationServiceBlocking;
        this.invoicingInvoiceTemplateValidationServiceBlocking = invoicingInvoiceTemplateValidationServiceBlocking;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.treasuryBankTransferQueryServiceFuture = treasuryBankTransferQueryServiceFuture;
    }

    public record NormalizeEInvoiceStage0Requested(QuoteInvoiceBatchRequest request, CompletableFuture<QuoteInvoiceBatchResponse> completion, io.grpc.Context context) {}

    /**
     * Validates an e invoice.
     */
    public ValidateEInvoiceResponse validateEInvoice(ValidateEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = QuoteInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteInvoiceBatchRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getEInvoice().getBillingAddress().getLocality())
                                .setPostalCode(request.getEInvoice().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getEInvoice().getAudit().getCreatedAt())
                                .build())
                        .setCard(InvoiceBatchCardSettlement.newBuilder()
                                .setToken(switch (request.getEInvoice().getSettlementCase()) {
                                    case CARD -> request.getEInvoice().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteInvoiceBatch(outbound0);

        var futures1 = request.getEInvoice().getLinesList().stream()
                .map(element -> ComparePremiumOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .addLines(PremiumLine.newBuilder()
                                .setTax(PremiumLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePremiumOptions(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ComparePremiumOptionsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BatchCreateExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateExpenseReportsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateExpenseReportsResponse.getDefaultInstance());
        var input2 = expensesExpenseReportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateExpenseReports(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getEInvoice().getAudit().getCreatedAt())
                                .setCreatedBy(request.getEInvoice().getAudit().getCreatedBy())
                                .setEtag(request.getEInvoice().getAudit().getEtag())
                                .setUpdatedAt(request.getEInvoice().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateEInvoice", request.getEInvoice().getEInvoiceId());
        return ValidateEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getInvoiceBatch().getBillingAddress().getLinesList())
                                .build())
                        .setCounterparty(request.getEInvoice().getCounterparty())
                        .setEInvoiceId(request.getEInvoiceId())
                        .putAllMetadata(reply0.getInvoiceBatch().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Previews an e invoice.
     */
    public PreviewEInvoiceResponse previewEInvoice(PreviewEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = NormalizeComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getEInvoice().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getEInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<NormalizeComplianceCaseRequest, NormalizeComplianceCaseResponse> operation0 = complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeComplianceCase;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = ScoreInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreInvoiceBatchRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getEInvoice().getBillingAddress().getRecipient())
                                .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getEInvoice().getBillingAddress().getLocality())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(switch (reply0.getComplianceCase().getOriginCase()) {
                                    case CUSTOMER -> reply0.getComplianceCase().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreInvoiceBatch(outbound1);

        var outbound2 = GetInvoiceBatchRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatchId(request.getEInvoice().getCounterparty().getInvoiceBatchId())
                .build();
        var channel2 = channels.lookup("acme.invoicing.v1.InvoiceBatchCalculationService");
        var serviceName2 = "acme.invoicing.v1" + "." + "InvoiceBatchCalculationService";
        var methodName2 = "Get" + "InvoiceBatchRates";
        var descriptor2 = MethodDescriptor.<GetInvoiceBatchRatesRequest, GetInvoiceBatchRatesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(GetInvoiceBatchRatesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(GetInvoiceBatchRatesResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewEInvoice", request.getEInvoice().getEInvoiceId());
        return PreviewEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply2.getInvoiceBatch().getBillingAddress().getLinesList())
                                .setRecipient(reply2.getInvoiceBatch().getBillingAddress().getRecipient())
                                .build())
                        .setCounterparty(request.getEInvoice().getCounterparty())
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setOutcome(PreviewEInvoiceResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks an e invoice.
     */
    public CheckEInvoiceEligibilityResponse checkEInvoiceEligibility(CheckEInvoiceEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckEInvoiceEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        List<SimulateEInvoiceResponse> results0 = request.getEInvoice().getAttachmentsList().stream()
                .map(element -> SimulateEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setEInvoice(EInvoice.newBuilder()
                        .addAttachments(EInvoiceAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(invoicingEInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateEInvoice(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? SimulateEInvoiceResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ResolveAuditTrailsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = auditAuditTrailQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditTrails(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = BrowseLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getEInvoice().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getEInvoice().getCounterparty().getInvoiceBatchId())
                        .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var adapter2 = new Function<BrowseLedgerAccountsRequest, BrowseLedgerAccountsResponse>() {
            @Override
            public BrowseLedgerAccountsResponse apply(BrowseLedgerAccountsRequest input) {
                return ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseLedgerAccounts(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var builder3 = CheckInvoiceBatchEligibilityRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setInvoiceBatch(InvoiceBatch.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedBy(reply0.getEInvoice().getAudit().getCreatedBy())
                        .build())
                .setCard(InvoiceBatchCardSettlement.newBuilder()
                        .setHolderName(request.getEInvoice().getCard().getHolderName())
                        .setNetwork(request.getEInvoice().getCard().getNetwork())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkInvoiceBatchEligibility(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckEInvoiceEligibility", request.getEInvoice().getEInvoiceId());
        return CheckEInvoiceEligibilityResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setDisplayName(reply3.getInvoiceBatch().getCounterparty().getDisplayName())
                                .build())
                        .setDueAt(request.getEInvoice().getDueAt())
                        .setEInvoiceId(request.getEInvoiceId())
                        .putAllMetadata(reply3.getInvoiceBatch().getMetadataMap())
                        .build())
                .setOutcome(CheckEInvoiceEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Normalizes an e invoice.
     */
    public NormalizeEInvoiceResponse normalizeEInvoice(NormalizeEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = QuoteInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBank(InvoiceBatchBankSettlement.newBuilder()
                                .setBic(request.getEInvoice().getBank().getBic())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<QuoteInvoiceBatchResponse>();
        events.publishEvent(new NormalizeEInvoiceStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = ResolveBankTransfersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = treasuryBankTransferQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).resolveBankTransfers(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeEInvoice", request.getEInvoice().getEInvoiceId());
        return NormalizeEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getInvoiceBatch().getAudit().getCreatedBy())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNotes(request.getEInvoice().getNotes())
                        .build())
                .setOutcome(NormalizeEInvoiceResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Explains an e invoice.
     */
    public ExplainEInvoiceDecisionResponse explainEInvoiceDecision(ExplainEInvoiceDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainEInvoiceDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var futures0 = request.getEInvoice().getLinesList().stream()
                .map(element -> ScoreInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .addLines(InvoiceTemplateLine.newBuilder()
                                .setTax(InvoiceTemplateLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreInvoiceTemplate(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ScoreInvoiceTemplateResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = PreviewInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewInvoiceTemplateRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setCard(InvoiceTemplateCardSettlement.newBuilder()
                                .setNetwork(switch (request.getEInvoice().getSettlementCase()) {
                                    case CARD -> request.getEInvoice().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getInvoiceTemplate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewInvoiceTemplate(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainEInvoiceDecision", request.getEInvoice().getEInvoiceId());
        return ExplainEInvoiceDecisionResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getInvoiceTemplate().getBillingAddress().getLocality())
                                .build())
                        .setCounterparty(request.getEInvoice().getCounterparty())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setIssuedAt(reply1.getInvoiceTemplate().getIssuedAt())
                        .build())
                .build();
    }

    /**
     * Scores an e invoice.
     */
    public ScoreEInvoiceResponse scoreEInvoice(ScoreEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = CompareInvoiceBatchOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getEInvoice().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getEInvoice().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareInvoiceBatchOptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreEInvoice", request.getEInvoice().getEInvoiceId());
        return ScoreEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .putAllMetadata(request.getEInvoice().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lints an e invoice.
     */
    public LintEInvoicesResponse lintEInvoices(LintEInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintEInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = NormalizeInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBank(InvoiceTemplateBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getEInvoice().getSettlementCase()) {
                                    case BANK -> request.getEInvoice().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeInvoiceTemplate(outbound0);

        var outbound1 = JsonCodec.roundTrip(ListAuditTrailExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailExportsRequest.Filter.newBuilder()
                        .setRegion(reply0.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build(), ListAuditTrailExportsRequest.class);
        var reply1 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailExports(outbound1);

        var outbound2 = PreviewInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setCard(InvoiceTemplateCardSettlement.newBuilder()
                                .setLast4(switch (request.getEInvoice().getSettlementCase()) {
                                    case CARD -> request.getEInvoice().getCard().getLast4();
                                    default -> "";
                                })
                                .setToken(switch (reply0.getInvoiceTemplate().getSettlementCase()) {
                                    case CARD -> reply0.getInvoiceTemplate().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewInvoiceTemplate(outbound2);

        var outbound3 = CheckInvoiceTemplateEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getInvoiceTemplate().getAudit().getCreatedBy())
                                .build())
                        .setBank(InvoiceTemplateBankSettlement.newBuilder()
                                .setAccountHolder(switch (reply0.getInvoiceTemplate().getSettlementCase()) {
                                    case BANK -> reply0.getInvoiceTemplate().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply2.getInvoiceTemplate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkInvoiceTemplateEligibility(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintEInvoices", request.getEInvoice().getEInvoiceId());
        return LintEInvoicesResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setDisplayName(reply3.getInvoiceTemplate().getCounterparty().getDisplayName())
                                .setNumber(request.getEInvoice().getNumber())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNumber(reply2.getInvoiceTemplate().getNumber())
                        .build())
                .setOutcome(LintEInvoicesResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an e invoice.
     */
    public GetEInvoiceRulesResponse getEInvoiceRules(GetEInvoiceRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetEInvoiceRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "GetEInvoiceRules");

        var outbound0 = LintInvoiceTemplatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintInvoiceTemplatesRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintInvoiceTemplates(value)).orElseThrow();
        return GetEInvoiceRulesResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getInvoiceTemplate().getBillingAddress().getRecipient())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .build();
    }
}
