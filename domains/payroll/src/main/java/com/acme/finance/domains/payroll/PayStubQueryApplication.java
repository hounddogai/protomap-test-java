package com.acme.finance.domains.payroll;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.CalculateAssetTransferRequest;
import com.acme.assets.v1.CalculateAssetTransferResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.banking.v1.SummarizeBankAccountsRequest;
import com.acme.banking.v1.SummarizeBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionRequest;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.payroll.v1.BrowsePayStubsRequest;
import com.acme.payroll.v1.BrowsePayStubsResponse;
import com.acme.payroll.v1.CheckPayRunEligibilityRequest;
import com.acme.payroll.v1.CheckPayRunEligibilityResponse;
import com.acme.payroll.v1.CountPayStubsRequest;
import com.acme.payroll.v1.CountPayStubsResponse;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExportPayStubsRequest;
import com.acme.payroll.v1.ExportPayStubsResponse;
import com.acme.payroll.v1.FindPayStubsRequest;
import com.acme.payroll.v1.FindPayStubsResponse;
import com.acme.payroll.v1.LookupPayStubRequest;
import com.acme.payroll.v1.LookupPayStubResponse;
import com.acme.payroll.v1.NormalizeTaxWithholdingRequest;
import com.acme.payroll.v1.NormalizeTaxWithholdingResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PeekPayStubRequest;
import com.acme.payroll.v1.PeekPayStubResponse;
import com.acme.payroll.v1.ResolvePayStubsRequest;
import com.acme.payroll.v1.ResolvePayStubsResponse;
import com.acme.payroll.v1.SummarizePayStubsRequest;
import com.acme.payroll.v1.SummarizePayStubsResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingBlockAction;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.ValidatePayRunRequest;
import com.acme.payroll.v1.ValidatePayRunResponse;
import com.acme.payroll.v1.ValidatePayStubRequest;
import com.acme.payroll.v1.ValidatePayStubResponse;
import com.acme.payroll.v1.ValidateTaxWithholdingRequest;
import com.acme.payroll.v1.ValidateTaxWithholdingResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about pay stubs, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PayStubQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(PayStubQueryApplication.class);

    private final AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync;

    private final ChannelRegistry channels;

    private final ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking;

    private final PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking;

    private final TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking;

    private final TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking;

    private final PayrollRepository repository;

    public PayStubQueryApplication(
            AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync,
            ChannelRegistry channels,
            ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking,
            PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking,
            TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking,
            TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking,
            PayrollRepository repository) {
        this.assetsAssetTransferCalculationServiceBlocking = assetsAssetTransferCalculationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.bankingBankAccountServiceAsync = bankingBankAccountServiceAsync;
        this.channels = channels;
        this.expensesExpensePolicyValidationServiceBlocking = expensesExpensePolicyValidationServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.payrollPayRunValidationServiceBlocking = payrollPayRunValidationServiceBlocking;
        this.payrollPayStubValidationServiceBlocking = payrollPayStubValidationServiceBlocking;
        this.payrollTaxWithholdingCalculationServiceBlocking = payrollTaxWithholdingCalculationServiceBlocking;
        this.payrollTaxWithholdingValidationServiceBlocking = payrollTaxWithholdingValidationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Returns a pay stub.
     */
    public LookupPayStubResponse lookupPayStub(LookupPayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupPayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "LookupPayStub");

        var builder0 = EstimateTaxWithholdingRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setTaxWithholding(TaxWithholding.newBuilder()
                .setOwner(UserRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateTaxWithholding(outbound0);

        var outbound1 = BatchCreateBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBankAccountsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateBankAccountsResponse.getDefaultInstance());
        var input1 = bankingBankAccountServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBankAccounts(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        return LookupPayStubResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getTaxWithholding().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setVersion(reply0.getTaxWithholding().getVersion())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Lists pay stubs a page at a time.
     */
    public BrowsePayStubsResponse browsePayStubs(BrowsePayStubsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowsePayStubs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getContext().getTraceId(), "BrowsePayStubs");

        var outbound0 = ValidatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePayStubRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ValidatePayStubResponse fetch(ValidatePayStubRequest input) {
                return payrollPayStubValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePayStub(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowsePayStubs", request.getPage().getPageToken());
        return BrowsePayStubsResponse.newBuilder()
                .addPayStubs(PayStub.newBuilder()
                        .setNumber(reply0.getPayStub().getNumber())
                        .build())
                .build();
    }

    /**
     * Lists pay stubs a page at a time.
     */
    public FindPayStubsResponse findPayStubs(FindPayStubsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindPayStubs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getContext().getTraceId(), "FindPayStubs");

        var outbound0 = CheckPayRunEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckPayRunEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.PayRunValidationService");
        var serviceName0 = "acme.payroll.v1" + "." + "PayRunValidationService";
        var methodName0 = "Che" + "ckPayRunEligibility";
        var descriptor0 = MethodDescriptor.<CheckPayRunEligibilityRequest, CheckPayRunEligibilityResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CheckPayRunEligibilityRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CheckPayRunEligibilityResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindPayStubs", request.getPage().getPageToken());
        return FindPayStubsResponse.newBuilder()
                .addPayStubs(PayStub.newBuilder()
                        .setIssuedAt(reply0.getPayRun().getIssuedAt())
                        .build())
                .build();
    }

    /**
     * Counts pay stubs by status.
     */
    public CountPayStubsResponse countPayStubs(CountPayStubsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountPayStubs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getContext().getTraceId(), "CountPayStubs");

        var outbound0 = ValidatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePayRunRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setIssuedAt(request.getFilter().getCreatedAfter())
                        .build())
                .build();
        Function<ValidatePayRunRequest, ValidatePayRunResponse> operation0 = payrollPayRunValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validatePayRun;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountPayStubs", request.getPage().getPageToken());
        return CountPayStubsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several pay stubs at once.
     */
    public ResolvePayStubsResponse resolvePayStubs(ResolvePayStubsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolvePayStubs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getContext().getTraceId(), "ResolvePayStubs");

        var outbound0 = ShareFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareFiscalPeriodExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getContext().getBaggageMap())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareFiscalPeriodExport(value));

        var outbound1 = ValidateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getFiscalPeriod().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getFiscalPeriod().getOrganizer().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(payrollTaxWithholdingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateTaxWithholding(value)))
                .collect(Collectors.toList()).getFirst();
        return ResolvePayStubsResponse.newBuilder()
                .addPayStubs(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getTaxWithholding().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getFiscalPeriod().getLocation().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Streams every matching pay stub.
     */
    public ExportPayStubsResponse exportPayStubs(ExportPayStubsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportPayStubs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getContext().getTraceId(), "ExportPayStubs");

        var outbound0 = (ExplainExpensePolicyDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainExpensePolicyDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = expensesExpensePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainExpensePolicyDecision(outbound0);

        var outbound1 = QuoteAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getExpensePolicy().getAudit().getEtag())
                                .setUpdatedBy(reply0.getExpensePolicy().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteAssetTransfer(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportPayStubs", request.getFilter().getQuery());
        return ExportPayStubsResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getExpensePolicy().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .addLines(PayStubLine.newBuilder()
                                .setDescription(reply0.getExpensePolicy().getDescription())
                                .build())
                        .setTotal(reply1.getAssetTransfer().getTotal())
                        .build())
                .build();
    }

    /**
     * Summarizes a pay stub.
     */
    public SummarizePayStubsResponse summarizePayStubs(SummarizePayStubsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizePayStubs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "SummarizePayStubs");

        var builder0 = NormalizeTaxWithholdingRequest.newBuilder();
        builder0.putAllAnnotations(request.getAnnotationsMap());
        builder0.setContext(request.getContext());
        builder0.setDetails(NormalizeTaxWithholdingRequest.Details.newBuilder()
                .setLocale(request.getContext().getLocale())
                .build());
        builder0.setTaxWithholding(TaxWithholding.newBuilder()
                .setBlock(TaxWithholdingBlockAction.newBuilder()
                        .setReason(request.getReason())
                        .build())
                .setOwner(UserRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = payrollTaxWithholdingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeTaxWithholding(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizePayStubs", request.getPayStubId());
        return SummarizePayStubsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a pay stub.
     */
    public PeekPayStubResponse peekPayStub(PeekPayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekPayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "PeekPayStub");

        var outbound0 = SummarizeBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizeBankAccountsRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .build();
        Function<SummarizeBankAccountsRequest, SummarizeBankAccountsResponse> operation0 = bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::summarizeBankAccounts;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = CalculateAssetTransferRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setDueAt(request.getContext().getDeadline())
                        .addLines(AssetTransferLine.newBuilder()
                                .putAllAttributes(reply0.getResult().getDetailsMap())
                                .build())
                        .putAllMetadata(reply0.getSummaryMap())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            CalculateAssetTransferResponse fetch(CalculateAssetTransferRequest input) {
                return assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateAssetTransfer(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        return PeekPayStubResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setIssuedAt(reply1.getAssetTransfer().getIssuedAt())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
