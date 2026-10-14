package com.acme.finance.domains.payroll;

import com.acme.anomalies.v1.AnomalyQueryServiceGrpc;
import com.acme.anomalies.v1.BrowseAnomaliesRequest;
import com.acme.anomalies.v1.BrowseAnomaliesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.expenses.v1.CheckExpenseReportEligibilityRequest;
import com.acme.expenses.v1.CheckExpenseReportEligibilityResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.ScoreLedgerAccountRequest;
import com.acme.ledger.v1.ScoreLedgerAccountResponse;
import com.acme.payroll.v1.CheckPayRunEligibilityRequest;
import com.acme.payroll.v1.CheckPayRunEligibilityResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.ExplainPayRunDecisionRequest;
import com.acme.payroll.v1.ExplainPayRunDecisionResponse;
import com.acme.payroll.v1.GetPayRunRulesRequest;
import com.acme.payroll.v1.GetPayRunRulesResponse;
import com.acme.payroll.v1.LintPayRunsRequest;
import com.acme.payroll.v1.LintPayRunsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunCardSettlement;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubAttachment;
import com.acme.payroll.v1.PayStubBankSettlement;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.RecalculatePayStubsRequest;
import com.acme.payroll.v1.RecalculatePayStubsResponse;
import com.acme.payroll.v1.SimulatePayStubRequest;
import com.acme.payroll.v1.SimulatePayStubResponse;
import com.acme.payroll.v1.ValidatePayRunRequest;
import com.acme.payroll.v1.ValidatePayRunResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks pay runs against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class PayRunValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(PayRunValidationApplication.class);

    private final AnomalyQueryServiceGrpc.AnomalyQueryServiceBlockingStub anomaliesAnomalyQueryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync;

    private final ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final PayrollRepository repository;

    public PayRunValidationApplication(
            AnomalyQueryServiceGrpc.AnomalyQueryServiceBlockingStub anomaliesAnomalyQueryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync,
            ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            PayrollRepository repository) {
        this.anomaliesAnomalyQueryServiceBlocking = anomaliesAnomalyQueryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.contractsContractServiceAsync = contractsContractServiceAsync;
        this.expensesExpenseReportValidationServiceBlocking = expensesExpenseReportValidationServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Validates a pay run.
     */
    public ValidatePayRunResponse validatePayRun(ValidatePayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidatePayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = EstimatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPayRun().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        Function<EstimatePayStubRequest, EstimatePayStubResponse> operation0 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimatePayStub;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = CheckExpenseReportEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPayStub().getAudit().getUpdatedAt())
                                .build())
                        .setBank(ExpenseReportBankSettlement.newBuilder()
                                .setIban(switch (request.getPayRun().getSettlementCase()) {
                                    case BANK -> request.getPayRun().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getPayStub().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply1 = expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkExpenseReportEligibility(outbound1);

        var results2 = new ArrayList<SimulatePayStubResponse>();
        for (var element : request.getPayRun().getLinesList()) {
            SimulatePayStubRequest itemRequest = SimulatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .build())
                        .build())
                .build();
            results2.add(payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePayStub(itemRequest));
        }
        var reply2 = results2.isEmpty() ? SimulatePayStubResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = BatchCreateContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateContractsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateContractsResponse.getDefaultInstance());
        var input3 = contractsContractServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateContracts(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPayRun().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPayRun().getAudit().getCreatedBy())
                                .setEtag(request.getPayRun().getAudit().getEtag())
                                .setUpdatedAt(request.getPayRun().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidatePayRun", request.getPayRun().getPayRunId());
        return ValidatePayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPayStub().getAudit().getCreatedAt())
                                .setUpdatedAt(reply1.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .setDueAt(reply0.getPayStub().getDueAt())
                        .setIssuedAt(request.getPayRun().getIssuedAt())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews a pay run.
     */
    public PreviewPayRunResponse previewPayRun(PreviewPayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewPayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = ScoreLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPayRun().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreLedgerAccount(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewPayRun", request.getPayRun().getPayRunId());
        return PreviewPayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getLedgerAccount().getAudit().getCreatedBy())
                                .build())
                        .setNumber(request.getPayRun().getNumber())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }

    /**
     * Checks a pay run.
     */
    public CheckPayRunEligibilityResponse checkPayRunEligibility(CheckPayRunEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckPayRunEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        List<ComparePayStubOptionsResponse> results0 = request.getPayRun().getAttachmentsList().stream()
                .map(element -> ComparePayStubOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addAttachments(PayStubAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setFileName(element.getFileName())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePayStubOptions(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ComparePayStubOptionsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = EstimatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setBank(PayStubBankSettlement.newBuilder()
                                .setAccountHolder(reply0.getPayStub().getBank().getAccountHolder())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDeductionId(request.getPayRun().getCounterparty().getDeductionId())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePayStub(value));
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckPayRunEligibility", request.getPayRun().getPayRunId());
        return CheckPayRunEligibilityResponse.newBuilder()
                .setOutcome(CheckPayRunEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setCounterparty(DeductionRef.newBuilder()
                                .setVersion(reply1.getPayStub().getCounterparty().getVersion())
                                .build())
                        .setDueAt(request.getPayRun().getDueAt())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }

    /**
     * Explains a pay run.
     */
    public ExplainPayRunDecisionResponse explainPayRunDecision(ExplainPayRunDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPayRunDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = QuotePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setCard(PayRunCardSettlement.newBuilder()
                                .setToken(switch (request.getPayRun().getSettlementCase()) {
                                    case CARD -> request.getPayRun().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePayRun(outbound0);

        List<ComparePayStubOptionsResponse> results1 = request.getPayRun().getLinesList().stream()
                .map(element -> ComparePayStubOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePayStubOptions(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? ComparePayStubOptionsResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPayRunDecision", request.getPayRun().getPayRunId());
        return ExplainPayRunDecisionResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPayRun().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(request.getPayRun().getBillingAddress())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(reply0.getPayRun().getCounterparty().getDisplayName())
                                .build())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }

    /**
     * Lints a pay run.
     */
    public LintPayRunsResponse lintPayRuns(LintPayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintPayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        List<RecalculatePayStubsResponse> results0 = request.getPayRun().getLinesList().stream()
                .map(element -> RecalculatePayStubsRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setDescription(element.getDescription())
                                .setTax(PayStubLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePayStubs(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? RecalculatePayStubsResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintPayRuns", request.getPayRun().getPayRunId());
        return LintPayRunsResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .putAllMetadata(request.getPayRun().getMetadataMap())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }

    /**
     * Returns a pay run.
     */
    public GetPayRunRulesResponse getPayRunRules(GetPayRunRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPayRunRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getPayRunId(), "GetPayRunRules");

        var outbound0 = BrowseAnomaliesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAnomaliesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getPayRunId())
                        .build())
                .build();
        RpcPort<BrowseAnomaliesRequest, BrowseAnomaliesResponse> port0 = RpcPort.adapt(anomaliesAnomalyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::browseAnomalies);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = SimulatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .putAllAttributes(reply0.getResult().getDetailsMap())
                                .build())
                        .build())
                .build();
        var reply1 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePayStub(outbound1);
        return GetPayRunRulesResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(reply1.getPayStub().getBillingAddress())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
