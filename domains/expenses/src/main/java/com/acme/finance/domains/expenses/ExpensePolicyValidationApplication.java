package com.acme.finance.domains.expenses;

import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.SummarizeDepreciationsRequest;
import com.acme.assets.v1.SummarizeDepreciationsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.CheckRenewalEligibilityRequest;
import com.acme.contracts.v1.CheckRenewalEligibilityResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.documents.v1.CreateDocumentRequest;
import com.acme.documents.v1.CreateDocumentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentLine;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.FindDocumentsRequest;
import com.acme.documents.v1.FindDocumentsResponse;
import com.acme.expenses.v1.CheckExpensePolicyEligibilityRequest;
import com.acme.expenses.v1.CheckExpensePolicyEligibilityResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCardSettlement;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionRequest;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionResponse;
import com.acme.expenses.v1.GetExpensePolicyRulesRequest;
import com.acme.expenses.v1.GetExpensePolicyRulesResponse;
import com.acme.expenses.v1.LintExpensePoliciesRequest;
import com.acme.expenses.v1.LintExpensePoliciesResponse;
import com.acme.expenses.v1.LintExpenseReportsRequest;
import com.acme.expenses.v1.LintExpenseReportsResponse;
import com.acme.expenses.v1.LintMileageClaimsRequest;
import com.acme.expenses.v1.LintMileageClaimsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimLineTax;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.MileageClaimValidationServiceGrpc;
import com.acme.expenses.v1.NormalizeExpensePolicyRequest;
import com.acme.expenses.v1.NormalizeExpensePolicyResponse;
import com.acme.expenses.v1.NormalizeReceiptRequest;
import com.acme.expenses.v1.NormalizeReceiptResponse;
import com.acme.expenses.v1.PreviewExpensePolicyRequest;
import com.acme.expenses.v1.PreviewExpensePolicyResponse;
import com.acme.expenses.v1.PreviewExpenseReportRequest;
import com.acme.expenses.v1.PreviewExpenseReportResponse;
import com.acme.expenses.v1.RecalculateReceiptsRequest;
import com.acme.expenses.v1.RecalculateReceiptsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptBankSettlement;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ScoreExpensePolicyRequest;
import com.acme.expenses.v1.ScoreExpensePolicyResponse;
import com.acme.expenses.v1.ScoreMileageClaimRequest;
import com.acme.expenses.v1.ScoreMileageClaimResponse;
import com.acme.expenses.v1.SimulateReceiptRequest;
import com.acme.expenses.v1.SimulateReceiptResponse;
import com.acme.expenses.v1.ValidateExpensePolicyRequest;
import com.acme.expenses.v1.ValidateExpensePolicyResponse;
import com.acme.expenses.v1.ValidateExpenseReportRequest;
import com.acme.expenses.v1.ValidateExpenseReportResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.BatchGetUsersRequest;
import com.acme.identity.v1.BatchGetUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchCardSettlement;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.PreviewPostingBatchRequest;
import com.acme.ledger.v1.PreviewPostingBatchResponse;
import com.acme.ledger.v1.RevertPostingBatchRequest;
import com.acme.ledger.v1.RevertPostingBatchResponse;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks expense policies against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ExpensePolicyValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpensePolicyValidationApplication.class);

    private final DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceFutureStub documentsDocumentServiceFuture;

    private final ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking;

    private final MileageClaimValidationServiceGrpc.MileageClaimValidationServiceBlockingStub expensesMileageClaimValidationServiceBlocking;

    private final ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking;

    private final ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final ExpensesRepository repository;

    public ExpensePolicyValidationApplication(
            DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            DocumentServiceGrpc.DocumentServiceFutureStub documentsDocumentServiceFuture,
            ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking,
            MileageClaimValidationServiceGrpc.MileageClaimValidationServiceBlockingStub expensesMileageClaimValidationServiceBlocking,
            ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking,
            ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            ExpensesRepository repository) {
        this.assetsDepreciationQueryServiceBlocking = assetsDepreciationQueryServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.documentsDocumentServiceFuture = documentsDocumentServiceFuture;
        this.expensesExpenseReportValidationServiceBlocking = expensesExpenseReportValidationServiceBlocking;
        this.expensesMileageClaimValidationServiceBlocking = expensesMileageClaimValidationServiceBlocking;
        this.expensesReceiptCalculationServiceBlocking = expensesReceiptCalculationServiceBlocking;
        this.expensesReceiptValidationServiceBlocking = expensesReceiptValidationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.ledgerPostingBatchHistoryServiceBlocking = ledgerPostingBatchHistoryServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Validates an expense policy.
     */
    public ValidateExpensePolicyResponse validateExpensePolicy(ValidateExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = ValidateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateExpenseReportRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExpensePolicy().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateExpenseReport(value)).orElseThrow();

        var outbound1 = BatchGetUsersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.retryRead(() -> identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetUsers(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return ValidateExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setName(request.getExpensePolicy().getName())
                        .build())
                .setOutcome(ValidateExpensePolicyResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Previews an expense policy.
     */
    public PreviewExpensePolicyResponse previewExpensePolicy(PreviewExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = ScoreMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getExpensePolicy().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply0 = expensesMileageClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreMileageClaim(outbound0);

        var outbound1 = CompareRfqOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareRfqOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(reply0.getMileageClaim().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        Function<CompareRfqOptionsRequest, CompareRfqOptionsResponse> operation1 = procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::compareRfqOptions;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = RecalculateReceiptsRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getMileageClaim().getAudit().getCreatedAt())
                                .setEtag(reply1.getRfq().getAudit().getEtag())
                                .build())
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setDisplayName(request.getExpensePolicy().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateReceipts(outbound2);

        var outbound3 = BeanCopies.copy(RevertPostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertPostingBatchRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getMileageClaim().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), RevertPostingBatchRequest.class);
        var reply3 = ledgerPostingBatchHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertPostingBatch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return PreviewExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getRfq().getAudit().getCreatedAt())
                                .build())
                        .setDescription(request.getExpensePolicy().getDescription())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getReceipt().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setOutcome(PreviewExpensePolicyResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks an expense policy.
     */
    public CheckExpensePolicyEligibilityResponse checkExpensePolicyEligibility(CheckExpensePolicyEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckExpensePolicyEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = RpcSupport.unpack(Any.pack(CheckRenewalEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getContact().getName())
                                        .setPhone(request.getDetails().getContact().getPhone())
                                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build()), CheckRenewalEligibilityRequest.class);
        var reply0 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRenewalEligibility(outbound0);

        var outbound1 = LintMileageClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintMileageClaimsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getRenewal().getLocation().getCountryCode())
                                .build())
                        .addLines(MileageClaimLine.newBuilder()
                                .setTax(MileageClaimLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = expensesMileageClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintMileageClaims(outbound1);

        var outbound2 = PreviewPostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getMileageClaim().getBillingAddress().getCountryCode())
                                .build())
                        .setCard(PostingBatchCardSettlement.newBuilder()
                                .setLast4(switch (reply1.getMileageClaim().getSettlementCase()) {
                                    case CARD -> reply1.getMileageClaim().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPostingBatch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckExpensePolicyEligibility", request.getExpensePolicy().getExpensePolicyId());
        return CheckExpensePolicyEligibilityResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRenewal().getAudit().getUpdatedAt())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setPriority(request.getExpensePolicy().getPriority())
                        .build())
                .setOutcome(CheckExpensePolicyEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Normalizes an expense policy.
     */
    public NormalizeExpensePolicyResponse normalizeExpensePolicy(NormalizeExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = CheckReceiptEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .addLines(ReceiptLine.newBuilder()
                                .setTax(ReceiptLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkReceiptEligibility(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return NormalizeExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getReceipt().getAudit().getEtag())
                                .build())
                        .setEffective(request.getExpensePolicy().getEffective())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getReceipt().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Explains an expense policy.
     */
    public ExplainExpensePolicyDecisionResponse explainExpensePolicyDecision(ExplainExpensePolicyDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainExpensePolicyDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = CreateDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getExpensePolicy().getAudit().getEtag())
                                .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                                .build())
                        .addLines(DocumentLine.newBuilder()
                                .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                .setDescription(request.getExpensePolicy().getDescription())
                                .build())
                        .build())
                .build();
        var pending0 = documentsDocumentServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).createDocument(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainExpensePolicyDecision", request.getExpensePolicy().getExpensePolicyId());
        return ExplainExpensePolicyDecisionResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getDocument().getAudit().getCreatedBy())
                                .build())
                        .setEffective(request.getExpensePolicy().getEffective())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .build();
    }

    /**
     * Scores an expense policy.
     */
    public ScoreExpensePolicyResponse scoreExpensePolicy(ScoreExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = PreviewExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewExpenseReport(value));

        var outbound1 = LintExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getExpenseReport().getBillingAddress().getLocality())
                                .build())
                        .setCard(ExpenseReportCardSettlement.newBuilder()
                                .setHolderName(switch (reply0.getExpenseReport().getSettlementCase()) {
                                    case CARD -> reply0.getExpenseReport().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintExpenseReports(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return ScoreExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getExpenseReport().getAudit().getCreatedAt())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setStatus(request.getExpensePolicy().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lints an expense policy.
     */
    public LintExpensePoliciesResponse lintExpensePolicies(LintExpensePoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintExpensePolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = SimulateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpensePolicy().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateReceipt(value));

        var outbound1 = FindDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getReceipt().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getReceipt().getMetadataMap())
                        .setOwnerId(reply0.getReceipt().getCounterparty().getMileageClaimId())
                        .setRegion(reply0.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var adapter1 = new Function<FindDocumentsRequest, FindDocumentsResponse>() {
            @Override
            public FindDocumentsResponse apply(FindDocumentsRequest input) {
                return documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findDocuments(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = SummarizeDepreciationsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(switch (request.getExpensePolicy().getActionCase()) {
                    case BLOCK -> request.getExpensePolicy().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply2 = assetsDepreciationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeDepreciations(outbound2);

        var outbound3 = NormalizeReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setBank(ReceiptBankSettlement.newBuilder()
                                .setIban(switch (reply0.getReceipt().getSettlementCase()) {
                                    case BANK -> reply0.getReceipt().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setMileageClaimId(reply0.getReceipt().getCounterparty().getMileageClaimId())
                                .setNumber(reply0.getReceipt().getCounterparty().getNumber())
                                .build())
                        .addLines(ReceiptLine.newBuilder()
                                .setTax(ReceiptLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply3 = expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeReceipt(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintExpensePolicies", request.getExpensePolicy().getExpensePolicyId());
        return LintExpensePoliciesResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getReceipt().getAudit().getUpdatedBy())
                                .build())
                        .setEffective(request.getExpensePolicy().getEffective())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getReceipt().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setOutcome(LintExpensePoliciesResponse.Outcome.newBuilder()
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply3.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an expense policy.
     */
    public GetExpensePolicyRulesResponse getExpensePolicyRules(GetExpensePolicyRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetExpensePolicyRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "GetExpensePolicyRules");

        var outbound0 = LintMileageClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintMileageClaimsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = expensesMileageClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintMileageClaims(outbound0);
        return GetExpensePolicyRulesResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getMileageClaim().getAudit().getEtag())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .build();
    }
}
