package com.acme.finance.domains.expenses;

import com.acme.careplans.v1.CarePlan;
import com.acme.careplans.v1.CarePlanBankSettlement;
import com.acme.careplans.v1.CarePlanServiceGrpc;
import com.acme.careplans.v1.CreateCarePlanRequest;
import com.acme.careplans.v1.CreateCarePlanResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlDiscountAction;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.ResolveContractsRequest;
import com.acme.contracts.v1.ResolveContractsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.CheckMileageClaimEligibilityRequest;
import com.acme.expenses.v1.CheckMileageClaimEligibilityResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExplainReceiptCalculationRequest;
import com.acme.expenses.v1.ExplainReceiptCalculationResponse;
import com.acme.expenses.v1.GetMileageClaimRulesRequest;
import com.acme.expenses.v1.GetMileageClaimRulesResponse;
import com.acme.expenses.v1.LintExpenseReportsRequest;
import com.acme.expenses.v1.LintExpenseReportsResponse;
import com.acme.expenses.v1.LintMileageClaimsRequest;
import com.acme.expenses.v1.LintMileageClaimsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.NormalizeMileageClaimRequest;
import com.acme.expenses.v1.NormalizeMileageClaimResponse;
import com.acme.expenses.v1.PreviewExpenseReportRequest;
import com.acme.expenses.v1.PreviewExpenseReportResponse;
import com.acme.expenses.v1.PreviewMileageClaimRequest;
import com.acme.expenses.v1.PreviewMileageClaimResponse;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptCardSettlement;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ScoreExpenseReportRequest;
import com.acme.expenses.v1.ScoreExpenseReportResponse;
import com.acme.expenses.v1.ScoreMileageClaimRequest;
import com.acme.expenses.v1.ScoreMileageClaimResponse;
import com.acme.expenses.v1.ValidateExpenseReportRequest;
import com.acme.expenses.v1.ValidateExpenseReportResponse;
import com.acme.expenses.v1.ValidateMileageClaimRequest;
import com.acme.expenses.v1.ValidateMileageClaimResponse;
import com.acme.expenses.v1.ValidateReceiptRequest;
import com.acme.expenses.v1.ValidateReceiptResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.ScoreRfqRequest;
import com.acme.procurement.v1.ScoreRfqResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Checks mileage claims against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class MileageClaimValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(MileageClaimValidationApplication.class);

    private final CarePlanServiceGrpc.CarePlanServiceBlockingStub careplansCarePlanServiceBlocking;

    private final ChannelRegistry channels;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final ContractQueryServiceGrpc.ContractQueryServiceFutureStub contractsContractQueryServiceFuture;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking;

    private final ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking;

    private final ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceFutureStub expensesExpenseReportValidationServiceFuture;

    private final ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking;

    private final ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    public MileageClaimValidationApplication(
            CarePlanServiceGrpc.CarePlanServiceBlockingStub careplansCarePlanServiceBlocking,
            ChannelRegistry channels,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            ContractQueryServiceGrpc.ContractQueryServiceFutureStub contractsContractQueryServiceFuture,
            ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking,
            ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking,
            ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceFutureStub expensesExpenseReportValidationServiceFuture,
            ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking,
            ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.careplansCarePlanServiceBlocking = careplansCarePlanServiceBlocking;
        this.channels = channels;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.contractsContractQueryServiceFuture = contractsContractQueryServiceFuture;
        this.expensesExpenseReportCalculationServiceBlocking = expensesExpenseReportCalculationServiceBlocking;
        this.expensesExpenseReportValidationServiceBlocking = expensesExpenseReportValidationServiceBlocking;
        this.expensesExpenseReportValidationServiceFuture = expensesExpenseReportValidationServiceFuture;
        this.expensesReceiptCalculationServiceBlocking = expensesReceiptCalculationServiceBlocking;
        this.expensesReceiptValidationServiceBlocking = expensesReceiptValidationServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.procurementRfqValidationServiceBlocking = procurementRfqValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates a mileage claim.
     */
    public ValidateMileageClaimResponse validateMileageClaim(ValidateMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = PreviewExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewExpenseReportRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.ExpenseReportValidationService");
        var selected0 = ExpenseReportValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.previewExpenseReport(outbound0);

        var futures1 = request.getMileageClaim().getLinesList().stream()
                .map(element -> CheckReceiptEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .addLines(ReceiptLine.newBuilder()
                                .setTax(ReceiptLineTax.newBuilder()
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkReceiptEligibility(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? CheckReceiptEligibilityResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CloneNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getExpenseReport().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getMileageClaim().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<CloneNotificationRequest, CloneNotificationResponse>() {
            @Override
            public CloneNotificationResponse apply(CloneNotificationRequest input) {
                return notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneNotification(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateMileageClaim", request.getMileageClaim().getMileageClaimId());
        return ValidateMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getNotification().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(request.getMileageClaim().getCounterparty())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setOutcome(ValidateMileageClaimResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Previews a mileage claim.
     */
    public PreviewMileageClaimResponse previewMileageClaim(PreviewMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        List<EstimateControlResponse> results0 = request.getMileageClaim().getLinesList().stream()
                .map(element -> EstimateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setDescription(element.getDescription())
                        .setDiscount(ControlDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateControl(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? EstimateControlResponse.getDefaultInstance()
                : results0.getLast();

        var builder1 = ScoreRfqRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(ScoreRfqRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setLocality(request.getMileageClaim().getBillingAddress().getLocality())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = procurementRfqValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRfq(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewMileageClaim", request.getMileageClaim().getMileageClaimId());
        return PreviewMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .putAllMetadata(request.getMileageClaim().getMetadataMap())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Checks a mileage claim.
     */
    public CheckMileageClaimEligibilityResponse checkMileageClaimEligibility(CheckMileageClaimEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckMileageClaimEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var builder0 = PreviewExpenseReportRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setExpenseReport(ExpenseReport.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getMileageClaim().getAudit().getCreatedAt())
                        .setEtag(request.getMileageClaim().getAudit().getEtag())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewExpenseReport(outbound0);

        var outbound1 = ResolveContractsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = contractsContractQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).resolveContracts(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckMileageClaimEligibility", request.getMileageClaim().getMileageClaimId());
        return CheckMileageClaimEligibilityResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .putAllMetadata(reply0.getExpenseReport().getMetadataMap())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setTotal(request.getMileageClaim().getTotal())
                        .build())
                .setOutcome(CheckMileageClaimEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Normalizes a mileage claim.
     */
    public NormalizeMileageClaimResponse normalizeMileageClaim(NormalizeMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        List<ScoreExpenseReportResponse> results0 = request.getMileageClaim().getLinesList().stream()
                .map(element -> ScoreExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreExpenseReport(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ScoreExpenseReportResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = QuoteExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getExpenseReport().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            QuoteExpenseReportResponse fetch(QuoteExpenseReportRequest input) {
                return expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteExpenseReport(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = ExplainReceiptCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setCard(ReceiptCardSettlement.newBuilder()
                                .setLast4(reply0.getExpenseReport().getCard().getLast4())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainReceiptCalculation(value));
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeMileageClaim", request.getMileageClaim().getMileageClaimId());
        return NormalizeMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBillingAddress(reply1.getExpenseReport().getBillingAddress())
                        .setIssuedAt(request.getMileageClaim().getIssuedAt())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setTotal(reply2.getReceipt().getTotal())
                        .build())
                .build();
    }

    /**
     * Scores a mileage claim.
     */
    public ScoreMileageClaimResponse scoreMileageClaim(ScoreMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        List<ScoreExpenseReportResponse> results0 = request.getMileageClaim().getLinesList().stream()
                .map(element -> ScoreExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreExpenseReport(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ScoreExpenseReportResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ValidateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getExpenseReport().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ValidateReceiptResponse fetch(ValidateReceiptRequest input) {
                return expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateReceipt(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = NormalizeLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getMileageClaim().getAudit().getEtag())
                                .build())
                        .setCard(LedgerAccountCardFunding.newBuilder()
                                .setToken(switch (request.getMileageClaim().getSettlementCase()) {
                                    case CARD -> request.getMileageClaim().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLedgerAccount(outbound2);

        var outbound3 = ValidateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateExpenseReportRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply2.getLedgerAccount().getContact().getName())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getReceipt().getAudit().getEtag())
                                .setUpdatedAt(request.getMileageClaim().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getReceipt().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var pending3 = expensesExpenseReportValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).validateExpenseReport(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreMileageClaim", request.getMileageClaim().getMileageClaimId());
        return ScoreMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getReceipt().getBillingAddress().getCountryCode())
                                .setLocality(reply3.getExpenseReport().getBillingAddress().getLocality())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setStatus(request.getMileageClaim().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Lints a mileage claim.
     */
    public LintMileageClaimsResponse lintMileageClaims(LintMileageClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintMileageClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = QuoteExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getMileageClaim().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteExpenseReport(value));

        var outbound1 = LintExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBank(ExpenseReportBankSettlement.newBuilder()
                                .setBic(switch (request.getMileageClaim().getSettlementCase()) {
                                    case BANK -> request.getMileageClaim().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintExpenseReports(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintMileageClaims", request.getMileageClaim().getMileageClaimId());
        return LintMileageClaimsResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setMileageClaimId(request.getMileageClaimId())
                        .setStatus(request.getMileageClaim().getStatus())
                        .build())
                .setOutcome(LintMileageClaimsResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a mileage claim.
     */
    public GetMileageClaimRulesResponse getMileageClaimRules(GetMileageClaimRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetMileageClaimRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "GetMileageClaimRules");

        var outbound0 = RpcSupport.unpack(Any.pack(ScoreExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()), ScoreExpenseReportRequest.class);
        var reply0 = expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreExpenseReport(outbound0);

        var outbound1 = CreateCarePlanRequest.newBuilder()
                .setCarePlan(CarePlan.newBuilder()
                        .setBank(CarePlanBankSettlement.newBuilder()
                                .setIban(switch (reply0.getExpenseReport().getSettlementCase()) {
                                    case BANK -> reply0.getExpenseReport().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getExpenseReport().getBillingAddress().getCountryCode())
                                .addAllLines(reply0.getExpenseReport().getBillingAddress().getLinesList())
                                .setPostalCode(reply0.getExpenseReport().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = careplansCarePlanServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createCarePlan(outbound1);
        return GetMileageClaimRulesResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getExpenseReport().getBillingAddress().getRegion())
                                .build())
                        .putAllMetadata(reply1.getCarePlan().getMetadataMap())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
