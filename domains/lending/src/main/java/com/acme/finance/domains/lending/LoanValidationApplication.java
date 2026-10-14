package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.CountBankAccountsRequest;
import com.acme.banking.v1.CountBankAccountsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.collections.v1.FindPaymentPromisesRequest;
import com.acme.collections.v1.FindPaymentPromisesResponse;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ExplainUserDecisionRequest;
import com.acme.identity.v1.ExplainUserDecisionResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumBankSettlement;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.CheckLoanEligibilityRequest;
import com.acme.lending.v1.CheckLoanEligibilityResponse;
import com.acme.lending.v1.CompareLoanApplicationOptionsRequest;
import com.acme.lending.v1.CompareLoanApplicationOptionsResponse;
import com.acme.lending.v1.CompareLoanOptionsRequest;
import com.acme.lending.v1.CompareLoanOptionsResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.ExplainLoanDecisionRequest;
import com.acme.lending.v1.ExplainLoanDecisionResponse;
import com.acme.lending.v1.GetLoanRatesRequest;
import com.acme.lending.v1.GetLoanRatesResponse;
import com.acme.lending.v1.GetLoanRulesRequest;
import com.acme.lending.v1.GetLoanRulesResponse;
import com.acme.lending.v1.LintLoansRequest;
import com.acme.lending.v1.LintLoansResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.LoanLimit;
import com.acme.lending.v1.NormalizeLoanRequest;
import com.acme.lending.v1.NormalizeLoanResponse;
import com.acme.lending.v1.PreviewLoanRequest;
import com.acme.lending.v1.PreviewLoanResponse;
import com.acme.lending.v1.QuoteLoanRequest;
import com.acme.lending.v1.QuoteLoanResponse;
import com.acme.lending.v1.ScoreLoanRequest;
import com.acme.lending.v1.ScoreLoanResponse;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.lending.v1.ValidateLoanRequest;
import com.acme.lending.v1.ValidateLoanResponse;
import com.acme.shows.v1.CompareShowOptionsRequest;
import com.acme.shows.v1.CompareShowOptionsResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowCalculationServiceGrpc;
import com.acme.workplaces.v1.Building;
import com.acme.workplaces.v1.BuildingCalculationServiceGrpc;
import com.acme.workplaces.v1.RecalculateBuildingsRequest;
import com.acme.workplaces.v1.RecalculateBuildingsResponse;
import java.util.ArrayList;
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
import org.springframework.stereotype.Service;

/**
 * Checks loans against the rules before anything acts on them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class LoanValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanValidationApplication.class);

    private final AuditTrailAdminServiceGrpc.AuditTrailAdminServiceBlockingStub auditAuditTrailAdminServiceBlocking;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    private final ShowCalculationServiceGrpc.ShowCalculationServiceBlockingStub showsShowCalculationServiceBlocking;

    private final BuildingCalculationServiceGrpc.BuildingCalculationServiceBlockingStub workplacesBuildingCalculationServiceBlocking;

    public LoanValidationApplication(
            AuditTrailAdminServiceGrpc.AuditTrailAdminServiceBlockingStub auditAuditTrailAdminServiceBlocking,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            ShowCalculationServiceGrpc.ShowCalculationServiceBlockingStub showsShowCalculationServiceBlocking,
            BuildingCalculationServiceGrpc.BuildingCalculationServiceBlockingStub workplacesBuildingCalculationServiceBlocking) {
        this.auditAuditTrailAdminServiceBlocking = auditAuditTrailAdminServiceBlocking;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.collectionsPaymentPromiseQueryServiceBlocking = collectionsPaymentPromiseQueryServiceBlocking;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.showsShowCalculationServiceBlocking = showsShowCalculationServiceBlocking;
        this.workplacesBuildingCalculationServiceBlocking = workplacesBuildingCalculationServiceBlocking;
    }

    /**
     * Validates a loan.
     */
    public ValidateLoanResponse validateLoan(ValidateLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = FindPaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPaymentPromisesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLoan().getAudit().getCreatedAt())
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .setOwnerId(request.getLoan().getLoanId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> collectionsPaymentPromiseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPaymentPromises(outbound0), rpcExecutor);

        var outbound1 = GetLoanRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanId(request.getLoanId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getLoanRates(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = StreamLedgerAccountNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setName(request.getLoan().getContact().getName())
                        .build())
                .build();
        var stream2 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamLedgerAccountNotices(outbound2);
        var reply2 = StreamLedgerAccountNoticesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        List<ExplainLoanCalculationResponse> results3 = request.getLoan().getLimitsList().stream()
                .map(element -> ExplainLoanCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .addLimits(LoanLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanCalculation(value)))
                .collect(Collectors.toList());
        var reply3 = results3.isEmpty() ? ExplainLoanCalculationResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getLoan().getHolder().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateLoan", request.getLoan().getHolder().getLoanApplicationId());
        return ValidateLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setBalance(request.getLoan().getBalance())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getLoan().getContact().getPreferredChannel())
                                .build())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Previews a loan.
     */
    public PreviewLoanResponse previewLoan(PreviewLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = QuoteLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setNumber(request.getLoan().getHolder().getNumber())
                                .build())
                        .build())
                .build();
        RpcPort<QuoteLoanRequest, QuoteLoanResponse> port0 = RpcPort.adapt(lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::quoteLoan);
        var reply0 = port0.exchange(outbound0);

        var futures1 = request.getLoan().getLimitsList().stream()
                .map(element -> CheckLoanApplicationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLoan().getAudit().getCreatedBy())
                                .build())
                        .addLines(LoanApplicationLine.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkLoanApplicationEligibility(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? CheckLoanApplicationEligibilityResponse.getDefaultInstance()
                : results1.getLast();

        var results2 = new ArrayList<CompareLoanApplicationOptionsResponse>();
        for (var element : request.getLoan().getLimitsList()) {
            CompareLoanApplicationOptionsRequest itemRequest = CompareLoanApplicationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareLoanApplicationOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(reply1.getLoanApplication().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getLoanApplication().getBillingAddress().getRegion())
                                .build())
                        .addLines(LoanApplicationLine.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build();
            results2.add(lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareLoanApplicationOptions(itemRequest));
        }
        var reply2 = results2.isEmpty() ? CompareLoanApplicationOptionsResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = RepairAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getLoan().getAudit().getUpdatedAt())
                                .build())
                        .setBank(AuditTrailBankSettlement.newBuilder()
                                .setIban(switch (request.getLoan().getFundingCase()) {
                                    case BANK -> request.getLoan().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RepairAuditTrailRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply3 = auditAuditTrailAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).repairAuditTrail(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewLoan", request.getLoan().getLoanId());
        return PreviewLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getAuditTrail().getAudit().getCreatedAt())
                                .build())
                        .setBalance(reply0.getLoan().getBalance())
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks a loan.
     */
    public CheckLoanEligibilityResponse checkLoanEligibility(CheckLoanEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckLoanEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = RecalculatePremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setBank(PremiumBankSettlement.newBuilder()
                                .setIban(request.getLoan().getBank().getIban())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePremiums(outbound0), rpcExecutor);

        var outbound1 = SimulateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setBank(LoanBankFunding.newBuilder()
                                .setIban(request.getLoan().getBank().getIban())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateLoan(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ExplainUserDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainUserDecision(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckLoanEligibility", request.getLoan().getLoanId());
        return CheckLoanEligibilityResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getUser().getAudit().getUpdatedAt())
                                .build())
                        .setAvailable(request.getLoan().getAvailable())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply2.getUser().getContact().getPhone())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(reply0.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .setLoanId(request.getLoanId())
                        .build())
                .setOutcome(CheckLoanEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes a loan.
     */
    public NormalizeLoanResponse normalizeLoan(NormalizeLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = ListAuditTrailExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailExportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLoan().getAudit().getUpdatedAt())
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .setOwnerId(switch (request.getLoan().getFundingCase()) {
                            case EXTERNAL_ACCOUNT_ID -> request.getLoan().getExternalAccountId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply0 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailExports(outbound0);

        var outbound1 = QuoteLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .setBank(LoanBankFunding.newBuilder()
                                .setIban(request.getLoan().getBank().getIban())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(request.getLoan().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            QuoteLoanResponse fetch(QuoteLoanRequest input) {
                return lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteLoan(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeLoan", request.getLoan().getLoanId());
        return NormalizeLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setLoanApplicationId(reply1.getLoan().getHolder().getLoanApplicationId())
                                .build())
                        .putAllLabels(reply1.getLoan().getLabelsMap())
                        .setLoanId(request.getLoanId())
                        .setStatus(request.getLoan().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a loan.
     */
    public ExplainLoanDecisionResponse explainLoanDecision(ExplainLoanDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainLoanDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = QuoteLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLoan().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .setBank(LoanBankFunding.newBuilder()
                                .setAccountHolder(request.getLoan().getBank().getAccountHolder())
                                .build())
                        .build())
                .build();
        Function<QuoteLoanRequest, QuoteLoanResponse> operation0 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::quoteLoan;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = CalculateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setCard(LoanCardFunding.newBuilder()
                                .setHolderName(switch (reply0.getLoan().getFundingCase()) {
                                    case CARD -> reply0.getLoan().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateLoan(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainLoanDecision", request.getLoan().getLoanId());
        return ExplainLoanDecisionResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                .setEtag(reply0.getLoan().getAudit().getEtag())
                                .build())
                        .setBalance(reply1.getLoan().getBalance())
                        .setCurrencyCode(request.getLoan().getCurrencyCode())
                        .setLoanId(request.getLoanId())
                        .build())
                .build();
    }

    /**
     * Scores a loan.
     */
    public ScoreLoanResponse scoreLoan(ScoreLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var results0 = new ArrayList<RecalculateBuildingsResponse>();
        for (var element : request.getLoan().getLimitsList()) {
            RecalculateBuildingsRequest itemRequest = RecalculateBuildingsRequest.newBuilder()
                .setBuilding(Building.newBuilder()
                        .addChildren(Building.newBuilder()
                                .addChildren(Building.newBuilder()
                                        .addChildren(Building.newBuilder()
                                                .setName(element.getName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(workplacesBuildingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateBuildings(itemRequest));
        }
        var reply0 = results0.isEmpty() ? RecalculateBuildingsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = GetLoanRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanId(request.getLoan().getLoanId())
                .build();
        var reply1 = RpcSupport.retryRead(() -> lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getLoanRates(outbound1));

        var outbound2 = CountBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBankAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLoan().getAudit().getCreatedAt())
                        .putAllLabels(reply1.getLoan().getLabelsMap())
                        .setRegion(reply0.getBuilding().getLocation().getRegion())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.banking.v1.BankAccountQueryService/CountBankAccounts", outbound2);
        var reply2 = bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countBankAccounts(command2.payload());

        var outbound3 = JsonCodec.roundTrip(CompareLoanOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(reply1.getLoan().getHolder().getDisplayName())
                                .build())
                        .build())
                .build(), CompareLoanOptionsRequest.class);
        var reply3 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareLoanOptions(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getHolder().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreLoan", request.getLoan().getHolder().getLoanApplicationId());
        return ScoreLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getLoan().getAudit().getCreatedAt())
                                .build())
                        .setCurrencyCode(request.getLoan().getCurrencyCode())
                        .putAllLabels(reply3.getLoan().getLabelsMap())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lints a loan.
     */
    public LintLoansResponse lintLoans(LintLoansRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintLoans")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = GetLoanRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanId(request.getLoanId())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getLoanRates(value)).orElseThrow();

        var outbound1 = RecalculateMandatesRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getLoan().getAudit().getCreatedBy())
                                .setEtag(request.getLoan().getAudit().getEtag())
                                .build())
                        .setCard(MandateCardSettlement.newBuilder()
                                .setHolderName(switch (reply0.getLoan().getFundingCase()) {
                                    case CARD -> reply0.getLoan().getCard().getHolderName();
                                    default -> "";
                                })
                                .setToken(switch (request.getLoan().getFundingCase()) {
                                    case CARD -> request.getLoan().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateMandates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintLoans", request.getLoan().getLoanId());
        return LintLoansResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setLoanId(request.getLoanId())
                        .setOpenedAt(request.getLoan().getOpenedAt())
                        .build())
                .setOutcome(LintLoansResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a loan.
     */
    public GetLoanRulesResponse getLoanRules(GetLoanRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLoanRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "GetLoanRules");

        var outbound0 = CompareShowOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setShow(Show.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = showsShowCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareShowOptions(outbound0);
        return GetLoanRulesResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getShow().getAudit().getUpdatedAt())
                                .build())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
