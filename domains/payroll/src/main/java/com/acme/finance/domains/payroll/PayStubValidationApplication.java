package com.acme.finance.domains.payroll;

import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyBlockAction;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.payroll.v1.CheckPayStubEligibilityRequest;
import com.acme.payroll.v1.CheckPayStubEligibilityResponse;
import com.acme.payroll.v1.ComparePayRunOptionsRequest;
import com.acme.payroll.v1.ComparePayRunOptionsResponse;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.ExplainPayRunCalculationRequest;
import com.acme.payroll.v1.ExplainPayRunCalculationResponse;
import com.acme.payroll.v1.ExplainPayStubDecisionRequest;
import com.acme.payroll.v1.ExplainPayStubDecisionResponse;
import com.acme.payroll.v1.GetDeductionRatesRequest;
import com.acme.payroll.v1.GetDeductionRatesResponse;
import com.acme.payroll.v1.GetPayStubRulesRequest;
import com.acme.payroll.v1.GetPayStubRulesResponse;
import com.acme.payroll.v1.LintPayStubsRequest;
import com.acme.payroll.v1.LintPayStubsResponse;
import com.acme.payroll.v1.NormalizePayStubRequest;
import com.acme.payroll.v1.NormalizePayStubResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunAttachment;
import com.acme.payroll.v1.PayRunBankSettlement;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunCardSettlement;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubCardSettlement;
import com.acme.payroll.v1.PreviewPayStubRequest;
import com.acme.payroll.v1.PreviewPayStubResponse;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.QuotePayStubRequest;
import com.acme.payroll.v1.QuotePayStubResponse;
import com.acme.payroll.v1.ScorePayStubRequest;
import com.acme.payroll.v1.ScorePayStubResponse;
import com.acme.payroll.v1.SimulatePayRunRequest;
import com.acme.payroll.v1.SimulatePayRunResponse;
import com.acme.payroll.v1.ValidatePayStubRequest;
import com.acme.payroll.v1.ValidatePayStubResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
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
 * Checks pay stubs against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class PayStubValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(PayStubValidationApplication.class);

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final DeductionCalculationServiceGrpc.DeductionCalculationServiceFutureStub payrollDeductionCalculationServiceFuture;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final PayrollRepository repository;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final Executor rpcExecutor;

    public PayStubValidationApplication(
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            DeductionCalculationServiceGrpc.DeductionCalculationServiceFutureStub payrollDeductionCalculationServiceFuture,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            PayrollRepository repository,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.budgetingBudgetServiceAsync = budgetingBudgetServiceAsync;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.payrollDeductionCalculationServiceFuture = payrollDeductionCalculationServiceFuture;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates a pay stub.
     */
    public ValidatePayStubResponse validatePayStub(ValidatePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidatePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = SimulatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulatePayRunRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getPayStub().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPayStub().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getPayStub().getAudit().getUpdatedAt())
                                .build())
                        .setCard(PayRunCardSettlement.newBuilder()
                                .setHolderName(switch (request.getPayStub().getSettlementCase()) {
                                    case CARD -> request.getPayStub().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePayRun(outbound0);

        List<ExplainPayRunCalculationResponse> results1 = request.getPayStub().getAttachmentsList().stream()
                .map(element -> ExplainPayRunCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .addAttachments(PayRunAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setFileName(element.getFileName())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPayRunCalculation(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? ExplainPayRunCalculationResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BatchCreateBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBudgetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateBudgetsResponse.getDefaultInstance());
        var input2 = budgetingBudgetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBudgets(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPayStub().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPayStub().getAudit().getCreatedBy())
                                .setEtag(request.getPayStub().getAudit().getEtag())
                                .setUpdatedAt(request.getPayStub().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidatePayStub", request.getPayStub().getPayStubId());
        return ValidatePayStubResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(request.getPayStub().getBillingAddress())
                        .setIssuedAt(reply0.getPayRun().getIssuedAt())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Previews a pay stub.
     */
    public PreviewPayStubResponse previewPayStub(PreviewPayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewPayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = ValidateRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getPayStub().getAudit().getUpdatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(request.getPayStub().getBillingAddress().getCountryCode())
                                                .setPostalCode(request.getPayStub().getBillingAddress().getPostalCode())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.risk.v1.RiskRegisterValidationService/ValidateRiskRegister", outbound0);
        var reply0 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRiskRegister(command0.payload());

        var outbound1 = ComparePayRunOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ComparePayRunOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getPayStub().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getPayStub().getBillingAddress().getLocality())
                                .build())
                        .setCard(PayRunCardSettlement.newBuilder()
                                .setHolderName(switch (request.getPayStub().getSettlementCase()) {
                                    case CARD -> request.getPayStub().getCard().getHolderName();
                                    default -> "";
                                })
                                .setToken(switch (request.getPayStub().getSettlementCase()) {
                                    case CARD -> request.getPayStub().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePayRunOptions(outbound1);

        var outbound2 = StreamLedgerAccountNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getContact().getEmail())
                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream2 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamLedgerAccountNotices(outbound2);
        var reply2 = StreamLedgerAccountNoticesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewPayStub", request.getPayStub().getPayStubId());
        return PreviewPayStubResponse.newBuilder()
                .setOutcome(PreviewPayStubResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setNumber(request.getPayStub().getNumber())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Checks a pay stub.
     */
    public CheckPayStubEligibilityResponse checkPayStubEligibility(CheckPayStubEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckPayStubEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = QuotePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePayRunRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getPayStub().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBank(PayRunBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getPayStub().getSettlementCase()) {
                                    case BANK -> request.getPayStub().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePayRun(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckPayStubEligibility", request.getPayStub().getPayStubId());
        return CheckPayStubEligibilityResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setNotes(request.getPayStub().getNotes())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes a pay stub.
     */
    public NormalizePayStubResponse normalizePayStub(NormalizePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = QuotePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPayStub().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPayStub().getBillingAddress().getLinesList())
                                .build())
                        .setCard(PayStubCardSettlement.newBuilder()
                                .setNetwork(request.getPayStub().getCard().getNetwork())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePayStub(outbound0), rpcExecutor);

        var outbound1 = SendLedgerAccountTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendLedgerAccountTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendLedgerAccountTestNotice(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizePayStub", request.getPayStub().getPayStubId());
        return NormalizePayStubResponse.newBuilder()
                .setOutcome(NormalizePayStubResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getPayStub().getBillingAddress().getLocality())
                                .setRecipient(reply0.getPayStub().getBillingAddress().getRecipient())
                                .build())
                        .setNotes(request.getPayStub().getNotes())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Explains a pay stub.
     */
    public ExplainPayStubDecisionResponse explainPayStubDecision(ExplainPayStubDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPayStubDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = SimulatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulatePayRunRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getPayStub().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPayStub().getAudit().getCreatedAt())
                                .build())
                        .setCard(PayRunCardSettlement.newBuilder()
                                .setLast4(switch (request.getPayStub().getSettlementCase()) {
                                    case CARD -> request.getPayStub().getCard().getLast4();
                                    default -> "";
                                })
                                .setNetwork(switch (request.getPayStub().getSettlementCase()) {
                                    case CARD -> request.getPayStub().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePayRun(outbound0);

        var results1 = new ArrayList<CheckFixedAssetEligibilityResponse>();
        for (var element : request.getPayStub().getLinesList()) {
            CheckFixedAssetEligibilityRequest itemRequest = CheckFixedAssetEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .build())
                .build();
            results1.add(assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkFixedAssetEligibility(itemRequest));
        }
        var reply1 = results1.isEmpty() ? CheckFixedAssetEligibilityResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPayStubDecision", request.getPayStub().getPayStubId());
        return ExplainPayStubDecisionResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getPayRun().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDeductionId(reply0.getPayRun().getCounterparty().getDeductionId())
                                .build())
                        .setNotes(request.getPayStub().getNotes())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Scores a pay stub.
     */
    public ScorePayStubResponse scorePayStub(ScorePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScorePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = JsonCodec.roundTrip(ResolveNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .build(), ResolveNotificationsRequest.class);
        var reply0 = notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveNotifications(outbound0);

        List<ComparePayRunOptionsResponse> results1 = request.getPayStub().getLinesList().stream()
                .map(element -> ComparePayRunOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .addLines(PayRunLine.newBuilder()
                                .setQuantity(element.getQuantity())
                                .setTax(PayRunLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePayRunOptions(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? ComparePayRunOptionsResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScorePayStub", request.getPayStub().getPayStubId());
        return ScorePayStubResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setCounterparty(request.getPayStub().getCounterparty())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lints a pay stub.
     */
    public LintPayStubsResponse lintPayStubs(LintPayStubsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintPayStubs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = GetDeductionRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDeductionId(request.getPayStub().getCounterparty().getDeductionId())
                .build();
        var pending0 = payrollDeductionCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).getDeductionRates(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintPayStubs", request.getPayStub().getPayStubId());
        return LintPayStubsResponse.newBuilder()
                .setOutcome(LintPayStubsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .putAllMetadata(request.getPayStub().getMetadataMap())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a pay stub.
     */
    public GetPayStubRulesResponse getPayStubRules(GetPayStubRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPayStubRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "GetPayStubRules");

        var outbound0 = CloneAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(AccessPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<CloneAccessPolicyRequest, CloneAccessPolicyResponse> operation0 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::cloneAccessPolicy;
        var reply0 = operation0.apply(outbound0);
        return GetPayStubRulesResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getAccessPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
