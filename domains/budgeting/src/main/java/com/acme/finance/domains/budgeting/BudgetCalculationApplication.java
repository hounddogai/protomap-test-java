package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.CheckStandingOrderEligibilityRequest;
import com.acme.banking.v1.CheckStandingOrderEligibilityResponse;
import com.acme.banking.v1.FindBankAccountsRequest;
import com.acme.banking.v1.FindBankAccountsResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.CalculateBudgetRequest;
import com.acme.budgeting.v1.CalculateBudgetResponse;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.budgeting.v1.EstimateBudgetRequest;
import com.acme.budgeting.v1.EstimateBudgetResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.GetBudgetRatesRequest;
import com.acme.budgeting.v1.GetBudgetRatesResponse;
import com.acme.budgeting.v1.QuoteBudgetRequest;
import com.acme.budgeting.v1.QuoteBudgetResponse;
import com.acme.budgeting.v1.RecalculateBudgetsRequest;
import com.acme.budgeting.v1.RecalculateBudgetsResponse;
import com.acme.budgeting.v1.SimulateBudgetRequest;
import com.acme.budgeting.v1.SimulateBudgetResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.customers.v1.CustomerRef;
import com.acme.employees.v1.EmployeeServiceGrpc;
import com.acme.employees.v1.ListEmployeesRequest;
import com.acme.employees.v1.ListEmployeesResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ScoreUserRequest;
import com.acme.identity.v1.ScoreUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserNationalId;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExplainPolicyDecisionRequest;
import com.acme.insurance.v1.ExplainPolicyDecisionResponse;
import com.acme.insurance.v1.LintPoliciesRequest;
import com.acme.insurance.v1.LintPoliciesResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.ledger.v1.CreateLedgerAccountRequest;
import com.acme.ledger.v1.CreateLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountLimit;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureCardFunding;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.PreviewExposureRequest;
import com.acme.risk.v1.PreviewExposureResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ValidateExposureRequest;
import com.acme.risk.v1.ValidateExposureResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.venues.v1.FindVenuesRequest;
import com.acme.venues.v1.FindVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a budget, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BudgetCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetCalculationApplication.class);

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final EmployeeServiceGrpc.EmployeeServiceBlockingStub employeesEmployeeServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceFutureStub insurancePolicyValidationServiceFuture;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceFutureStub ledgerLedgerAccountServiceFuture;

    private final BudgetingRepository repository;

    private final ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    private final Executor rpcExecutor;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public BudgetCalculationApplication(
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            ChannelRegistry channels,
            EmployeeServiceGrpc.EmployeeServiceBlockingStub employeesEmployeeServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            PolicyValidationServiceGrpc.PolicyValidationServiceFutureStub insurancePolicyValidationServiceFuture,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceFutureStub ledgerLedgerAccountServiceFuture,
            BudgetingRepository repository,
            ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.channels = channels;
        this.employeesEmployeeServiceBlocking = employeesEmployeeServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.insurancePolicyValidationServiceFuture = insurancePolicyValidationServiceFuture;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.ledgerLedgerAccountServiceFuture = ledgerLedgerAccountServiceFuture;
        this.repository = repository;
        this.riskExposureValidationServiceBlocking = riskExposureValidationServiceBlocking;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    /**
     * Calculates a budget.
     */
    public CalculateBudgetResponse calculateBudget(CalculateBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = ScoreUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBudget().getAudit().getCreatedBy())
                                .build())
                        .setNationalId(UserNationalId.newBuilder()
                                .setNumber(request.getBudget().getHolder().getNumber())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.identity.v1.UserValidationService");
        var serviceName0 = "acme.identity.v1" + "." + "UserValidationService";
        var methodName0 = "Sco" + "reUser";
        var descriptor0 = MethodDescriptor.<ScoreUserRequest, ScoreUserResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ScoreUserRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScoreUserResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = LintRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getUser().getAudit().getUpdatedBy())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(reply0.getUser().getRelationshipManager().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getUser().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.risk.v1.RiskScenarioValidationService/LintRiskScenarios", outbound1);
        var reply1 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintRiskScenarios(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateBudget", request.getBudget().getBudgetId());
        return CalculateBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getUser().getAudit().getCreatedBy())
                                .build())
                        .setBalance(request.getBudget().getBalance())
                        .setBudgetId(request.getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getRiskScenario().getCustomer().getEmail())
                                .setName(reply0.getUser().getContact().getName())
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(reply1.getRiskScenario().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Estimates a budget.
     */
    public EstimateBudgetResponse estimateBudget(EstimateBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = ListLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getAudit().getCreatedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending0 = ledgerLedgerAccountServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccounts(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = PreviewExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setHolder(SupplierRef.newBuilder()
                                .setEmail(request.getBudget().getContact().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewExposureRequest, PreviewExposureResponse> port1 = RpcPort.adapt(riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewExposure);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateBudget", request.getBudget().getBudgetId());
        return EstimateBudgetResponse.newBuilder()
                .setBreakdown(EstimateBudgetResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply1.getExposure().getCurrencyCode())
                        .build())
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply1.getExposure().getContact().getPhone())
                                .build())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Quotes a budget.
     */
    public QuoteBudgetResponse quoteBudget(QuoteBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = LintPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setBank(PolicyBankFunding.newBuilder()
                                .setAccountHolder(request.getBudget().getBank().getAccountHolder())
                                .build())
                        .build())
                .build();
        var pending0 = insurancePolicyValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).lintPolicies(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = ValidateExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setCard(ExposureCardFunding.newBuilder()
                                .setLast4(switch (reply0.getPolicy().getFundingCase()) {
                                    case CARD -> reply0.getPolicy().getCard().getLast4();
                                    default -> "";
                                })
                                .setNetwork(switch (request.getBudget().getFundingCase()) {
                                    case CARD -> request.getBudget().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getBudget().getContact().getEmail())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(request.getBudget().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateExposure(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteBudget", request.getBudget().getBudgetId());
        return QuoteBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBalance(reply0.getPolicy().getBalance())
                        .setBudgetId(request.getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getExposure().getContact().getName())
                                .build())
                        .setCurrencyCode(reply0.getPolicy().getCurrencyCode())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .build();
    }

    /**
     * Compares a budget.
     */
    public CompareBudgetOptionsResponse compareBudgetOptions(CompareBudgetOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareBudgetOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = CreateLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setCard(LedgerAccountCardFunding.newBuilder()
                                .setHolderName(switch (request.getBudget().getFundingCase()) {
                                    case CARD -> request.getBudget().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getBudget().getContact().getPreferredChannel())
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setDisplayName(request.getBudget().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createLedgerAccount(outbound0);

        var outbound1 = JsonCodec.roundTrip(ExplainPolicyDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPolicyDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getBudget().getContact().getPhone())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setCard(PolicyCardFunding.newBuilder()
                                .setNetwork(request.getBudget().getCard().getNetwork())
                                .build())
                        .build())
                .build(), ExplainPolicyDecisionRequest.class);
        var reply1 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPolicyDecision(outbound1);

        var futures2 = request.getBudget().getLimitsList().stream()
                .map(element -> RecordLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .addLimits(LedgerAccountLimit.newBuilder()
                                .setName(element.getName())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordLedgerAccountVersion(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? RecordLedgerAccountVersionResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareBudgetOptions", request.getBudget().getBudgetId());
        return CompareBudgetOptionsResponse.newBuilder()
                .setBreakdown(CompareBudgetOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setBudget(Budget.newBuilder()
                        .setBalance(reply0.getLedgerAccount().getBalance())
                        .setBudgetId(request.getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getLedgerAccount().getContact().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalculates a budget.
     */
    public RecalculateBudgetsResponse recalculateBudgets(RecalculateBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = ExplainRiskScenarioDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getBudget().getContact().getPhone())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainRiskScenarioDecisionRequest, ExplainRiskScenarioDecisionResponse> port0 = RpcPort.adapt(riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainRiskScenarioDecision);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = FindVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getAudit().getCreatedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .setOwnerId(reply0.getRiskScenario().getAssignee().getUserId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        Function<FindVenuesRequest, FindVenuesResponse> operation1 = venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::findVenues;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = CheckStandingOrderEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getBudget().getContact().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.banking.v1.StandingOrderValidationService");
        var serviceName2 = "acme.banking.v1" + "." + "StandingOrderValidationService";
        var methodName2 = "Che" + "ckStandingOrderEligibility";
        var descriptor2 = MethodDescriptor.<CheckStandingOrderEligibilityRequest, CheckStandingOrderEligibilityResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(CheckStandingOrderEligibilityRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CheckStandingOrderEligibilityResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateBudgets", request.getBudget().getBudgetId());
        return RecalculateBudgetsResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRiskScenario().getAudit().getCreatedAt())
                                .build())
                        .setBudgetId(request.getBudgetId())
                        .setContact(request.getBudget().getContact())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Simulates a budget.
     */
    public SimulateBudgetResponse simulateBudget(SimulateBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = BeanCopies.copy(NormalizeCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBudget().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getBudget().getContact().getEmail())
                                .build())
                        .build())
                .setDetails(NormalizeCoverageRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getBudget().getContact().getPhone())
                                .build())
                        .build())
                .build(), NormalizeCoverageRequest.class);
        var reply0 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCoverage(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateBudget", request.getBudget().getBudgetId());
        return SimulateBudgetResponse.newBuilder()
                .setBreakdown(SimulateBudgetResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getBudget().getCurrencyCode())
                        .build())
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Explains a budget.
     */
    public ExplainBudgetCalculationResponse explainBudgetCalculation(ExplainBudgetCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainBudgetCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = StreamAuditTrailExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditTrailExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getAudit().getCreatedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .setOwnerId(request.getBudget().getBudgetId())
                        .build())
                .build();
        var stream0 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditTrailExport(outbound0);
        var reply0 = StreamAuditTrailExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ListEmployeesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListEmployeesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getAudit().getUpdatedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> employeesEmployeeServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listEmployees(value));
        log.atDebug().addKeyValue("auditValue", request.getBudget().getHolder().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainBudgetCalculation", request.getBudget().getHolder().getForecastId());
        return ExplainBudgetCalculationResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .setOutcome(ExplainBudgetCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a budget.
     */
    public GetBudgetRatesResponse getBudgetRates(GetBudgetRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBudgetRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "GetBudgetRates");

        var outbound0 = JsonCodec.roundTrip(FindBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindBankAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getBudgetId())
                        .build())
                .build(), FindBankAccountsRequest.class);
        var reply0 = bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findBankAccounts(outbound0);
        return GetBudgetRatesResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
