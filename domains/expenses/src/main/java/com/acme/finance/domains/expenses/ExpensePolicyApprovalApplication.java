package com.acme.finance.domains.expenses;

import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.ScheduleBudgetLineReportRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineReportResponse;
import com.acme.budgeting.v1.ScheduleVarianceExpiryRequest;
import com.acme.budgeting.v1.ScheduleVarianceExpiryResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceLifecycleServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.ControlDiscountAction;
import com.acme.compliance.v1.RecallControlRequest;
import com.acme.compliance.v1.RecallControlResponse;
import com.acme.customers.v1.CheckPreferenceEligibilityRequest;
import com.acme.customers.v1.CheckPreferenceEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.expenses.v1.ApproveExpensePolicyRequest;
import com.acme.expenses.v1.ApproveExpensePolicyResponse;
import com.acme.expenses.v1.CommentExpensePolicyApprovalRequest;
import com.acme.expenses.v1.CommentExpensePolicyApprovalResponse;
import com.acme.expenses.v1.CommentExpenseReportApprovalRequest;
import com.acme.expenses.v1.CommentExpenseReportApprovalResponse;
import com.acme.expenses.v1.CommentMileageClaimApprovalRequest;
import com.acme.expenses.v1.CommentMileageClaimApprovalResponse;
import com.acme.expenses.v1.DelegateExpensePolicyApprovalRequest;
import com.acme.expenses.v1.DelegateExpensePolicyApprovalResponse;
import com.acme.expenses.v1.DelegateExpenseReportApprovalRequest;
import com.acme.expenses.v1.DelegateExpenseReportApprovalResponse;
import com.acme.expenses.v1.EscalateExpensePolicyRequest;
import com.acme.expenses.v1.EscalateExpensePolicyResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyReportingServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.GetExpensePolicyLifecycleRequest;
import com.acme.expenses.v1.GetExpensePolicyLifecycleResponse;
import com.acme.expenses.v1.ListPendingExpensePoliciesRequest;
import com.acme.expenses.v1.ListPendingExpensePoliciesResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimApprovalServiceGrpc;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.PauseReceiptWorkflowRequest;
import com.acme.expenses.v1.PauseReceiptWorkflowResponse;
import com.acme.expenses.v1.RecallExpensePolicyRequest;
import com.acme.expenses.v1.RecallExpensePolicyResponse;
import com.acme.expenses.v1.RecallExpenseReportRequest;
import com.acme.expenses.v1.RecallExpenseReportResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.ReinstateExpensePolicyRequest;
import com.acme.expenses.v1.ReinstateExpensePolicyResponse;
import com.acme.expenses.v1.RejectExpensePolicyRequest;
import com.acme.expenses.v1.RejectExpensePolicyResponse;
import com.acme.expenses.v1.StreamExpensePolicyMetricsRequest;
import com.acme.expenses.v1.StreamExpensePolicyMetricsResponse;
import com.acme.expenses.v1.SubmitExpensePolicyRequest;
import com.acme.expenses.v1.SubmitExpensePolicyResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.modelregistry.v1.CalculateRegisteredModelRequest;
import com.acme.modelregistry.v1.CalculateRegisteredModelResponse;
import com.acme.modelregistry.v1.RegisteredModel;
import com.acme.modelregistry.v1.RegisteredModelCalculationServiceGrpc;
import com.acme.procurement.v1.AdvanceGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.AdvanceGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.acme.procurement.v1.GoodsReceiptLineTax;
import com.acme.procurement.v1.GoodsReceiptWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.risk.v1.SignalRiskScenarioWorkflowRequest;
import com.acme.risk.v1.SignalRiskScenarioWorkflowResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Routes expense policies through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ExpensePolicyApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpensePolicyApprovalApplication.class);

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking;

    private final VarianceLifecycleServiceGrpc.VarianceLifecycleServiceBlockingStub budgetingVarianceLifecycleServiceBlocking;

    private final ChannelRegistry channels;

    private final ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking;

    private final ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceFutureStub expensesExpensePolicyLifecycleServiceFuture;

    private final ExpensePolicyReportingServiceGrpc.ExpensePolicyReportingServiceBlockingStub expensesExpensePolicyReportingServiceBlocking;

    private final ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking;

    private final MileageClaimApprovalServiceGrpc.MileageClaimApprovalServiceBlockingStub expensesMileageClaimApprovalServiceBlocking;

    private final ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking;

    private final RegisteredModelCalculationServiceGrpc.RegisteredModelCalculationServiceBlockingStub modelregistryRegisteredModelCalculationServiceBlocking;

    private final GoodsReceiptWorkflowServiceGrpc.GoodsReceiptWorkflowServiceBlockingStub procurementGoodsReceiptWorkflowServiceBlocking;

    private final ExpensesRepository repository;

    private final RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking;

    private final Executor rpcExecutor;

    public ExpensePolicyApprovalApplication(
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking,
            VarianceLifecycleServiceGrpc.VarianceLifecycleServiceBlockingStub budgetingVarianceLifecycleServiceBlocking,
            ChannelRegistry channels,
            ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking,
            ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceFutureStub expensesExpensePolicyLifecycleServiceFuture,
            ExpensePolicyReportingServiceGrpc.ExpensePolicyReportingServiceBlockingStub expensesExpensePolicyReportingServiceBlocking,
            ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking,
            MileageClaimApprovalServiceGrpc.MileageClaimApprovalServiceBlockingStub expensesMileageClaimApprovalServiceBlocking,
            ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking,
            RegisteredModelCalculationServiceGrpc.RegisteredModelCalculationServiceBlockingStub modelregistryRegisteredModelCalculationServiceBlocking,
            GoodsReceiptWorkflowServiceGrpc.GoodsReceiptWorkflowServiceBlockingStub procurementGoodsReceiptWorkflowServiceBlocking,
            ExpensesRepository repository,
            RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.budgetingBudgetLineReportingServiceBlocking = budgetingBudgetLineReportingServiceBlocking;
        this.budgetingVarianceLifecycleServiceBlocking = budgetingVarianceLifecycleServiceBlocking;
        this.channels = channels;
        this.complianceControlApprovalServiceBlocking = complianceControlApprovalServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.expensesExpensePolicyLifecycleServiceBlocking = expensesExpensePolicyLifecycleServiceBlocking;
        this.expensesExpensePolicyLifecycleServiceFuture = expensesExpensePolicyLifecycleServiceFuture;
        this.expensesExpensePolicyReportingServiceBlocking = expensesExpensePolicyReportingServiceBlocking;
        this.expensesExpenseReportApprovalServiceBlocking = expensesExpenseReportApprovalServiceBlocking;
        this.expensesMileageClaimApprovalServiceBlocking = expensesMileageClaimApprovalServiceBlocking;
        this.expensesReceiptWorkflowServiceBlocking = expensesReceiptWorkflowServiceBlocking;
        this.modelregistryRegisteredModelCalculationServiceBlocking = modelregistryRegisteredModelCalculationServiceBlocking;
        this.procurementGoodsReceiptWorkflowServiceBlocking = procurementGoodsReceiptWorkflowServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioWorkflowServiceBlocking = riskRiskScenarioWorkflowServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits an expense policy.
     */
    public SubmitExpensePolicyResponse submitExpensePolicy(SubmitExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = CommentMileageClaimApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentMileageClaimApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExpensePolicy().getAudit().getCreatedBy())
                                .build())
                        .addLines(MileageClaimLine.newBuilder()
                                .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        Function<CommentMileageClaimApprovalRequest, CommentMileageClaimApprovalResponse> operation0 = expensesMileageClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::commentMileageClaimApproval;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = PauseReceiptWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PauseReceiptWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setNumber(reply0.getMileageClaim().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> expensesReceiptWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseReceiptWorkflow(value));

        var builder2 = SignalRiskScenarioWorkflowRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setDetails(SignalRiskScenarioWorkflowRequest.Details.newBuilder()
                .setNotify(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build());
        builder2.setRiskScenario(RiskScenario.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(reply0.getMileageClaim().getAudit().getUpdatedAt())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = riskRiskScenarioWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalRiskScenarioWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return SubmitExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getMileageClaim().getAudit().getCreatedAt())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .putAllParameters(request.getExpensePolicy().getParametersMap())
                        .build())
                .setOutcome(SubmitExpensePolicyResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves an expense policy.
     */
    public ApproveExpensePolicyResponse approveExpensePolicy(ApproveExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = ScoreHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<ScoreHouseholdRequest, ScoreHouseholdResponse> port0 = RpcPort.adapt(customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreHousehold);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = CheckPreferenceEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getExpensePolicy().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.customers.v1.PreferenceValidationService");
        var serviceName1 = "acme.customers.v1" + "." + "PreferenceValidationService";
        var methodName1 = "Che" + "ckPreferenceEligibility";
        var descriptor1 = MethodDescriptor.<CheckPreferenceEligibilityRequest, CheckPreferenceEligibilityResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(CheckPreferenceEligibilityRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CheckPreferenceEligibilityResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = GetExpensePolicyLifecycleRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicyId(request.getExpensePolicyId())
                .build();
        var pending2 = expensesExpensePolicyLifecycleServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).getExpensePolicyLifecycle(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = DelegateExpenseReportApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(reply2.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> expensesExpenseReportApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateExpenseReportApproval(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return ApproveExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPreference().getAudit().getCreatedBy())
                                .setUpdatedAt(reply1.getPreference().getAudit().getUpdatedAt())
                                .build())
                        .setEffective(request.getExpensePolicy().getEffective())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getHousehold().getManager().getUserId())
                                .build())
                        .build())
                .setOutcome(ApproveExpensePolicyResponse.Outcome.newBuilder()
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects an expense policy.
     */
    public RejectExpensePolicyResponse rejectExpensePolicy(RejectExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = RecallControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setDiscount(ControlDiscountAction.newBuilder()
                                .setPercent(request.getExpensePolicy().getDiscount().getPercent())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setDetails(RecallControlRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getExpensePolicy().getOwner().getEmail())
                                .setUserId(request.getExpensePolicy().getOwner().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> complianceControlApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallControl(value)).orElseThrow();

        var builder1 = EscalateStandingOrderRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setStandingOrder(StandingOrder.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(request.getExpensePolicy().getAudit().getUpdatedBy())
                        .build())
                .addParticipants(StandingOrderParticipant.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getControl().getOwner().getEmail())
                                .build())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateStandingOrder(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return RejectExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(request.getExpensePolicy().getOwner())
                        .build())
                .setOutcome(RejectExpensePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates an expense policy.
     */
    public EscalateExpensePolicyResponse escalateExpensePolicy(EscalateExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = ScheduleBudgetLineReportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(switch (request.getExpensePolicy().getActionCase()) {
                    case BLOCK -> request.getExpensePolicy().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply0 = budgetingBudgetLineReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleBudgetLineReport(outbound0);

        var outbound1 = StreamExpensePolicyMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamExpensePolicyMetricsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpensePolicy().getAudit().getUpdatedAt())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream1 = expensesExpensePolicyReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamExpensePolicyMetrics(outbound1);
        var reply1 = StreamExpensePolicyMetricsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = JsonCodec.roundTrip(ScheduleVarianceExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build(), ScheduleVarianceExpiryRequest.class);
        var reply2 = budgetingVarianceLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleVarianceExpiry(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return EscalateExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply2.getVariance().getAssignee().getUserId())
                                .build())
                        .setVersion(request.getExpensePolicy().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists expense policies a page at a time.
     */
    public ListPendingExpensePoliciesResponse listPendingExpensePolicies(ListPendingExpensePoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingExpensePolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getContext().getTraceId(), "ListPendingExpensePolicies");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingExpensePolicies", request.getPage().getPageToken());
        return ListPendingExpensePoliciesResponse.getDefaultInstance();
    }

    /**
     * Delegates an expense policy.
     */
    public DelegateExpensePolicyApprovalResponse delegateExpensePolicyApproval(DelegateExpensePolicyApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateExpensePolicyApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = JsonCodec.roundTrip(ReinstateExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReinstateExpensePolicyRequest.Details.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getExpensePolicy().getOwner().getDisplayName())
                                .setEmail(request.getExpensePolicy().getOwner().getEmail())
                                .setUserId(request.getExpensePolicy().getOwner().getUserId())
                                .build())
                        .build())
                .setExpensePolicyId(request.getExpensePolicy().getExpensePolicyId())
                .build(), ReinstateExpensePolicyRequest.class);
        var reply0 = expensesExpensePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reinstateExpensePolicy(outbound0);

        var outbound1 = CommentExpenseReportApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentExpenseReportApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(reply0.getExpensePolicy().getOwner().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(reply0.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.expenses.v1.ExpenseReportApprovalService/CommentExpenseReportApproval", outbound1);
        var reply1 = expensesExpenseReportApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentExpenseReportApproval(command1.payload());

        var outbound2 = ExplainConsentRecordDecisionRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(reply1.getExpenseReport().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainConsentRecordDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainConsentRecordDecision(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = CalculateRegisteredModelRequest.newBuilder()
                .setContext(request.getContext())
                .setRegisteredModel(RegisteredModel.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var command3 = new DispatchCommand<>("acme.modelregistry.v1.RegisteredModelCalculationService/CalculateRegisteredModel", outbound3);
        var reply3 = modelregistryRegisteredModelCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateRegisteredModel(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateExpensePolicyApproval", request.getExpensePolicy().getName());
        return DelegateExpensePolicyApprovalResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(reply0.getExpensePolicy().getOwner())
                        .setPriority(request.getExpensePolicy().getPriority())
                        .build())
                .setOutcome(DelegateExpensePolicyApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls an expense policy.
     */
    public RecallExpensePolicyResponse recallExpensePolicy(RecallExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = RpcSupport.unpack(Any.pack(RecallExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallExpenseReportRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getExpensePolicy().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build()), RecallExpenseReportRequest.class);
        var reply0 = expensesExpenseReportApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallExpenseReport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return RecallExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .setOutcome(RecallExpensePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Comments an expense policy.
     */
    public CommentExpensePolicyApprovalResponse commentExpensePolicyApproval(CommentExpensePolicyApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentExpensePolicyApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = EscalateStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateStandingOrderRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getExpensePolicy().getOwner().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateStandingOrder(outbound0), rpcExecutor);

        var outbound1 = AdvanceGoodsReceiptWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .addLines(GoodsReceiptLine.newBuilder()
                                .setTax(GoodsReceiptLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> procurementGoodsReceiptWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceGoodsReceiptWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentExpensePolicyApproval", request.getExpensePolicy().getExpensePolicyId());
        return CommentExpensePolicyApprovalResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getGoodsReceipt().getAudit().getCreatedBy())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getStandingOrder().getOrganizer().getEmail())
                                .build())
                        .build())
                .setOutcome(CommentExpensePolicyApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
