package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.NotifyAuditEventWatchersRequest;
import com.acme.audit.v1.NotifyAuditEventWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.QuoteRepaymentRequest;
import com.acme.lending.v1.QuoteRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.procurement.v1.CalculatePurchaseOrderRequest;
import com.acme.procurement.v1.CalculatePurchaseOrderResponse;
import com.acme.procurement.v1.CalculatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CalculatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ComparePurchaseOrderOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseOrderOptionsResponse;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsResponse;
import com.acme.procurement.v1.EstimatePurchaseOrderRequest;
import com.acme.procurement.v1.EstimatePurchaseOrderResponse;
import com.acme.procurement.v1.EstimatePurchaseRequisitionRequest;
import com.acme.procurement.v1.EstimatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationResponse;
import com.acme.procurement.v1.GetPurchaseOrderRatesRequest;
import com.acme.procurement.v1.GetPurchaseOrderRatesResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionBankSettlement;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionLineTax;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.QuotePurchaseRequisitionRequest;
import com.acme.procurement.v1.QuotePurchaseRequisitionResponse;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.UpdateRiskRegisterRequest;
import com.acme.risk.v1.UpdateRiskRegisterResponse;
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
 * Computes the figures of a purchase order, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class PurchaseOrderCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseOrderCalculationApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync;

    private final RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceFutureStub procurementPurchaseRequisitionCalculationServiceFuture;

    private final ProcurementRepository repository;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking;

    private final Executor rpcExecutor;

    public PurchaseOrderCalculationApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync,
            RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceFutureStub procurementPurchaseRequisitionCalculationServiceFuture,
            ProcurementRepository repository,
            RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.contractsContractServiceAsync = contractsContractServiceAsync;
        this.lendingRepaymentCalculationServiceBlocking = lendingRepaymentCalculationServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceBlocking = procurementPurchaseRequisitionCalculationServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceFuture = procurementPurchaseRequisitionCalculationServiceFuture;
        this.repository = repository;
        this.riskRiskRegisterServiceBlocking = riskRiskRegisterServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a purchase order.
     */
    public CalculatePurchaseOrderResponse calculatePurchaseOrder(CalculatePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculatePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = ComparePurchaseRequisitionOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ComparePurchaseRequisitionOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBank(PurchaseRequisitionBankSettlement.newBuilder()
                                .setIban(switch (request.getPurchaseOrder().getSettlementCase()) {
                                    case BANK -> request.getPurchaseOrder().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setNumber(request.getPurchaseOrder().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var reply0 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePurchaseRequisitionOptions(outbound0);

        var outbound1 = ExplainPurchaseRequisitionCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setCard(PurchaseRequisitionCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getPurchaseRequisition().getSettlementCase()) {
                                    case CARD -> reply0.getPurchaseRequisition().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setDisplayName(request.getPurchaseOrder().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPurchaseRequisitionCalculation(outbound1);

        var outbound2 = UpdateRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getDetails().getDestination().getLocality())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> riskRiskRegisterServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateRiskRegister(value));

        List<EstimatePurchaseRequisitionResponse> results3 = request.getPurchaseOrder().getLinesList().stream()
                .map(element -> EstimatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addLines(PurchaseRequisitionLine.newBuilder()
                                .setTax(PurchaseRequisitionLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePurchaseRequisition(value)))
                .collect(Collectors.toList());
        var reply3 = results3.isEmpty() ? EstimatePurchaseRequisitionResponse.getDefaultInstance()
                : results3.getLast();

        var outbound4 = BatchCreateContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateContractsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateContractsResponse.getDefaultInstance());
        var input4 = contractsContractServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateContracts(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPurchaseOrder().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPurchaseOrder().getAudit().getCreatedBy())
                                .setEtag(request.getPurchaseOrder().getAudit().getEtag())
                                .setUpdatedAt(request.getPurchaseOrder().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input5 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculatePurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return CalculatePurchaseOrderResponse.newBuilder()
                .setBreakdown(CalculatePurchaseOrderResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setTotal(reply1.getPurchaseRequisition().getTotal())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply2.getRiskRegister().getLocation().getLinesList())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply0.getPurchaseRequisition().getCounterparty().getNumber())
                                .build())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Estimates a purchase order.
     */
    public EstimatePurchaseOrderResponse estimatePurchaseOrder(EstimatePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimatePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = QuoteRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteRepaymentRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPurchaseOrder().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getPurchaseOrder().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply0 = lendingRepaymentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteRepayment(outbound0);

        var outbound1 = NotifyAuditEventWatchersRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(NotifyAuditEventWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getRepayment().getOrganizer().getEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<NotifyAuditEventWatchersRequest, NotifyAuditEventWatchersResponse>() {
            @Override
            public NotifyAuditEventWatchersResponse apply(NotifyAuditEventWatchersRequest input) {
                return auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyAuditEventWatchers(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimatePurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return EstimatePurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(reply0.getRepayment().getOrganizer().getDisplayName())
                                .build())
                        .setIssuedAt(request.getPurchaseOrder().getIssuedAt())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Quotes a purchase order.
     */
    public QuotePurchaseOrderResponse quotePurchaseOrder(QuotePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuotePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = ComparePurchaseRequisitionOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .setRecipient(request.getPurchaseOrder().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply0 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePurchaseRequisitionOptions(outbound0);

        var results1 = new ArrayList<EstimatePurchaseRequisitionResponse>();
        for (var element : request.getPurchaseOrder().getLinesList()) {
            EstimatePurchaseRequisitionRequest itemRequest = EstimatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addLines(PurchaseRequisitionLine.newBuilder()
                                .setTax(PurchaseRequisitionLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePurchaseRequisition(itemRequest));
        }
        var reply1 = results1.isEmpty() ? EstimatePurchaseRequisitionResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuotePurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return QuotePurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply0.getPurchaseRequisition().getCounterparty().getNumber())
                                .build())
                        .setIssuedAt(request.getPurchaseOrder().getIssuedAt())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Compares a purchase order.
     */
    public ComparePurchaseOrderOptionsResponse comparePurchaseOrderOptions(ComparePurchaseOrderOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ComparePurchaseOrderOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = CompareBudgetOptionsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPurchaseOrder().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareBudgetOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build();
        Function<CompareBudgetOptionsRequest, CompareBudgetOptionsResponse> operation0 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::compareBudgetOptions;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = JsonCodec.roundTrip(RevertRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertRetentionPolicyRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBudget().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getPurchaseOrder().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build(), RevertRetentionPolicyRequest.class);
        var reply1 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertRetentionPolicy(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ComparePurchaseOrderOptions", request.getPurchaseOrder().getPurchaseOrderId());
        return ComparePurchaseOrderOptionsResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(request.getPurchaseOrder().getNumber())
                                .build())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Simulates a purchase order.
     */
    public SimulatePurchaseOrderResponse simulatePurchaseOrder(SimulatePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulatePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = QuotePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePurchaseRequisitionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setPostalCode(request.getPurchaseOrder().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPurchaseOrder().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePurchaseRequisition(outbound0), rpcExecutor);

        var outbound1 = CalculatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePurchaseRequisitionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getPurchaseOrder().getBillingAddress().getLinesList())
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPurchaseOrder().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePurchaseRequisition(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ExplainPurchaseRequisitionCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPurchaseRequisition().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getPurchaseRequisition().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setNumber(reply1.getPurchaseRequisition().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var pending2 = procurementPurchaseRequisitionCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainPurchaseRequisitionCalculation(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulatePurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return SimulatePurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getPurchaseRequisition().getAudit().getCreatedBy())
                                .build())
                        .setDueAt(reply0.getPurchaseRequisition().getDueAt())
                        .putAllMetadata(reply1.getPurchaseRequisition().getMetadataMap())
                        .setNumber(reply2.getPurchaseRequisition().getCounterparty().getNumber())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .setStatus(request.getPurchaseOrder().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a purchase order.
     */
    public GetPurchaseOrderRatesResponse getPurchaseOrderRates(GetPurchaseOrderRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPurchaseOrderRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "GetPurchaseOrderRates");

        var outbound0 = RecalculatePurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setPurchaseOrderId(request.getPurchaseOrderId())
                                .build())
                        .setDueAt(request.getContext().getDeadline())
                        .putAllMetadata(request.getContext().getBaggageMap())
                        .build())
                .build();
        Function<RecalculatePurchaseRequisitionsRequest, RecalculatePurchaseRequisitionsResponse> operation0 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculatePurchaseRequisitions;
        var reply0 = operation0.apply(outbound0);
        return GetPurchaseOrderRatesResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getPurchaseRequisition().getBillingAddress().getPostalCode())
                                .build())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }
}
