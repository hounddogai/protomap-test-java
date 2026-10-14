package com.acme.finance.domains.procurement;

import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.CommentBudgetApprovalRequest;
import com.acme.budgeting.v1.CommentBudgetApprovalResponse;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.StreamVarianceMetricsRequest;
import com.acme.budgeting.v1.StreamVarianceMetricsResponse;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.careplans.v1.BrowseCarePlansRequest;
import com.acme.careplans.v1.BrowseCarePlansResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.StartAttestationWorkflowRequest;
import com.acme.compliance.v1.StartAttestationWorkflowResponse;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.SearchArticlesRequest;
import com.acme.content.v1.SearchArticlesResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.EstimatePreferenceRequest;
import com.acme.customers.v1.EstimatePreferenceResponse;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.UpdateCustomerRequest;
import com.acme.customers.v1.UpdateCustomerResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.giftcards.v1.GiftCardServiceGrpc;
import com.acme.giftcards.v1.SearchGiftCardsRequest;
import com.acme.giftcards.v1.SearchGiftCardsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CancelClaimWorkflowRequest;
import com.acme.insurance.v1.CancelClaimWorkflowResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.ListPendingPoliciesRequest;
import com.acme.insurance.v1.ListPendingPoliciesResponse;
import com.acme.insurance.v1.MapUnderwritingImportColumnsRequest;
import com.acme.insurance.v1.MapUnderwritingImportColumnsResponse;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.metrics.v1.CreateMetricSeriesRequest;
import com.acme.metrics.v1.CreateMetricSeriesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.procurement.v1.ActivateRfqRequest;
import com.acme.procurement.v1.ActivateRfqResponse;
import com.acme.procurement.v1.DeleteRfqReportRequest;
import com.acme.procurement.v1.DeleteRfqReportResponse;
import com.acme.procurement.v1.ExpireRfqRequest;
import com.acme.procurement.v1.ExpireRfqResponse;
import com.acme.procurement.v1.ExplainGoodsReceiptCalculationRequest;
import com.acme.procurement.v1.ExplainGoodsReceiptCalculationResponse;
import com.acme.procurement.v1.GetRfqLifecycleRequest;
import com.acme.procurement.v1.GetRfqLifecycleResponse;
import com.acme.procurement.v1.GetRfqRulesRequest;
import com.acme.procurement.v1.GetRfqRulesResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptCardSettlement;
import com.acme.procurement.v1.GoodsReceiptLifecycleServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.ReinstateRfqRequest;
import com.acme.procurement.v1.ReinstateRfqResponse;
import com.acme.procurement.v1.RenewRfqRequest;
import com.acme.procurement.v1.RenewRfqResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.ScheduleGoodsReceiptExpiryRequest;
import com.acme.procurement.v1.ScheduleGoodsReceiptExpiryResponse;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryRequest;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryResponse;
import com.acme.procurement.v1.ScheduleRfqExpiryRequest;
import com.acme.procurement.v1.ScheduleRfqExpiryResponse;
import com.acme.procurement.v1.SuspendRfqRequest;
import com.acme.procurement.v1.SuspendRfqResponse;
import com.acme.procurement.v1.TerminateRfqRequest;
import com.acme.procurement.v1.TerminateRfqResponse;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowRequest;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.suppliers.v1.SignalSupplierWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierWorkflowResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Moves a rfq between the states of its lifecycle. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class RfqLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(RfqLifecycleApplication.class);

    private final BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking;

    private final VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking;

    private final ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceFutureStub customersCustomerSegmentValidationServiceFuture;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final GiftCardServiceGrpc.GiftCardServiceBlockingStub giftcardsGiftCardServiceBlocking;

    private final ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking;

    private final GoodsReceiptLifecycleServiceGrpc.GoodsReceiptLifecycleServiceBlockingStub procurementGoodsReceiptLifecycleServiceBlocking;

    private final PurchaseOrderLifecycleServiceGrpc.PurchaseOrderLifecycleServiceBlockingStub procurementPurchaseOrderLifecycleServiceBlocking;

    private final RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking;

    private final RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking;

    private final ProcurementRepository repository;

    private final RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking;

    public RfqLifecycleApplication(
            BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking,
            VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking,
            ChannelRegistry channels,
            AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking,
            ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceFutureStub customersCustomerSegmentValidationServiceFuture,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            GiftCardServiceGrpc.GiftCardServiceBlockingStub giftcardsGiftCardServiceBlocking,
            ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking,
            GoodsReceiptLifecycleServiceGrpc.GoodsReceiptLifecycleServiceBlockingStub procurementGoodsReceiptLifecycleServiceBlocking,
            PurchaseOrderLifecycleServiceGrpc.PurchaseOrderLifecycleServiceBlockingStub procurementPurchaseOrderLifecycleServiceBlocking,
            RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking,
            RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking,
            ProcurementRepository repository,
            RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking) {
        this.budgetingBudgetApprovalServiceBlocking = budgetingBudgetApprovalServiceBlocking;
        this.budgetingVarianceReportingServiceBlocking = budgetingVarianceReportingServiceBlocking;
        this.channels = channels;
        this.complianceAttestationWorkflowServiceBlocking = complianceAttestationWorkflowServiceBlocking;
        this.contentArticleServiceBlocking = contentArticleServiceBlocking;
        this.customersCustomerSegmentValidationServiceFuture = customersCustomerSegmentValidationServiceFuture;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.giftcardsGiftCardServiceBlocking = giftcardsGiftCardServiceBlocking;
        this.insuranceClaimWorkflowServiceBlocking = insuranceClaimWorkflowServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.insuranceUnderwritingImportServiceBlocking = insuranceUnderwritingImportServiceBlocking;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.procurementGoodsReceiptCalculationServiceBlocking = procurementGoodsReceiptCalculationServiceBlocking;
        this.procurementGoodsReceiptLifecycleServiceBlocking = procurementGoodsReceiptLifecycleServiceBlocking;
        this.procurementPurchaseOrderLifecycleServiceBlocking = procurementPurchaseOrderLifecycleServiceBlocking;
        this.procurementRfqReportingServiceBlocking = procurementRfqReportingServiceBlocking;
        this.procurementRfqValidationServiceBlocking = procurementRfqValidationServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterWorkflowServiceBlocking = riskRiskRegisterWorkflowServiceBlocking;
    }

    /**
     * Activates a rfq.
     */
    public ActivateRfqResponse activateRfq(ActivateRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "ActivateRfq");

        var outbound0 = BeanCopies.copy(DeleteRfqReportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setRfqId(request.getRfqId())
                .build(), DeleteRfqReportRequest.class);
        var reply0 = procurementRfqReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deleteRfqReport(outbound0);

        var outbound1 = StreamVarianceMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .build())
                .build();
        var stream1 = budgetingVarianceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceMetrics(outbound1);
        var reply1 = StreamVarianceMetricsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = CommentBudgetApprovalRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter2 = new Function<CommentBudgetApprovalRequest, CommentBudgetApprovalResponse>() {
            @Override
            public CommentBudgetApprovalResponse apply(CommentBudgetApprovalRequest input) {
                return budgetingBudgetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentBudgetApproval(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateRfq", request.getDetails().getReasonCode());
        return ActivateRfqResponse.newBuilder()
                .setOutcome(ActivateRfqResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Suspends a rfq.
     */
    public SuspendRfqResponse suspendRfq(SuspendRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "SuspendRfq");

        var outbound0 = SchedulePurchaseOrderExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SchedulePurchaseOrderExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> procurementPurchaseOrderLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).schedulePurchaseOrderExpiry(value));

        var outbound1 = BrowseCarePlansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCarePlansRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPurchaseOrder().getAudit().getCreatedAt())
                        .setRegion(reply0.getPurchaseOrder().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.careplans.v1.CarePlanQueryService");
        var serviceName1 = "acme.careplans.v1" + "." + "CarePlanQueryService";
        var methodName1 = "Bro" + "wseCarePlans";
        var descriptor1 = MethodDescriptor.<BrowseCarePlansRequest, BrowseCarePlansResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(BrowseCarePlansRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BrowseCarePlansResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendRfq", request.getDetails().getReasonCode());
        return SuspendRfqResponse.newBuilder()
                .setOutcome(SuspendRfqResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getPurchaseOrder().getCounterparty().getDisplayName())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Reinstates a rfq.
     */
    public ReinstateRfqResponse reinstateRfq(ReinstateRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "ReinstateRfq");

        var outbound0 = ExplainGoodsReceiptCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainGoodsReceiptCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = procurementGoodsReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainGoodsReceiptCalculation(outbound0);

        var outbound1 = ScheduleGoodsReceiptExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleGoodsReceiptExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCard(GoodsReceiptCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getGoodsReceipt().getSettlementCase()) {
                                    case CARD -> reply0.getGoodsReceipt().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = procurementGoodsReceiptLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleGoodsReceiptExpiry(outbound1);

        var outbound2 = ListPendingPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getGoodsReceipt().getAudit().getCreatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(reply1.getOutcome().getReferenceId())
                        .setRegion(reply1.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        Function<ListPendingPoliciesRequest, ListPendingPoliciesResponse> operation2 = insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listPendingPolicies;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = LintCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var pending3 = customersCustomerSegmentValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomerSegments(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateRfq", request.getDetails().getReasonCode());
        return ReinstateRfqResponse.newBuilder()
                .setOutcome(ReinstateRfqResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getGoodsReceipt().getAudit().getEtag())
                                .setUpdatedAt(reply1.getGoodsReceipt().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply3.getCustomerSegment().getAudit().getUpdatedBy())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Terminates a rfq.
     */
    public TerminateRfqResponse terminateRfq(TerminateRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "TerminateRfq");

        var outbound0 = MapUnderwritingImportColumnsRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.insurance.v1.UnderwritingImportService/MapUnderwritingImportColumns", outbound0);
        var reply0 = insuranceUnderwritingImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).mapUnderwritingImportColumns(command0.payload());

        var outbound1 = RpcSupport.unpack(Any.pack(UpdateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getUnderwriting().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getUnderwriting().getRequester().getPhone())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(reply0.getUnderwriting().getRequester().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build()), UpdateCustomerRequest.class);
        var reply1 = customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateCustomer(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateRfq", request.getDetails().getReasonCode());
        return TerminateRfqResponse.newBuilder()
                .setOutcome(TerminateRfqResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getUnderwriting().getCustomer().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getCustomer().getAudit().getUpdatedBy())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Renews a rfq.
     */
    public RenewRfqResponse renewRfq(RenewRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "RenewRfq");

        var outbound0 = SignalSupplierWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalSupplierWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.suppliers.v1.SupplierWorkflowService");
        var selected0 = SupplierWorkflowServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.signalSupplierWorkflow(outbound0);

        var outbound1 = EstimatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getSupplier().getRelationshipManager().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<EstimatePreferenceRequest, EstimatePreferenceResponse> operation1 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimatePreference;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = StartAttestationWorkflowRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getSupplier().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command2 = new DispatchCommand<>("acme.compliance.v1.AttestationWorkflowService/StartAttestationWorkflow", outbound2);
        var reply2 = complianceAttestationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startAttestationWorkflow(command2.payload());
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewRfq", request.getDetails().getReasonCode());
        return RenewRfqResponse.newBuilder()
                .setOutcome(RenewRfqResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getSupplier().getRelationshipManager().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPreference().getAudit().getCreatedBy())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Expires a rfq.
     */
    public ExpireRfqResponse expireRfq(ExpireRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "ExpireRfq");

        var outbound0 = JsonCodec.roundTrip(SearchGiftCardsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchGiftCardsRequest.Filter.newBuilder()
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build(), SearchGiftCardsRequest.class);
        var reply0 = giftcardsGiftCardServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchGiftCards(outbound0);

        var outbound1 = BeanCopies.copy(CancelClaimWorkflowRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), CancelClaimWorkflowRequest.class);
        var reply1 = insuranceClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelClaimWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireRfq", request.getDetails().getReasonCode());
        return ExpireRfqResponse.newBuilder()
                .setOutcome(ExpireRfqResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getClaim().getRequester().getEmail())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Returns a rfq.
     */
    public GetRfqLifecycleResponse getRfqLifecycle(GetRfqLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRfqLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "GetRfqLifecycle");

        var outbound0 = ResumeRiskRegisterWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getContext().getDeadline())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .setParentId(request.getRfqId())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> riskRiskRegisterWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeRiskRegisterWorkflow(value));

        var outbound1 = CreateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRiskRegister().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getRiskRegister().getManager().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createMetricSeries(outbound1);
        return GetRfqLifecycleResponse.newBuilder()
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getRiskRegister().getManager().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getMetricSeries().getAudit().getEtag())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Schedules a rfq.
     */
    public ScheduleRfqExpiryResponse scheduleRfqExpiry(ScheduleRfqExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleRfqExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        List<SearchArticlesResponse> results0 = request.getRfq().getCommentsList().stream()
                .map(element -> SearchArticlesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchArticlesRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(element.getCommentId())
                        .build())
                .build())
                .flatMap(value -> Stream.of(contentArticleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchArticles(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? SearchArticlesResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RpcSupport.unpack(Any.pack(GetRfqRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setRfqId(request.getRfq().getRfqId())
                .build()), GetRfqRulesRequest.class);
        var reply1 = procurementRfqValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getRfqRules(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getAssignee().getUserId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleRfqExpiry", request.getRfq().getAssignee().getUserId());
        return ScheduleRfqExpiryResponse.newBuilder()
                .setRfq(Rfq.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getRfq().getRequester().getPreferredChannel())
                                .build())
                        .setRfqId(request.getRfqId())
                        .setSubject(request.getRfq().getSubject())
                        .build())
                .build();
    }
}
