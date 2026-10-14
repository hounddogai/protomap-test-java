package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualCardSettlement;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.ValidateAccrualRequest;
import com.acme.accounting.v1.ValidateAccrualResponse;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.BatchGetAccessPoliciesRequest;
import com.acme.authorization.v1.BatchGetAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.customers.v1.BatchGetCustomersRequest;
import com.acme.customers.v1.BatchGetCustomersResponse;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.ExplainPreferenceDecisionRequest;
import com.acme.customers.v1.ExplainPreferenceDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.NormalizePreferenceRequest;
import com.acme.customers.v1.NormalizePreferenceResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceDiscountAction;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvancePolicyWorkflowRequest;
import com.acme.insurance.v1.AdvancePolicyWorkflowResponse;
import com.acme.insurance.v1.CancelPolicyWorkflowRequest;
import com.acme.insurance.v1.CancelPolicyWorkflowResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.GenerateUnderwritingReportRequest;
import com.acme.insurance.v1.GenerateUnderwritingReportResponse;
import com.acme.insurance.v1.ListPolicyWorkflowsRequest;
import com.acme.insurance.v1.ListPolicyWorkflowsResponse;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.MapUnderwritingImportColumnsRequest;
import com.acme.insurance.v1.MapUnderwritingImportColumnsResponse;
import com.acme.insurance.v1.PausePolicyWorkflowRequest;
import com.acme.insurance.v1.PausePolicyWorkflowResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PushUnderwritingRequest;
import com.acme.insurance.v1.PushUnderwritingResponse;
import com.acme.insurance.v1.ResumePolicyWorkflowRequest;
import com.acme.insurance.v1.ResumePolicyWorkflowResponse;
import com.acme.insurance.v1.SignalPolicyWorkflowRequest;
import com.acme.insurance.v1.SignalPolicyWorkflowResponse;
import com.acme.insurance.v1.StartPolicyWorkflowRequest;
import com.acme.insurance.v1.StartPolicyWorkflowResponse;
import com.acme.insurance.v1.SubmitClaimRequest;
import com.acme.insurance.v1.SubmitClaimResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountLimit;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.ScorePayStubRequest;
import com.acme.payroll.v1.ScorePayStubResponse;
import com.acme.procurement.v1.ListPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.sellers.v1.BatchGetSellersRequest;
import com.acme.sellers.v1.BatchGetSellersResponse;
import com.acme.sellers.v1.SellerServiceGrpc;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingBankSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a policy from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PolicyWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(PolicyWorkflowApplication.class);

    private final AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final SellerServiceGrpc.SellerServiceBlockingStub sellersSellerServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    public PolicyWorkflowApplication(
            AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ChannelRegistry channels,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SellerServiceGrpc.SellerServiceBlockingStub sellersSellerServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking) {
        this.accountingAccrualValidationServiceBlocking = accountingAccrualValidationServiceBlocking;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.channels = channels;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.insuranceClaimApprovalServiceBlocking = insuranceClaimApprovalServiceBlocking;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.payrollPayStubValidationServiceBlocking = payrollPayStubValidationServiceBlocking;
        this.procurementPurchaseRequisitionServiceBlocking = procurementPurchaseRequisitionServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.sellersSellerServiceBlocking = sellersSellerServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
    }

    /**
     * Starts a policy.
     */
    public StartPolicyWorkflowResponse startPolicyWorkflow(StartPolicyWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartPolicyWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = BeanCopies.copy(BatchGetSellersRequest.newBuilder()
                .setContext(request.getContext())
                .build(), BatchGetSellersRequest.class);
        var reply0 = sellersSellerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSellers(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartPolicyWorkflow", request.getPolicy().getPolicyId());
        return StartPolicyWorkflowResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setOpenedAt(request.getPolicy().getOpenedAt())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances a policy.
     */
    public AdvancePolicyWorkflowResponse advancePolicyWorkflow(AdvancePolicyWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvancePolicyWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = PreviewSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSupplierRatingRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getPolicy().getContact().getName())
                                .setPreferredChannel(request.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBank(SupplierRatingBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getPolicy().getFundingCase()) {
                                    case BANK -> request.getPolicy().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSupplierRating(outbound0);

        var outbound1 = BrowseCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCustomersRequest.Filter.newBuilder()
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .setRegion(reply0.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseCustomers(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvancePolicyWorkflow", request.getPolicy().getPolicyId());
        return AdvancePolicyWorkflowResponse.newBuilder()
                .setOutcome(AdvancePolicyWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setPolicyId(request.getPolicyId())
                        .setStatus(request.getPolicy().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Cancels a policy.
     */
    public CancelPolicyWorkflowResponse cancelPolicyWorkflow(CancelPolicyWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelPolicyWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var results0 = new ArrayList<ScorePayStubResponse>();
        for (var element : request.getPolicy().getLimitsList()) {
            ScorePayStubRequest itemRequest = ScorePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
            results0.add(payrollPayStubValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePayStub(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ScorePayStubResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPolicy().getOpenedAt())
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .setOwnerId(switch (request.getPolicy().getFundingCase()) {
                            case EXTERNAL_ACCOUNT_ID -> request.getPolicy().getExternalAccountId();
                            default -> "";
                        })
                        .setRegion(reply0.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = procurementPurchaseRequisitionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPurchaseRequisitions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelPolicyWorkflow", request.getPolicy().getPolicyId());
        return CancelPolicyWorkflowResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setContact(request.getPolicy().getContact())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Pauses a policy.
     */
    public PausePolicyWorkflowResponse pausePolicyWorkflow(PausePolicyWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PausePolicyWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = GenerateUnderwritingReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateUnderwritingReportRequest.Details.newBuilder()
                        .putAllFilters(request.getPolicy().getLabelsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.UnderwritingReportingService");
        var serviceName0 = "acme.insurance.v1" + "." + "UnderwritingReportingService";
        var methodName0 = "Gen" + "erateUnderwritingReport";
        var descriptor0 = MethodDescriptor.<GenerateUnderwritingReportRequest, GenerateUnderwritingReportResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(GenerateUnderwritingReportRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(GenerateUnderwritingReportResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = BatchGetCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.customers.v1.CustomerService");
        var selected1 = CustomerServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.batchGetCustomers(outbound1);

        var outbound2 = ListUnderwritingConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListUnderwritingConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPolicy().getAudit().getUpdatedAt())
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .setOwnerId(request.getPolicy().getHolder().getCoverageId())
                        .build())
                .build();
        final class Stage2Adapter {
            ListUnderwritingConflictsResponse fetch(ListUnderwritingConflictsRequest input) {
                return insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listUnderwritingConflicts(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = ValidateAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPolicy().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setCard(AccrualCardSettlement.newBuilder()
                                .setLast4(request.getPolicy().getCard().getLast4())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command3 = new DispatchCommand<>("acme.accounting.v1.AccrualValidationService/ValidateAccrual", outbound3);
        var reply3 = accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAccrual(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PausePolicyWorkflow", request.getPolicy().getPolicyId());
        return PausePolicyWorkflowResponse.newBuilder()
                .setOutcome(PausePolicyWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAvailable(request.getPolicy().getAvailable())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply3.getAccrual().getCounterparty().getDisplayName())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes a policy.
     */
    public ResumePolicyWorkflowResponse resumePolicyWorkflow(ResumePolicyWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumePolicyWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = RecalculateHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getPolicy().getAudit().getCreatedBy())
                                                .setUpdatedAt(request.getPolicy().getAudit().getUpdatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateHouseholds(value));

        var outbound1 = JsonCodec.roundTrip(SubmitClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getHousehold().getManager().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getHousehold().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), SubmitClaimRequest.class);
        var reply1 = insuranceClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitClaim(outbound1);

        var outbound2 = (BatchGetAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply2 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetAccessPolicies(outbound2);

        var results3 = new ArrayList<NormalizePreferenceResponse>();
        for (var element : request.getPolicy().getLimitsList()) {
            NormalizePreferenceRequest itemRequest = NormalizePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setDiscount(PreferenceDiscountAction.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build();
            results3.add(customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePreference(itemRequest));
        }
        var reply3 = results3.isEmpty() ? NormalizePreferenceResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumePolicyWorkflow", request.getPolicyId());
        return ResumePolicyWorkflowResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setCurrencyCode(request.getPolicy().getCurrencyCode())
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .setPolicyId(reply1.getClaim().getSla().getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists policies a page at a time.
     */
    public ListPolicyWorkflowsResponse listPolicyWorkflows(ListPolicyWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPolicyWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "ListPolicyWorkflows");

        var outbound0 = MapUnderwritingImportColumnsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(MapUnderwritingImportColumnsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.UnderwritingImportService");
        var serviceName0 = "acme.insurance.v1" + "." + "UnderwritingImportService";
        var methodName0 = "Map" + "UnderwritingImportColumns";
        var descriptor0 = MethodDescriptor.<MapUnderwritingImportColumnsRequest, MapUnderwritingImportColumnsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(MapUnderwritingImportColumnsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(MapUnderwritingImportColumnsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ExplainPreferenceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPreferenceDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getUnderwriting().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ExplainPreferenceDecisionResponse fetch(ExplainPreferenceDecisionRequest input) {
                return customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPreferenceDecision(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPolicyWorkflows", request.getPage().getPageToken());
        return ListPolicyWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Signals a policy.
     */
    public SignalPolicyWorkflowResponse signalPolicyWorkflow(SignalPolicyWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalPolicyWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = RpcSupport.unpack(Any.pack(PushUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushUnderwritingRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build()), PushUnderwritingRequest.class);
        var reply0 = insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushUnderwriting(outbound0);

        var futures1 = request.getPolicy().getLimitsList().stream()
                .map(element -> RecordLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .addLimits(LedgerAccountLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .setName(element.getName())
                                .setPeriod(element.getPeriod())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordLedgerAccountVersion(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? RecordLedgerAccountVersionResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalPolicyWorkflow", request.getPolicy().getPolicyId());
        return SignalPolicyWorkflowResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply0.getUnderwriting().getAssignee().getDisplayName())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .setStatus(request.getPolicy().getStatus())
                        .build())
                .build();
    }
}
