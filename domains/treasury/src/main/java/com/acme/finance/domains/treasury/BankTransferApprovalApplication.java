package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyDiscountAction;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityRequest;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityResponse;
import com.acme.authorization.v1.FindAccessPoliciesRequest;
import com.acme.authorization.v1.FindAccessPoliciesResponse;
import com.acme.authorization.v1.ScoreAccessPolicyRequest;
import com.acme.authorization.v1.ScoreAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.configuration.v1.ConfigSetServiceGrpc;
import com.acme.configuration.v1.SearchConfigSetsRequest;
import com.acme.configuration.v1.SearchConfigSetsResponse;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportAttachment;
import com.acme.expenses.v1.ExpenseReportWorkflowServiceGrpc;
import com.acme.expenses.v1.PauseExpenseReportWorkflowRequest;
import com.acme.expenses.v1.PauseExpenseReportWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCardSettlement;
import com.acme.payroll.v1.PayStubWorkflowServiceGrpc;
import com.acme.payroll.v1.SignalPayStubWorkflowRequest;
import com.acme.payroll.v1.SignalPayStubWorkflowResponse;
import com.acme.treasury.v1.ApproveBankTransferRequest;
import com.acme.treasury.v1.ApproveBankTransferResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferLifecycleServiceGrpc;
import com.acme.treasury.v1.BankTransferLine;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionApprovalServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CommentBankTransferApprovalRequest;
import com.acme.treasury.v1.CommentBankTransferApprovalResponse;
import com.acme.treasury.v1.DelegateBankTransferApprovalRequest;
import com.acme.treasury.v1.DelegateBankTransferApprovalResponse;
import com.acme.treasury.v1.DelegateCashPositionApprovalRequest;
import com.acme.treasury.v1.DelegateCashPositionApprovalResponse;
import com.acme.treasury.v1.EscalateBankTransferRequest;
import com.acme.treasury.v1.EscalateBankTransferResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.ListPendingBankTransfersRequest;
import com.acme.treasury.v1.ListPendingBankTransfersResponse;
import com.acme.treasury.v1.ReinstateBankTransferRequest;
import com.acme.treasury.v1.ReinstateBankTransferResponse;
import com.acme.treasury.v1.SubmitBankTransferRequest;
import com.acme.treasury.v1.SubmitBankTransferResponse;
import com.acme.treasury.v1.SyncLiquidityForecastsRequest;
import com.acme.treasury.v1.SyncLiquidityForecastsResponse;
import com.acme.yards.v1.SimulateYardRequest;
import com.acme.yards.v1.SimulateYardResponse;
import com.acme.yards.v1.Yard;
import com.acme.yards.v1.YardCalculationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Routes bank transfers through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BankTransferApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(BankTransferApprovalApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ConfigSetServiceGrpc.ConfigSetServiceBlockingStub configurationConfigSetServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final ExpenseReportWorkflowServiceGrpc.ExpenseReportWorkflowServiceBlockingStub expensesExpenseReportWorkflowServiceBlocking;

    private final PayStubWorkflowServiceGrpc.PayStubWorkflowServiceBlockingStub payrollPayStubWorkflowServiceBlocking;

    private final TreasuryRepository repository;

    private final BankTransferLifecycleServiceGrpc.BankTransferLifecycleServiceBlockingStub treasuryBankTransferLifecycleServiceBlocking;

    private final CashPositionApprovalServiceGrpc.CashPositionApprovalServiceBlockingStub treasuryCashPositionApprovalServiceBlocking;

    private final LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking;

    public BankTransferApprovalApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            ChannelRegistry channels,
            ConfigSetServiceGrpc.ConfigSetServiceBlockingStub configurationConfigSetServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            ExpenseReportWorkflowServiceGrpc.ExpenseReportWorkflowServiceBlockingStub expensesExpenseReportWorkflowServiceBlocking,
            PayStubWorkflowServiceGrpc.PayStubWorkflowServiceBlockingStub payrollPayStubWorkflowServiceBlocking,
            TreasuryRepository repository,
            BankTransferLifecycleServiceGrpc.BankTransferLifecycleServiceBlockingStub treasuryBankTransferLifecycleServiceBlocking,
            CashPositionApprovalServiceGrpc.CashPositionApprovalServiceBlockingStub treasuryCashPositionApprovalServiceBlocking,
            LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.channels = channels;
        this.configurationConfigSetServiceBlocking = configurationConfigSetServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.expensesExpenseReportWorkflowServiceBlocking = expensesExpenseReportWorkflowServiceBlocking;
        this.payrollPayStubWorkflowServiceBlocking = payrollPayStubWorkflowServiceBlocking;
        this.repository = repository;
        this.treasuryBankTransferLifecycleServiceBlocking = treasuryBankTransferLifecycleServiceBlocking;
        this.treasuryCashPositionApprovalServiceBlocking = treasuryCashPositionApprovalServiceBlocking;
        this.treasuryLiquidityForecastSyncServiceBlocking = treasuryLiquidityForecastSyncServiceBlocking;
    }

    /**
     * Submits a bank transfer.
     */
    public SubmitBankTransferResponse submitBankTransfer(SubmitBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = ScoreAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getBankTransfer().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreAccessPolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getBankTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ScoreAccessPolicyResponse fetch(ScoreAccessPolicyRequest input) {
                return authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAccessPolicy(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ReinstateBankTransferRequest.newBuilder()
                .setBankTransferId(request.getBankTransfer().getBankTransferId())
                .setContext(request.getContext())
                .setDetails(ReinstateBankTransferRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setEmail(request.getBankTransfer().getCounterparty().getEmail())
                                .setUserId(reply0.getAccessPolicy().getOwner().getUserId())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getAccessPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ReinstateBankTransferResponse fetch(ReinstateBankTransferRequest input) {
                return treasuryBankTransferLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reinstateBankTransfer(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = ScoreAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreAuditEventRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getBankTransfer().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<ScoreAuditEventRequest, ScoreAuditEventResponse>() {
            @Override
            public ScoreAuditEventResponse apply(ScoreAuditEventRequest input) {
                return auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAuditEvent(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitBankTransfer", request.getBankTransfer().getNumber());
        return SubmitBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .addLines(BankTransferLine.newBuilder()
                                .putAllAttributes(reply2.getAuditEvent().getAttributesMap())
                                .build())
                        .putAllMetadata(request.getBankTransfer().getMetadataMap())
                        .build())
                .setOutcome(SubmitBankTransferResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getAuditEvent().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Approves a bank transfer.
     */
    public ApproveBankTransferResponse approveBankTransfer(ApproveBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = StreamAuditEventNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream0 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventNotices(outbound0);
        var reply0 = StreamAuditEventNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = SearchConfigSetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchConfigSetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBankTransfer().getDueAt())
                        .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.configuration.v1.ConfigSetService/SearchConfigSets", outbound1);
        var reply1 = configurationConfigSetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchConfigSets(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveBankTransfer", request.getBankTransfer().getBankTransferId());
        return ApproveBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setDueAt(request.getBankTransfer().getDueAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates a bank transfer.
     */
    public EscalateBankTransferResponse escalateBankTransfer(EscalateBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = SimulateYardRequest.newBuilder()
                .setContext(request.getContext())
                .setYard(Yard.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getBankTransfer().getBillingAddress().getLinesList())
                                .setPostalCode(request.getBankTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.yards.v1.YardCalculationService");
        var selected0 = YardCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.simulateYard(outbound0);

        var outbound1 = FindAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getYard().getAudit().getCreatedAt())
                        .setRegion(reply0.getYard().getAddress().getRegion())
                        .build())
                .build();
        Function<FindAccessPoliciesRequest, FindAccessPoliciesResponse> operation1 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::findAccessPolicies;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateBankTransfer", request.getBankTransfer().getBankTransferId());
        return EscalateBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getYard().getAddress().getRecipient())
                                .build())
                        .setTotal(request.getBankTransfer().getTotal())
                        .build())
                .setOutcome(EscalateBankTransferResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists bank transfers a page at a time.
     */
    public ListPendingBankTransfersResponse listPendingBankTransfers(ListPendingBankTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingBankTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getContext().getTraceId(), "ListPendingBankTransfers");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingBankTransfers", request.getPage().getPageToken());
        return ListPendingBankTransfersResponse.getDefaultInstance();
    }

    /**
     * Delegates a bank transfer.
     */
    public DelegateBankTransferApprovalResponse delegateBankTransferApproval(DelegateBankTransferApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateBankTransferApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var results0 = new ArrayList<CheckAccessPolicyEligibilityResponse>();
        for (var element : request.getBankTransfer().getLinesList()) {
            CheckAccessPolicyEligibilityRequest itemRequest = CheckAccessPolicyEligibilityRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getBankTransfer().getAudit().getEtag())
                                .build())
                        .setDescription(element.getDescription())
                        .setDiscount(AccessPolicyDiscountAction.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAccessPolicyEligibility(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CheckAccessPolicyEligibilityResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(BrowseCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBankTransfer().getAudit().getCreatedAt())
                        .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build(), BrowseCustomersRequest.class);
        var reply1 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseCustomers(outbound1);

        var outbound2 = SignalPayStubWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalPayStubWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply0.getAccessPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAccessPolicy().getAudit().getCreatedBy())
                                .build())
                        .setCard(PayStubCardSettlement.newBuilder()
                                .setHolderName(switch (request.getBankTransfer().getSettlementCase()) {
                                    case CARD -> request.getBankTransfer().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(request.getBankTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = payrollPayStubWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalPayStubWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateBankTransferApproval", request.getBankTransfer().getBankTransferId());
        return DelegateBankTransferApprovalResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getPayStub().getAudit().getEtag())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setCounterparty(request.getBankTransfer().getCounterparty())
                        .setNumber(reply2.getPayStub().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Comments a bank transfer.
     */
    public CommentBankTransferApprovalResponse commentBankTransferApproval(CommentBankTransferApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentBankTransferApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = SyncLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncLiquidityForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getBankTransfer().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getBankTransfer().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        Function<SyncLiquidityForecastsRequest, SyncLiquidityForecastsResponse> operation0 = treasuryLiquidityForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::syncLiquidityForecasts;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = DelegateCashPositionApprovalRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBankTransfer().getAudit().getCreatedBy())
                                .build())
                        .setCard(CashPositionCardFunding.newBuilder()
                                .setNetwork(switch (reply0.getLiquidityForecast().getSettlementCase()) {
                                    case CARD -> reply0.getLiquidityForecast().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getBankTransfer().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = treasuryCashPositionApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateCashPositionApproval(outbound1);

        var results2 = new ArrayList<PauseExpenseReportWorkflowResponse>();
        for (var element : request.getBankTransfer().getAttachmentsList()) {
            PauseExpenseReportWorkflowRequest itemRequest = PauseExpenseReportWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .addAttachments(ExpenseReportAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .build())
                        .build())
                .build();
            results2.add(expensesExpenseReportWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseExpenseReportWorkflow(itemRequest));
        }
        var reply2 = results2.isEmpty() ? PauseExpenseReportWorkflowResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentBankTransferApproval", request.getBankTransfer().getBankTransferId());
        return CommentBankTransferApprovalResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCashPosition().getAudit().getCreatedAt())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getLiquidityForecast().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getCashPosition().getHolder().getDisplayName())
                                .build())
                        .setNotes(reply0.getLiquidityForecast().getNotes())
                        .setStatus(request.getBankTransfer().getStatus())
                        .build())
                .build();
    }
}
