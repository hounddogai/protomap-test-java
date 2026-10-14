package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.contracts.v1.AdvanceContractWorkflowRequest;
import com.acme.contracts.v1.AdvanceContractWorkflowResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractCardSettlement;
import com.acme.contracts.v1.ContractWorkflowServiceGrpc;
import com.acme.contracts.v1.RenewalRef;
import com.acme.customers.v1.CalculateConsentRecordRequest;
import com.acme.customers.v1.CalculateConsentRecordResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CountCustomersRequest;
import com.acme.customers.v1.CountCustomersResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.PreviewConsentRecordRequest;
import com.acme.customers.v1.PreviewConsentRecordResponse;
import com.acme.customers.v1.PreviewCustomerRequest;
import com.acme.customers.v1.PreviewCustomerResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ListCoverageReportsRequest;
import com.acme.insurance.v1.ListCoverageReportsResponse;
import com.acme.returns.v1.CreateReturnRequestRequest;
import com.acme.returns.v1.CreateReturnRequestResponse;
import com.acme.returns.v1.ReturnRequest;
import com.acme.returns.v1.ReturnRequestCardSettlement;
import com.acme.returns.v1.ReturnRequestServiceGrpc;
import com.acme.treasury.v1.ApproveCashPositionRequest;
import com.acme.treasury.v1.ApproveCashPositionResponse;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CommentCashPositionApprovalRequest;
import com.acme.treasury.v1.CommentCashPositionApprovalResponse;
import com.acme.treasury.v1.CountFxHedgeTotalsRequest;
import com.acme.treasury.v1.CountFxHedgeTotalsResponse;
import com.acme.treasury.v1.DelegateCashPositionApprovalRequest;
import com.acme.treasury.v1.DelegateCashPositionApprovalResponse;
import com.acme.treasury.v1.DeleteCashPositionRequest;
import com.acme.treasury.v1.DeleteCashPositionResponse;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.FxHedgeReportingServiceGrpc;
import com.acme.treasury.v1.GetCashPositionApprovalStatusRequest;
import com.acme.treasury.v1.GetCashPositionApprovalStatusResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastBankSettlement;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.ListPendingCashPositionsRequest;
import com.acme.treasury.v1.ListPendingCashPositionsResponse;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsRequest;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsResponse;
import com.acme.treasury.v1.RecallCashPositionRequest;
import com.acme.treasury.v1.RecallCashPositionResponse;
import com.acme.treasury.v1.ScheduleBankTransferReportRequest;
import com.acme.treasury.v1.ScheduleBankTransferReportResponse;
import com.acme.treasury.v1.SubmitCashPositionRequest;
import com.acme.treasury.v1.SubmitCashPositionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Routes cash positions through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class CashPositionApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(CashPositionApprovalApplication.class);

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking;

    private final ContractWorkflowServiceGrpc.ContractWorkflowServiceBlockingStub contractsContractWorkflowServiceBlocking;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final CashPositionApprovalRequestMapper mapper;

    private final TreasuryRepository repository;

    private final ReturnRequestServiceGrpc.ReturnRequestServiceBlockingStub returnsReturnRequestServiceBlocking;

    private final BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking;

    private final CashPositionServiceGrpc.CashPositionServiceBlockingStub treasuryCashPositionServiceBlocking;

    private final FxHedgeReportingServiceGrpc.FxHedgeReportingServiceBlockingStub treasuryFxHedgeReportingServiceBlocking;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking;

    public CashPositionApprovalApplication(
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            ChannelRegistry channels,
            AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking,
            ContractWorkflowServiceGrpc.ContractWorkflowServiceBlockingStub contractsContractWorkflowServiceBlocking,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            CashPositionApprovalRequestMapper mapper,
            TreasuryRepository repository,
            ReturnRequestServiceGrpc.ReturnRequestServiceBlockingStub returnsReturnRequestServiceBlocking,
            BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking,
            CashPositionServiceGrpc.CashPositionServiceBlockingStub treasuryCashPositionServiceBlocking,
            FxHedgeReportingServiceGrpc.FxHedgeReportingServiceBlockingStub treasuryFxHedgeReportingServiceBlocking,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking) {
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.channels = channels;
        this.complianceAttestationApprovalServiceBlocking = complianceAttestationApprovalServiceBlocking;
        this.contractsContractWorkflowServiceBlocking = contractsContractWorkflowServiceBlocking;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.returnsReturnRequestServiceBlocking = returnsReturnRequestServiceBlocking;
        this.treasuryBankTransferReportingServiceBlocking = treasuryBankTransferReportingServiceBlocking;
        this.treasuryCashPositionServiceBlocking = treasuryCashPositionServiceBlocking;
        this.treasuryFxHedgeReportingServiceBlocking = treasuryFxHedgeReportingServiceBlocking;
        this.treasuryLiquidityForecastImportServiceBlocking = treasuryLiquidityForecastImportServiceBlocking;
    }

    /**
     * Submits a cash position.
     */
    public SubmitCashPositionResponse submitCashPosition(SubmitCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = ScheduleBankTransferReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(treasuryBankTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleBankTransferReport(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitCashPosition", request.getCashPosition().getCashPositionId());
        return SubmitCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .setOpenedAt(request.getCashPosition().getOpenedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a cash position.
     */
    public ApproveCashPositionResponse approveCashPosition(ApproveCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = DeleteCashPositionRequest.newBuilder()
                .setCashPositionId(request.getCashPositionId())
                .setContext(request.getContext())
                .setEtag(request.getCashPosition().getAudit().getEtag())
                .setReason(request.getReason())
                .build();
        var reply0 = treasuryCashPositionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deleteCashPosition(outbound0);

        var outbound1 = CountFxHedgeTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountFxHedgeTotalsRequest.Filter.newBuilder()
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(treasuryFxHedgeReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countFxHedgeTotals(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveCashPosition", request.getCashPosition().getCashPositionId());
        return ApproveCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .setOpenedAt(request.getCashPosition().getOpenedAt())
                        .build())
                .setOutcome(ApproveCashPositionResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists cash positions a page at a time.
     */
    public ListPendingCashPositionsResponse listPendingCashPositions(ListPendingCashPositionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingCashPositions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getContext().getTraceId(), "ListPendingCashPositions");

        var outbound0 = CommentAttestationApprovalRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(complianceAttestationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentAttestationApproval(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingCashPositions", request.getPage().getPageToken());
        return ListPendingCashPositionsResponse.newBuilder()
                .addCashPositions(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAttestation().getAudit().getCreatedBy())
                                .build())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setDisplayName(reply0.getAttestation().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Delegates a cash position.
     */
    public DelegateCashPositionApprovalResponse delegateCashPositionApproval(DelegateCashPositionApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateCashPositionApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = CalculateConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getCashPosition().getHolder().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<CalculateConsentRecordRequest, CalculateConsentRecordResponse>() {
            @Override
            public CalculateConsentRecordResponse apply(CalculateConsentRecordRequest input) {
                return customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateConsentRecord(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = AdvanceContractWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setContract(Contract.newBuilder()
                        .setCard(ContractCardSettlement.newBuilder()
                                .setHolderName(switch (request.getCashPosition().getFundingCase()) {
                                    case CARD -> request.getCashPosition().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setDisplayName(reply0.getConsentRecord().getSubject().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = contractsContractWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceContractWorkflow(outbound1);

        var outbound2 = ListRetentionPolicyVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicyVersionsRequest.Filter.newBuilder()
                        .setRegion(reply1.getContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicyVersions(value));
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateCashPositionApproval", request.getCashPosition().getCashPositionId());
        return DelegateCashPositionApprovalResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getContract().getAudit().getCreatedAt())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setOpenedAt(request.getCashPosition().getOpenedAt())
                        .build())
                .setOutcome(DelegateCashPositionApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getConsentRecord().getCorrelationId())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalls a cash position.
     */
    public RecallCashPositionResponse recallCashPosition(RecallCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = (PreviewCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCashPosition().getAudit().getCreatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getCashPosition().getContact().getName())
                                .build())
                        .build())
                .setDetails(PreviewCustomerRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getCashPosition().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCustomer(outbound0);

        var outbound1 = MapLiquidityForecastImportColumnsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(MapLiquidityForecastImportColumnsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getCashPosition().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getCashPosition().getAudit().getUpdatedBy())
                                .build())
                        .setBank(LiquidityForecastBankSettlement.newBuilder()
                                .setBic(switch (request.getCashPosition().getFundingCase()) {
                                    case BANK -> request.getCashPosition().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).mapLiquidityForecastImportColumns(outbound1);

        var outbound2 = mapper.recallCashPositionStage2(request, reply0);
        var reply2 = customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewConsentRecord(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallCashPosition", request.getCashPosition().getCashPositionId());
        return RecallCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .setStatus(request.getCashPosition().getStatus())
                        .build())
                .setOutcome(RecallCashPositionResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a cash position.
     */
    public GetCashPositionApprovalStatusResponse getCashPositionApprovalStatus(GetCashPositionApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCashPositionApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "GetCashPositionApprovalStatus");

        var outbound0 = mapper.getCashPositionApprovalStatusStage0(request);
        var reply0 = auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeAuditTrailUpdates(outbound0);
        return GetCashPositionApprovalStatusResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Comments a cash position.
     */
    public CommentCashPositionApprovalResponse commentCashPositionApproval(CommentCashPositionApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentCashPositionApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = CreateReturnRequestRequest.newBuilder()
                .setContext(request.getContext())
                .setReturnRequest(ReturnRequest.newBuilder()
                        .setCard(ReturnRequestCardSettlement.newBuilder()
                                .setLast4(switch (request.getCashPosition().getFundingCase()) {
                                    case CARD -> request.getCashPosition().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = returnsReturnRequestServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createReturnRequest(outbound0);

        var outbound1 = mapper.commentCashPositionApprovalStage1(request, reply0);
        var reply1 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countCustomers(outbound1);

        var outbound2 = SubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getReturnRequest().getCounterparty().getEmail())
                                .setName(request.getCashPosition().getContact().getName())
                                .setPhone(request.getCashPosition().getContact().getPhone())
                                .setPreferredChannel(request.getCashPosition().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<SubscribeRetentionPolicyUpdatesRequest, SubscribeRetentionPolicyUpdatesResponse>() {
            @Override
            public SubscribeRetentionPolicyUpdatesResponse apply(SubscribeRetentionPolicyUpdatesRequest input) {
                return auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeRetentionPolicyUpdates(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var outbound3 = ListCoverageReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCoverageReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getReturnRequest().getAudit().getUpdatedAt())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .setOwnerId(reply2.getRetentionPolicy().getOwner().getUserId())
                        .setRegion(reply0.getReturnRequest().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.insurance.v1.CoverageReportingService");
        var serviceName3 = "acme.insurance.v1" + "." + "CoverageReportingService";
        var methodName3 = "Lis" + "tCoverageReports";
        var descriptor3 = MethodDescriptor.<ListCoverageReportsRequest, ListCoverageReportsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(ListCoverageReportsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListCoverageReportsResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentCashPositionApproval", request.getCashPosition().getCashPositionId());
        return CommentCashPositionApprovalResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setDisplayName(reply0.getReturnRequest().getCounterparty().getDisplayName())
                                .setNumber(reply0.getReturnRequest().getNumber())
                                .build())
                        .build())
                .setOutcome(CommentCashPositionApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply2.getOutcome().getMessage())
                        .addAllNextSteps(reply2.getOutcome().getNextStepsList())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
