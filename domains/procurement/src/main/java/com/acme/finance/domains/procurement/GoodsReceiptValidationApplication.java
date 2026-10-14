package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RecordAuditEventVersionRequest;
import com.acme.audit.v1.RecordAuditEventVersionResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CreateAccessPolicyRequest;
import com.acme.authorization.v1.CreateAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.EstimateAttestationRequest;
import com.acme.compliance.v1.EstimateAttestationResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.messaging.v1.ChannelRef;
import com.acme.messaging.v1.Conversation;
import com.acme.messaging.v1.ConversationCalculationServiceGrpc;
import com.acme.messaging.v1.RecalculateConversationsRequest;
import com.acme.messaging.v1.RecalculateConversationsResponse;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityRequest;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityResponse;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityResponse;
import com.acme.procurement.v1.GetGoodsReceiptRulesRequest;
import com.acme.procurement.v1.GetGoodsReceiptRulesResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptAttachment;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.LintGoodsReceiptsRequest;
import com.acme.procurement.v1.LintGoodsReceiptsResponse;
import com.acme.procurement.v1.NormalizeGoodsReceiptRequest;
import com.acme.procurement.v1.NormalizeGoodsReceiptResponse;
import com.acme.procurement.v1.NormalizePurchaseOrderRequest;
import com.acme.procurement.v1.NormalizePurchaseOrderResponse;
import com.acme.procurement.v1.PreviewGoodsReceiptRequest;
import com.acme.procurement.v1.PreviewGoodsReceiptResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderLine;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionBankSettlement;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.QuoteGoodsReceiptRequest;
import com.acme.procurement.v1.QuoteGoodsReceiptResponse;
import com.acme.procurement.v1.RecalculateGoodsReceiptsRequest;
import com.acme.procurement.v1.RecalculateGoodsReceiptsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.ScoreGoodsReceiptRequest;
import com.acme.procurement.v1.ScoreGoodsReceiptResponse;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import com.acme.procurement.v1.ValidateGoodsReceiptRequest;
import com.acme.procurement.v1.ValidateGoodsReceiptResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.WatchRiskRegistersRequest;
import com.acme.risk.v1.WatchRiskRegistersResponse;
import com.acme.venues.v1.CountVenuesRequest;
import com.acme.venues.v1.CountVenuesResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Checks goods receipts against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class GoodsReceiptValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(GoodsReceiptValidationApplication.class);

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final ConversationCalculationServiceGrpc.ConversationCalculationServiceBlockingStub messagingConversationCalculationServiceBlocking;

    private final GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking;

    private final PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking;

    private final PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceFutureStub procurementPurchaseOrderValidationServiceFuture;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final ProcurementRepository repository;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking;

    private final Executor rpcExecutor;

    public GoodsReceiptValidationApplication(
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ChannelRegistry channels,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            ConversationCalculationServiceGrpc.ConversationCalculationServiceBlockingStub messagingConversationCalculationServiceBlocking,
            GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking,
            PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking,
            PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceFutureStub procurementPurchaseOrderValidationServiceFuture,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            ProcurementRepository repository,
            RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.channels = channels;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.messagingConversationCalculationServiceBlocking = messagingConversationCalculationServiceBlocking;
        this.procurementGoodsReceiptCalculationServiceBlocking = procurementGoodsReceiptCalculationServiceBlocking;
        this.procurementPurchaseOrderCalculationServiceBlocking = procurementPurchaseOrderCalculationServiceBlocking;
        this.procurementPurchaseOrderValidationServiceFuture = procurementPurchaseOrderValidationServiceFuture;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterServiceBlocking = riskRiskRegisterServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates a goods receipt.
     */
    public ValidateGoodsReceiptResponse validateGoodsReceipt(ValidateGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var futures0 = request.getGoodsReceipt().getAttachmentsList().stream()
                .map(element -> RecalculateGoodsReceiptsRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .addAttachments(GoodsReceiptAttachment.newBuilder()
                                .setFileName(element.getFileName())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> procurementGoodsReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateGoodsReceipts(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? RecalculateGoodsReceiptsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = WatchRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchRiskRegistersRequest.Filter.newBuilder()
                        .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream1 = riskRiskRegisterServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchRiskRegisters(outbound1);
        var reply1 = WatchRiskRegistersResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return ValidateGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setNotes(request.getGoodsReceipt().getNotes())
                        .build())
                .build();
    }

    /**
     * Previews a goods receipt.
     */
    public PreviewGoodsReceiptResponse previewGoodsReceipt(PreviewGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = EstimateAttestationRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getGoodsReceipt().getAudit().getCreatedBy())
                                .setEtag(request.getGoodsReceipt().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateAttestationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getGoodsReceipt().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply0 = complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAttestation(outbound0);

        var outbound1 = RecordAuditEventVersionRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply0.getAttestation().getCounterparty().getDisplayName())
                                .setNumber(request.getGoodsReceipt().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecordAuditEventVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply1 = auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordAuditEventVersion(outbound1);

        var outbound2 = (RecalculateConversationsRequest.newBuilder()
                .setContext(request.getContext())
                .setConversation(Conversation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getGoodsReceipt().getAudit().getUpdatedBy())
                                .build())
                        .setCustomer(ChannelRef.newBuilder()
                                .setDisplayName(reply1.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .build())
                .setDetails(RecalculateConversationsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getGoodsReceipt().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = messagingConversationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateConversations(outbound2);

        var outbound3 = CountVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountVenuesRequest.Filter.newBuilder()
                        .setRegion(reply0.getAttestation().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.venues.v1.VenueQueryService");
        var serviceName3 = "acme.venues.v1" + "." + "VenueQueryService";
        var methodName3 = "Cou" + "ntVenues";
        var descriptor3 = MethodDescriptor.<CountVenuesRequest, CountVenuesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(CountVenuesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CountVenuesResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return PreviewGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getAttestation().getBillingAddress().getLinesList())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setTotal(request.getGoodsReceipt().getTotal())
                        .build())
                .setOutcome(PreviewGoodsReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Checks a goods receipt.
     */
    public CheckGoodsReceiptEligibilityResponse checkGoodsReceiptEligibility(CheckGoodsReceiptEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckGoodsReceiptEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = (ValidatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePurchaseRequisitionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setCard(PurchaseRequisitionCardSettlement.newBuilder()
                                .setNetwork(request.getGoodsReceipt().getCard().getNetwork())
                                .setToken(request.getGoodsReceipt().getCard().getToken())
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setNumber(request.getGoodsReceipt().getCounterparty().getNumber())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePurchaseRequisition(outbound0);

        var outbound1 = (CreateAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getGoodsReceipt().getAudit().getCreatedBy())
                                .setEtag(reply0.getPurchaseRequisition().getAudit().getEtag())
                                .setUpdatedAt(request.getGoodsReceipt().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getGoodsReceipt().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply1 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAccessPolicy(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckGoodsReceiptEligibility", request.getGoodsReceipt().getGoodsReceiptId());
        return CheckGoodsReceiptEligibilityResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getAccessPolicy().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getPurchaseRequisition().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getPurchaseRequisition().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(request.getGoodsReceipt().getNumber())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Normalizes a goods receipt.
     */
    public NormalizeGoodsReceiptResponse normalizeGoodsReceipt(NormalizeGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = CheckPurchaseRequisitionEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckPurchaseRequisitionEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBank(PurchaseRequisitionBankSettlement.newBuilder()
                                .setAccountHolder(request.getGoodsReceipt().getBank().getAccountHolder())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPurchaseRequisitionEligibility(value));

        var outbound1 = ExplainPolicyCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPolicyCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getGoodsReceipt().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getPurchaseRequisition().getAudit().getUpdatedBy())
                                .build())
                        .setCard(PolicyCardFunding.newBuilder()
                                .setNetwork(switch (request.getGoodsReceipt().getSettlementCase()) {
                                    case CARD -> request.getGoodsReceipt().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPolicyCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return NormalizeGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setIssuedAt(reply0.getPurchaseRequisition().getIssuedAt())
                        .setTotal(request.getGoodsReceipt().getTotal())
                        .build())
                .setOutcome(NormalizeGoodsReceiptResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Scores a goods receipt.
     */
    public ScoreGoodsReceiptResponse scoreGoodsReceipt(ScoreGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = NormalizePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(request.getGoodsReceipt().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var pending0 = procurementPurchaseOrderValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePurchaseOrder(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = SimulateRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateRfqRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPurchaseOrder().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        RpcPort<SimulateRfqRequest, SimulateRfqResponse> port1 = RpcPort.adapt(procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulateRfq);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = ExplainRiskScenarioDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getRfq().getRequester().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getRfq().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.risk.v1.RiskScenarioValidationService");
        var serviceName2 = "acme.risk.v1" + "." + "RiskScenarioValidationService";
        var methodName2 = "Exp" + "lainRiskScenarioDecision";
        var descriptor2 = MethodDescriptor.<ExplainRiskScenarioDecisionRequest, ExplainRiskScenarioDecisionResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainRiskScenarioDecisionRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainRiskScenarioDecisionResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return ScoreGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPurchaseOrder().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(request.getGoodsReceipt().getCounterparty())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints a goods receipt.
     */
    public LintGoodsReceiptsResponse lintGoodsReceipts(LintGoodsReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintGoodsReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = CalculatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getGoodsReceipt().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setCard(PolicyCardFunding.newBuilder()
                                .setHolderName(switch (request.getGoodsReceipt().getSettlementCase()) {
                                    case CARD -> request.getGoodsReceipt().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePolicy(outbound0);

        List<QuoteGoodsReceiptResponse> results1 = request.getGoodsReceipt().getAttachmentsList().stream()
                .map(element -> QuoteGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .addAttachments(GoodsReceiptAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(procurementGoodsReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteGoodsReceipt(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? QuoteGoodsReceiptResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintGoodsReceipts", request.getGoodsReceipt().getGoodsReceiptId());
        return LintGoodsReceiptsResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCounterparty(request.getGoodsReceipt().getCounterparty())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a goods receipt.
     */
    public GetGoodsReceiptRulesResponse getGoodsReceiptRules(GetGoodsReceiptRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetGoodsReceiptRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getGoodsReceiptId(), "GetGoodsReceiptRules");

        var outbound0 = SimulatePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setIssuedAt(request.getContext().getDeadline())
                        .addLines(PurchaseOrderLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build();
        Function<SimulatePurchaseOrderRequest, SimulatePurchaseOrderResponse> operation0 = procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulatePurchaseOrder;
        var reply0 = operation0.apply(outbound0);
        return GetGoodsReceiptRulesResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply0.getPurchaseOrder().getCounterparty().getNumber())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .build();
    }
}
