package com.acme.finance.domains.procurement;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.concessions.v1.BatchGetConcessionStandsRequest;
import com.acme.concessions.v1.BatchGetConcessionStandsResponse;
import com.acme.concessions.v1.ConcessionStandServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.SearchUsersRequest;
import com.acme.identity.v1.SearchUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.notifications.v1.NormalizeNotificationRequest;
import com.acme.notifications.v1.NormalizeNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.procurement.v1.CalculateRfqRequest;
import com.acme.procurement.v1.CalculateRfqResponse;
import com.acme.procurement.v1.CheckRfqEligibilityRequest;
import com.acme.procurement.v1.CheckRfqEligibilityResponse;
import com.acme.procurement.v1.EstimateRfqRequest;
import com.acme.procurement.v1.EstimateRfqResponse;
import com.acme.procurement.v1.ExplainRfqDecisionRequest;
import com.acme.procurement.v1.ExplainRfqDecisionResponse;
import com.acme.procurement.v1.GetRfqRulesRequest;
import com.acme.procurement.v1.GetRfqRulesResponse;
import com.acme.procurement.v1.LintRfqsRequest;
import com.acme.procurement.v1.LintRfqsResponse;
import com.acme.procurement.v1.NormalizeRfqRequest;
import com.acme.procurement.v1.NormalizeRfqResponse;
import com.acme.procurement.v1.PreviewRfqRequest;
import com.acme.procurement.v1.PreviewRfqResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.QuotePurchaseRequisitionRequest;
import com.acme.procurement.v1.QuotePurchaseRequisitionResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.ScoreRfqRequest;
import com.acme.procurement.v1.ScoreRfqResponse;
import com.acme.procurement.v1.ValidateRfqRequest;
import com.acme.procurement.v1.ValidateRfqResponse;
import com.acme.telehealth.v1.VideoVisitServiceGrpc;
import com.acme.telehealth.v1.WatchVideoVisitsRequest;
import com.acme.telehealth.v1.WatchVideoVisitsResponse;
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
 * Checks rfqs against the rules before anything acts on them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class RfqValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(RfqValidationApplication.class);

    private final ConcessionStandServiceGrpc.ConcessionStandServiceBlockingStub concessionsConcessionStandServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    private final VideoVisitServiceGrpc.VideoVisitServiceBlockingStub telehealthVideoVisitServiceBlocking;

    public RfqValidationApplication(
            ConcessionStandServiceGrpc.ConcessionStandServiceBlockingStub concessionsConcessionStandServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VideoVisitServiceGrpc.VideoVisitServiceBlockingStub telehealthVideoVisitServiceBlocking) {
        this.concessionsConcessionStandServiceBlocking = concessionsConcessionStandServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.procurementPurchaseOrderCalculationServiceBlocking = procurementPurchaseOrderCalculationServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceBlocking = procurementPurchaseRequisitionCalculationServiceBlocking;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.telehealthVideoVisitServiceBlocking = telehealthVideoVisitServiceBlocking;
    }

    /**
     * Validates a rfq.
     */
    public ValidateRfqResponse validateRfq(ValidateRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        List<CalculateRfqResponse> results0 = request.getRfq().getCommentsList().stream()
                .map(element -> CalculateRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .addComments(RfqComment.newBuilder()
                                .setCommentId(element.getCommentId())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateRfq(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CalculateRfqResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateRfq", request.getRfq().getRfqId());
        return ValidateRfqResponse.newBuilder()
                .setRfq(Rfq.newBuilder()
                        .setRequester(request.getRfq().getRequester())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Previews a rfq.
     */
    public PreviewRfqResponse previewRfq(PreviewRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = QuotePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRfq().getAudit().getCreatedBy())
                                .setEtag(request.getRfq().getAudit().getEtag())
                                .setUpdatedBy(request.getRfq().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setDisplayName(request.getRfq().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePurchaseRequisition(outbound0), rpcExecutor);

        var outbound1 = NormalizeNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeNotificationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getRfq().getAssignee().getEmail())
                                .build())
                        .build())
                .setNotification(Notification.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeNotification(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewRfq", request.getRfq().getRfqId());
        return PreviewRfqResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .putAllCustomFields(request.getRfq().getCustomFieldsMap())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getNotification().getAssignee().getEmail())
                                .setPhone(reply1.getNotification().getRequester().getPhone())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Checks a rfq.
     */
    public CheckRfqEligibilityResponse checkRfqEligibility(CheckRfqEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckRfqEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var futures0 = request.getRfq().getCommentsList().stream()
                .map(element -> QuotePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setCreatedBy(request.getRfq().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setDisplayName(request.getRfq().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePurchaseRequisition(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? QuotePurchaseRequisitionResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckRfqEligibility", request.getRfq().getRfqId());
        return CheckRfqEligibilityResponse.newBuilder()
                .setRfq(Rfq.newBuilder()
                        .putAllCustomFields(request.getRfq().getCustomFieldsMap())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Normalizes a rfq.
     */
    public NormalizeRfqResponse normalizeRfq(NormalizeRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = ListLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRfq().getAudit().getCreatedAt())
                        .setOwnerId(request.getRfq().getSla().getPolicyId())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccounts(outbound0));
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeRfq", request.getRfq().getRfqId());
        return NormalizeRfqResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setPriority(request.getRfq().getPriority())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Explains a rfq.
     */
    public ExplainRfqDecisionResponse explainRfqDecision(ExplainRfqDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainRfqDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = EstimateRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getRfq().getRequester().getEmail())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(switch (request.getRfq().getOriginCase()) {
                                    case CUSTOMER -> request.getRfq().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .setSla(RfqSla.newBuilder()
                                .setDueAt(request.getRfq().getSla().getDueAt())
                                .build())
                        .build())
                .build();
        var reply0 = procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateRfq(outbound0);

        var outbound1 = QuotePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRfq().getAudit().getEtag())
                                .setUpdatedAt(reply0.getRfq().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getRfq().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<QuotePurchaseOrderRequest, QuotePurchaseOrderResponse>() {
            @Override
            public QuotePurchaseOrderResponse apply(QuotePurchaseOrderRequest input) {
                return procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePurchaseOrder(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainRfqDecision", request.getRfq().getRfqId());
        return ExplainRfqDecisionResponse.newBuilder()
                .setOutcome(ExplainRfqDecisionResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getPurchaseOrder().getCounterparty().getDisplayName())
                                .build())
                        .putAllCustomFields(request.getRfq().getCustomFieldsMap())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply0.getRfq().getRequester().getPhone())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Scores a rfq.
     */
    public ScoreRfqResponse scoreRfq(ScoreRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = WatchVideoVisitsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getRfq().getCustomer().getEmail())
                        .setName(request.getRfq().getRequester().getName())
                        .setPhone(request.getRfq().getRequester().getPhone())
                        .build())
                .build();
        var stream0 = telehealthVideoVisitServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchVideoVisits(outbound0);
        var reply0 = WatchVideoVisitsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreRfq", request.getRfq().getRfqId());
        return ScoreRfqResponse.newBuilder()
                .setRfq(Rfq.newBuilder()
                        .setPriority(request.getRfq().getPriority())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Lints a rfq.
     */
    public LintRfqsResponse lintRfqs(LintRfqsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintRfqs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = BatchGetConcessionStandsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> concessionsConcessionStandServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetConcessionStands(value)).orElseThrow();

        var builder1 = SearchUsersRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(SearchUsersRequest.Filter.newBuilder()
                .setCreatedAfter(request.getRfq().getSla().getDueAt())
                .setOwnerId(request.getRfq().getAssignee().getUserId())
                .build());
        var outbound1 = builder1.build();
        var reply1 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchUsers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintRfqs", request.getRfq().getRfqId());
        return LintRfqsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setRfqId(request.getRfqId())
                        .setStatus(request.getRfq().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a rfq.
     */
    public GetRfqRulesResponse getRfqRules(GetRfqRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRfqRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "GetRfqRules");

        var outbound0 = JsonCodec.roundTrip(QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build(), QuotePolicyRequest.class);
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(outbound0);
        return GetRfqRulesResponse.newBuilder()
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }
}
