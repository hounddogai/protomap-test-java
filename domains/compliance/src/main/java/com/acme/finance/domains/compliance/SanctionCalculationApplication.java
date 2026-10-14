package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.SummarizeAuditTrailsRequest;
import com.acme.audit.v1.SummarizeAuditTrailsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CalculateSanctionRequest;
import com.acme.compliance.v1.CalculateSanctionResponse;
import com.acme.compliance.v1.CompareSanctionOptionsRequest;
import com.acme.compliance.v1.CompareSanctionOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.EstimateSanctionRequest;
import com.acme.compliance.v1.EstimateSanctionResponse;
import com.acme.compliance.v1.ExplainSanctionCalculationRequest;
import com.acme.compliance.v1.ExplainSanctionCalculationResponse;
import com.acme.compliance.v1.GetSanctionRatesRequest;
import com.acme.compliance.v1.GetSanctionRatesResponse;
import com.acme.compliance.v1.NormalizeControlRequest;
import com.acme.compliance.v1.NormalizeControlResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SimulateSanctionRequest;
import com.acme.compliance.v1.SimulateSanctionResponse;
import com.acme.configuration.v1.CompareConfigSetOptionsRequest;
import com.acme.configuration.v1.CompareConfigSetOptionsResponse;
import com.acme.configuration.v1.ConfigSet;
import com.acme.configuration.v1.ConfigSetCalculationServiceGrpc;
import com.acme.configuration.v1.ExplainConfigSetCalculationRequest;
import com.acme.configuration.v1.ExplainConfigSetCalculationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.messaging.v1.ConversationQueryServiceGrpc;
import com.acme.messaging.v1.ResolveConversationsRequest;
import com.acme.messaging.v1.ResolveConversationsResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.RiskScenario;
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
 * Computes the figures of a sanction, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class SanctionCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(SanctionCalculationApplication.class);

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking;

    private final ConfigSetCalculationServiceGrpc.ConfigSetCalculationServiceBlockingStub configurationConfigSetCalculationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final ConversationQueryServiceGrpc.ConversationQueryServiceBlockingStub messagingConversationQueryServiceBlocking;

    private final ComplianceRepository repository;

    private final Executor rpcExecutor;

    public SanctionCalculationApplication(
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking,
            ConfigSetCalculationServiceGrpc.ConfigSetCalculationServiceBlockingStub configurationConfigSetCalculationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            ConversationQueryServiceGrpc.ConversationQueryServiceBlockingStub messagingConversationQueryServiceBlocking,
            ComplianceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.auditAuditTrailQueryServiceBlocking = auditAuditTrailQueryServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceControlValidationServiceBlocking = complianceControlValidationServiceBlocking;
        this.configurationConfigSetCalculationServiceBlocking = configurationConfigSetCalculationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.messagingConversationQueryServiceBlocking = messagingConversationQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a sanction.
     */
    public CalculateSanctionResponse calculateSanction(CalculateSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = ExplainConfigSetCalculationRequest.newBuilder()
                .setConfigSet(ConfigSet.newBuilder()
                        .addChildren(ConfigSet.newBuilder()
                                .addChildren(ConfigSet.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getDetails().getDestination().getRegion())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> configurationConfigSetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainConfigSetCalculation(outbound0), rpcExecutor);

        var outbound1 = ResolveConversationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> messagingConversationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveConversations(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = JsonCodec.roundTrip(NormalizeControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getConfigSet().getAudit().getCreatedAt())
                                .setEtag(request.getSanction().getAudit().getEtag())
                                .setUpdatedBy(request.getSanction().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), NormalizeControlRequest.class);
        var reply2 = complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeControl(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateSanction", request.getSanction().getSanctionId());
        return CalculateSanctionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .putAllAttributes(request.getSanction().getAttributesMap())
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .addChildren(Sanction.newBuilder()
                                                .setName(reply2.getControl().getName())
                                                .setParentId(reply0.getConfigSet().getParentId())
                                                .build())
                                        .build())
                                .build())
                        .setParentId(reply0.getConfigSet().getParentId())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Estimates a sanction.
     */
    public EstimateSanctionResponse estimateSanction(EstimateSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = ExplainPolicyCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSanction().getAudit().getCreatedAt())
                                .setEtag(request.getSanction().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSanction().getManager().getEmail())
                                .build())
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPolicyCalculation(value)).orElseThrow();

        var outbound1 = BeanCopies.copy(UpdateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getSanction().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getSanction().getLocation().getLinesList())
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build(), UpdateUserRequest.class);
        var reply1 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateUser(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateSanction", request.getSanction().getSanctionId());
        return EstimateSanctionResponse.newBuilder()
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(reply1.getUser().getPrimaryAddress().getCountryCode())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .setLocation(request.getSanction().getLocation())
                        .setManager(UserRef.newBuilder()
                                .setUserId(reply1.getUser().getUserId())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Compares a sanction.
     */
    public CompareSanctionOptionsResponse compareSanctionOptions(CompareSanctionOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareSanctionOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = CalculateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(request.getSanction().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateComplianceCase(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = RecordLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getComplianceCase().getAudit().getEtag())
                                .setUpdatedAt(reply0.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setNumber(switch (reply0.getComplianceCase().getOriginCase()) {
                                    case CUSTOMER -> reply0.getComplianceCase().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordLedgerAccountVersion(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareSanctionOptions", request.getSanction().getSanctionId());
        return CompareSanctionOptionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .putAllAttributes(request.getSanction().getAttributesMap())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(reply0.getComplianceCase().getAudit().getUpdatedAt())
                                                .setUpdatedBy(reply1.getLedgerAccount().getAudit().getUpdatedBy())
                                                .build())
                                        .build())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Simulates a sanction.
     */
    public SimulateSanctionResponse simulateSanction(SimulateSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var builder0 = SummarizeAuditTrailsRequest.newBuilder();
        builder0.putAllAnnotations(request.getAnnotationsMap());
        builder0.setContext(request.getContext());
        builder0.setDetails(SummarizeAuditTrailsRequest.Details.newBuilder()
                .putAllFilters(request.getSanction().getAttributesMap())
                .build());
        builder0.setReason(request.getReason());
        var outbound0 = builder0.build();
        var reply0 = auditAuditTrailQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeAuditTrails(outbound0);

        var outbound1 = ListAuditTrailExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailExportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSanction().getAudit().getUpdatedAt())
                        .setRegion(request.getSanction().getLocation().getRegion())
                        .build())
                .build();
        var adapter1 = new Function<ListAuditTrailExportsRequest, ListAuditTrailExportsResponse>() {
            @Override
            public ListAuditTrailExportsResponse apply(ListAuditTrailExportsRequest input) {
                return auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailExports(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = CalculateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getSanction().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateComplianceCaseRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getSanction().getLocation().getPostalCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var reply2 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateComplianceCase(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateSanction", request.getSanction().getSanctionId());
        return SimulateSanctionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(reply2.getComplianceCase().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .setDepth(request.getSanction().getDepth())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Explains a sanction.
     */
    public ExplainSanctionCalculationResponse explainSanctionCalculation(ExplainSanctionCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainSanctionCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = ExplainRiskScenarioDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainRiskScenarioDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSanction().getManager().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getSanction().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.RiskScenarioValidationService");
        var serviceName0 = "acme.risk.v1" + "." + "RiskScenarioValidationService";
        var methodName0 = "Exp" + "lainRiskScenarioDecision";
        var descriptor0 = MethodDescriptor.<ExplainRiskScenarioDecisionRequest, ExplainRiskScenarioDecisionResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainRiskScenarioDecisionRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainRiskScenarioDecisionResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = CalculateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getSanction().getManager().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<CalculateComplianceCaseRequest, CalculateComplianceCaseResponse> port1 = RpcPort.adapt(complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::calculateComplianceCase);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = CompareConfigSetOptionsRequest.newBuilder()
                .setConfigSet(ConfigSet.newBuilder()
                        .addChildren(ConfigSet.newBuilder()
                                .addChildren(ConfigSet.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(reply0.getRiskScenario().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = configurationConfigSetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareConfigSetOptions(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainSanctionCalculation", request.getSanction().getSanctionId());
        return ExplainSanctionCalculationResponse.newBuilder()
                .setOutcome(ExplainSanctionCalculationResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .putAllAttributes(request.getSanction().getAttributesMap())
                        .addChildren(Sanction.newBuilder()
                                .setManager(UserRef.newBuilder()
                                        .setEmail(reply1.getComplianceCase().getAssignee().getEmail())
                                        .build())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply2.getConfigSet().getLocation().getLocality())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Returns a sanction.
     */
    public GetSanctionRatesResponse getSanctionRates(GetSanctionRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetSanctionRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "GetSanctionRates");

        var outbound0 = JsonCodec.roundTrip(PreviewMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewMandateRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setIssuedAt(request.getContext().getDeadline())
                        .addLines(MandateLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build(), PreviewMandateRequest.class);
        var reply0 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewMandate(outbound0);
        return GetSanctionRatesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(reply0.getMandate().getBillingAddress().getCountryCode())
                                                .build())
                                        .build())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }
}
