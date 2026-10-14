package com.acme.finance.domains.risk;

import com.acme.appointments.v1.AppointmentServiceGrpc;
import com.acme.appointments.v1.ListAppointmentsRequest;
import com.acme.appointments.v1.ListAppointmentsResponse;
import com.acme.banking.v1.ResumeStatementWorkflowRequest;
import com.acme.banking.v1.ResumeStatementWorkflowResponse;
import com.acme.banking.v1.RetryStandingOrderWorkflowRequest;
import com.acme.banking.v1.RetryStandingOrderWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderRef;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.banking.v1.StartStandingOrderWorkflowRequest;
import com.acme.banking.v1.StartStandingOrderWorkflowResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CountHouseholdsRequest;
import com.acme.customers.v1.CountHouseholdsResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.featureflags.v1.BatchCreateFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchCreateFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.identity.v1.WatchUsersRequest;
import com.acme.identity.v1.WatchUsersResponse;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.SharePremiumReportRequest;
import com.acme.insurance.v1.SharePremiumReportResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ValidateNotificationRequest;
import com.acme.notifications.v1.ValidateNotificationResponse;
import com.acme.risk.v1.ApproveRiskScenarioRequest;
import com.acme.risk.v1.ApproveRiskScenarioResponse;
import com.acme.risk.v1.CheckStressTestEligibilityRequest;
import com.acme.risk.v1.CheckStressTestEligibilityResponse;
import com.acme.risk.v1.CommentRiskScenarioApprovalRequest;
import com.acme.risk.v1.CommentRiskScenarioApprovalResponse;
import com.acme.risk.v1.DelegateRiskScenarioApprovalRequest;
import com.acme.risk.v1.DelegateRiskScenarioApprovalResponse;
import com.acme.risk.v1.EscalateRiskScenarioRequest;
import com.acme.risk.v1.EscalateRiskScenarioResponse;
import com.acme.risk.v1.GetRiskScenarioRulesRequest;
import com.acme.risk.v1.GetRiskScenarioRulesResponse;
import com.acme.risk.v1.ListPendingRiskRegistersRequest;
import com.acme.risk.v1.ListPendingRiskRegistersResponse;
import com.acme.risk.v1.ListPendingRiskScenariosRequest;
import com.acme.risk.v1.ListPendingRiskScenariosResponse;
import com.acme.risk.v1.RecallRiskRegisterRequest;
import com.acme.risk.v1.RecallRiskRegisterResponse;
import com.acme.risk.v1.RecallRiskScenarioRequest;
import com.acme.risk.v1.RecallRiskScenarioResponse;
import com.acme.risk.v1.RejectRiskScenarioRequest;
import com.acme.risk.v1.RejectRiskScenarioResponse;
import com.acme.risk.v1.ResolveRiskScenariosRequest;
import com.acme.risk.v1.ResolveRiskScenariosResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioComment;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.risk.v1.SubmitRiskScenarioRequest;
import com.acme.risk.v1.SubmitRiskScenarioResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskComment;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRiskRequest;
import com.acme.suppliers.v1.ValidateSupplierRiskResponse;
import com.google.protobuf.Any;
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
 * Routes risk scenarios through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RiskScenarioApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(RiskScenarioApprovalApplication.class);

    private final AppointmentServiceGrpc.AppointmentServiceBlockingStub appointmentsAppointmentServiceBlocking;

    private final StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceBlockingStub bankingStandingOrderWorkflowServiceBlocking;

    private final StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceStub featureflagsFeatureFlagServiceAsync;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final RiskRepository repository;

    private final RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking;

    private final RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    private final StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    public RiskScenarioApprovalApplication(
            AppointmentServiceGrpc.AppointmentServiceBlockingStub appointmentsAppointmentServiceBlocking,
            StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceBlockingStub bankingStandingOrderWorkflowServiceBlocking,
            StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            FeatureFlagServiceGrpc.FeatureFlagServiceStub featureflagsFeatureFlagServiceAsync,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            RiskRepository repository,
            RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking,
            RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking,
            StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking) {
        this.appointmentsAppointmentServiceBlocking = appointmentsAppointmentServiceBlocking;
        this.bankingStandingOrderWorkflowServiceBlocking = bankingStandingOrderWorkflowServiceBlocking;
        this.bankingStatementWorkflowServiceBlocking = bankingStatementWorkflowServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.featureflagsFeatureFlagServiceAsync = featureflagsFeatureFlagServiceAsync;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterApprovalServiceBlocking = riskRiskRegisterApprovalServiceBlocking;
        this.riskRiskScenarioQueryServiceBlocking = riskRiskScenarioQueryServiceBlocking;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
        this.riskStressTestValidationServiceBlocking = riskStressTestValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
    }

    /**
     * Submits a risk scenario.
     */
    public SubmitRiskScenarioResponse submitRiskScenario(SubmitRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = GetRiskScenarioRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenarioId(request.getRiskScenarioId())
                .build();
        var command0 = new DispatchCommand<>("acme.risk.v1.RiskScenarioValidationService/GetRiskScenarioRules", outbound0);
        var reply0 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getRiskScenarioRules(command0.payload());

        var outbound1 = ResolveRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> riskRiskScenarioQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveRiskScenarios(value));
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitRiskScenario", request.getRiskScenario().getRiskScenarioId());
        return SubmitRiskScenarioResponse.newBuilder()
                .setOutcome(SubmitRiskScenarioResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(request.getRiskScenario().getAssignee())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .setStatus(reply0.getRiskScenario().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a risk scenario.
     */
    public ApproveRiskScenarioResponse approveRiskScenario(ApproveRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        List<RecallRiskRegisterResponse> results0 = request.getRiskScenario().getCommentsList().stream()
                .map(element -> RecallRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(element.getCreatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(riskRiskRegisterApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallRiskRegister(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? RecallRiskRegisterResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListPendingRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingRiskRegistersRequest.Filter.newBuilder()
                        .setRegion(reply0.getRiskRegister().getLocation().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> riskRiskRegisterApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingRiskRegisters(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveRiskScenario", request.getRiskScenario().getRiskScenarioId());
        return ApproveRiskScenarioResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .putAllCustomFields(request.getRiskScenario().getCustomFieldsMap())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Rejects a risk scenario.
     */
    public RejectRiskScenarioResponse rejectRiskScenario(RejectRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        List<ListAppointmentsResponse> results0 = request.getRiskScenario().getHistoryList().stream()
                .map(element -> ListAppointmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAppointmentsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getChangedAt())
                        .setOwnerId(request.getRiskScenario().getSla().getPolicyId())
                        .build())
                .build())
                .flatMap(value -> Stream.of(appointmentsAppointmentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAppointments(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ListAppointmentsResponse.getDefaultInstance()
                : results0.getLast();

        var builder1 = CountHouseholdsRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(CountHouseholdsRequest.Filter.newBuilder()
                .setCreatedAfter(request.getRiskScenario().getAudit().getUpdatedAt())
                .setOwnerId(request.getRiskScenario().getCustomer().getCustomerId())
                .build());
        var outbound1 = builder1.build();
        var reply1 = customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countHouseholds(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectRiskScenario", request.getRiskScenario().getRiskScenarioId());
        return RejectRiskScenarioResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(request.getRiskScenario().getAssignee())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Escalates a risk scenario.
     */
    public EscalateRiskScenarioResponse escalateRiskScenario(EscalateRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = WatchUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getRiskScenario().getAssignee().getEmail())
                        .setName(request.getRiskScenario().getRequester().getName())
                        .setPhone(request.getRiskScenario().getRequester().getPhone())
                        .setPreferredChannel(request.getRiskScenario().getRequester().getPreferredChannel())
                        .build())
                .build();
        var stream0 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchUsers(outbound0);
        var reply0 = WatchUsersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = BatchCreateFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setRecord(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRiskScenario().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getRiskScenario().getCustomer().getEmail())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateFeatureFlagsResponse.getDefaultInstance());
        var input1 = featureflagsFeatureFlagServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFeatureFlags(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StartStandingOrderWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRiskScenario().getAudit().getEtag())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(switch (request.getRiskScenario().getOriginCase()) {
                                            case CUSTOMER -> request.getRiskScenario().getCustomer().getEmail();
                                            default -> "";
                                        })
                                        .setName(request.getRiskScenario().getRequester().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = bankingStandingOrderWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startStandingOrderWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateRiskScenario", request.getRiskScenario().getRiskScenarioId());
        return EscalateRiskScenarioResponse.newBuilder()
                .setOutcome(EscalateRiskScenarioResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getStandingOrder().getAudit().getUpdatedAt())
                                .build())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .setSubject(request.getRiskScenario().getSubject())
                        .build())
                .build();
    }

    /**
     * Lists risk scenarios a page at a time.
     */
    public ListPendingRiskScenariosResponse listPendingRiskScenarios(ListPendingRiskScenariosRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingRiskScenarios")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskScenario", request.getContext().getTraceId(), "ListPendingRiskScenarios");

        var outbound0 = LintCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            LintCustomersResponse fetch(LintCustomersRequest input) {
                return customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomers(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingRiskScenarios", request.getPage().getPageToken());
        return ListPendingRiskScenariosResponse.newBuilder()
                .addRiskScenarios(RiskScenario.newBuilder()
                        .addComments(RiskScenarioComment.newBuilder()
                                .setCreatedAt(reply0.getCustomer().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(reply0.getCustomer().getContact().getName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Delegates a risk scenario.
     */
    public DelegateRiskScenarioApprovalResponse delegateRiskScenarioApproval(DelegateRiskScenarioApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateRiskScenarioApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var builder0 = SharePremiumReportRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(SharePremiumReportRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setEmail(request.getRiskScenario().getRequester().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sharePremiumReport(outbound0);

        var outbound1 = ResumeStatementWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setStatement(Statement.newBuilder()
                        .setCounterparty(StandingOrderRef.newBuilder()
                                .setDisplayName(switch (request.getRiskScenario().getOriginCase()) {
                                    case CUSTOMER -> request.getRiskScenario().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = bankingStatementWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeStatementWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateRiskScenarioApproval", request.getRiskScenario().getRiskScenarioId());
        return DelegateRiskScenarioApprovalResponse.newBuilder()
                .setOutcome(DelegateRiskScenarioApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .addComments(RiskScenarioComment.newBuilder()
                                .setCreatedAt(reply0.getPremium().getAudit().getCreatedAt())
                                .build())
                        .putAllCustomFields(request.getRiskScenario().getCustomFieldsMap())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Recalls a risk scenario.
     */
    public RecallRiskScenarioResponse recallRiskScenario(RecallRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = CheckStressTestEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckStressTestEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getRiskScenario().getAssignee().getEmail())
                                .build())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(request.getRiskScenario().getCustomer().getCustomerId())
                                .setEmail(request.getRiskScenario().getCustomer().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> riskStressTestValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkStressTestEligibility(outbound0), rpcExecutor);

        var outbound1 = RetryStandingOrderWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getRiskScenario().getAssignee().getEmail())
                                        .setName(request.getRiskScenario().getRequester().getName())
                                        .setPreferredChannel(request.getRiskScenario().getRequester().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> bankingStandingOrderWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryStandingOrderWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = (BrowseSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSupplierContractsRequest.Filter.newBuilder()
                        .setRegion(reply0.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply2 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSupplierContracts(outbound2);

        var outbound3 = ValidateNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getStressTest().getAudit().getEtag())
                                .build())
                        .setCustomer(NotificationTemplateRef.newBuilder()
                                .setDisplayName(reply0.getStressTest().getCounterparty().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getStressTest().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateNotification(value));
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallRiskScenario", request.getRiskScenario().getRiskScenarioId());
        return RecallRiskScenarioResponse.newBuilder()
                .setOutcome(RecallRiskScenarioResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getStandingOrder().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply3.getNotification().getRequester().getEmail())
                                .build())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .setStatus(request.getRiskScenario().getStatus())
                        .build())
                .build();
    }

    /**
     * Comments a risk scenario.
     */
    public CommentRiskScenarioApprovalResponse commentRiskScenarioApproval(CommentRiskScenarioApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentRiskScenarioApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskScenario(request.getContext().getTenantId(), request.getRiskScenario());

        var outbound0 = ListPendingRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingRiskRegistersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRiskScenario().getAudit().getCreatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getRiskScenario().getRiskScenarioId())
                        .build())
                .build();
        var adapter0 = new Function<ListPendingRiskRegistersRequest, ListPendingRiskRegistersResponse>() {
            @Override
            public ListPendingRiskRegistersResponse apply(ListPendingRiskRegistersRequest input) {
                return riskRiskRegisterApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingRiskRegisters(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var futures1 = request.getRiskScenario().getCommentsList().stream()
                .map(element -> ValidateSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .addComments(SupplierRiskComment.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setVisibility(element.getVisibility())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSupplierRisk(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ValidateSupplierRiskResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRiskScenario().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentRiskScenarioApproval", request.getRiskScenario().getSubject());
        return CommentRiskScenarioApprovalResponse.newBuilder()
                .setOutcome(CommentRiskScenarioApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }
}
