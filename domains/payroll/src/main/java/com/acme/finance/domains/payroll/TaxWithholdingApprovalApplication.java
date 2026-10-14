package com.acme.finance.domains.payroll;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.WatchArticlesRequest;
import com.acme.content.v1.WatchArticlesResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationApprovalServiceGrpc;
import com.acme.contracts.v1.RecallObligationRequest;
import com.acme.contracts.v1.RecallObligationResponse;
import com.acme.contracts.v1.RejectObligationRequest;
import com.acme.contracts.v1.RejectObligationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.GetUserRequest;
import com.acme.identity.v1.GetUserResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.CommentPremiumApprovalRequest;
import com.acme.insurance.v1.CommentPremiumApprovalResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumLineTax;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.SummarizeNotificationsRequest;
import com.acme.notifications.v1.SummarizeNotificationsResponse;
import com.acme.payroll.v1.ApproveTaxWithholdingRequest;
import com.acme.payroll.v1.ApproveTaxWithholdingResponse;
import com.acme.payroll.v1.CommentTaxWithholdingApprovalRequest;
import com.acme.payroll.v1.CommentTaxWithholdingApprovalResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCondition;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DelegateTaxWithholdingApprovalRequest;
import com.acme.payroll.v1.DelegateTaxWithholdingApprovalResponse;
import com.acme.payroll.v1.DeleteTaxWithholdingReportRequest;
import com.acme.payroll.v1.DeleteTaxWithholdingReportResponse;
import com.acme.payroll.v1.EscalateTaxWithholdingRequest;
import com.acme.payroll.v1.EscalateTaxWithholdingResponse;
import com.acme.payroll.v1.GetTaxWithholdingApprovalStatusRequest;
import com.acme.payroll.v1.GetTaxWithholdingApprovalStatusResponse;
import com.acme.payroll.v1.ListTaxWithholdingReportsRequest;
import com.acme.payroll.v1.ListTaxWithholdingReportsResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubApprovalServiceGrpc;
import com.acme.payroll.v1.RecallTaxWithholdingRequest;
import com.acme.payroll.v1.RecallTaxWithholdingResponse;
import com.acme.payroll.v1.RejectPayStubRequest;
import com.acme.payroll.v1.RejectPayStubResponse;
import com.acme.payroll.v1.RejectTaxWithholdingRequest;
import com.acme.payroll.v1.RejectTaxWithholdingResponse;
import com.acme.payroll.v1.ScheduleDeductionExpiryRequest;
import com.acme.payroll.v1.ScheduleDeductionExpiryResponse;
import com.acme.payroll.v1.SubmitTaxWithholdingRequest;
import com.acme.payroll.v1.SubmitTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingRouteAction;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.ValidateTaxWithholdingRequest;
import com.acme.payroll.v1.ValidateTaxWithholdingResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes tax withholdings through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class TaxWithholdingApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(TaxWithholdingApprovalApplication.class);

    private final ChannelRegistry channels;

    private final ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking;

    private final ObligationApprovalServiceGrpc.ObligationApprovalServiceBlockingStub contractsObligationApprovalServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking;

    private final PayStubApprovalServiceGrpc.PayStubApprovalServiceBlockingStub payrollPayStubApprovalServiceBlocking;

    private final TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking;

    private final TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking;

    private final PayrollRepository repository;

    private final Executor rpcExecutor;

    public TaxWithholdingApprovalApplication(
            ChannelRegistry channels,
            ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking,
            ObligationApprovalServiceGrpc.ObligationApprovalServiceBlockingStub contractsObligationApprovalServiceBlocking,
            ApplicationEventPublisher events,
            PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking,
            PayStubApprovalServiceGrpc.PayStubApprovalServiceBlockingStub payrollPayStubApprovalServiceBlocking,
            TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking,
            TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking,
            PayrollRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.channels = channels;
        this.contentArticleServiceBlocking = contentArticleServiceBlocking;
        this.contractsObligationApprovalServiceBlocking = contractsObligationApprovalServiceBlocking;
        this.events = events;
        this.insurancePremiumApprovalServiceBlocking = insurancePremiumApprovalServiceBlocking;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.payrollDeductionLifecycleServiceBlocking = payrollDeductionLifecycleServiceBlocking;
        this.payrollPayStubApprovalServiceBlocking = payrollPayStubApprovalServiceBlocking;
        this.payrollTaxWithholdingReportingServiceBlocking = payrollTaxWithholdingReportingServiceBlocking;
        this.payrollTaxWithholdingValidationServiceBlocking = payrollTaxWithholdingValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record ApproveTaxWithholdingStage1Requested(GetUserRequest request, CompletableFuture<GetUserResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a tax withholding.
     */
    public SubmitTaxWithholdingResponse submitTaxWithholding(SubmitTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = DeleteTaxWithholdingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setEtag(request.getTaxWithholding().getAudit().getEtag())
                .setReason(switch (request.getTaxWithholding().getActionCase()) {
                    case BLOCK -> request.getTaxWithholding().getBlock().getReason();
                    default -> "";
                })
                .setTaxWithholdingId(request.getTaxWithholding().getTaxWithholdingId())
                .build();
        var reply0 = payrollTaxWithholdingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deleteTaxWithholdingReport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitTaxWithholding", request.getTaxWithholding().getName());
        return SubmitTaxWithholdingResponse.newBuilder()
                .setOutcome(SubmitTaxWithholdingResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setEffective(request.getTaxWithholding().getEffective())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Approves a tax withholding.
     */
    public ApproveTaxWithholdingResponse approveTaxWithholding(ApproveTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var futures0 = request.getTaxWithholding().getConditionsList().stream()
                .map(element -> ScheduleDeductionExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .addConditions(DeductionCondition.newBuilder()
                                .setNegate(element.getNegate())
                                .setOperator(element.getOperator())
                                .setValue(element.getValue())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> payrollDeductionLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleDeductionExpiry(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ScheduleDeductionExpiryResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = GetUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUserId(request.getTaxWithholding().getOwner().getUserId())
                .build();
        var completion1 = new CompletableFuture<GetUserResponse>();
        events.publishEvent(new ApproveTaxWithholdingStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return ApproveTaxWithholdingResponse.newBuilder()
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getUser().getAudit().getEtag())
                                .build())
                        .setEffective(request.getTaxWithholding().getEffective())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply1.getUser().getRelationshipManager().getUserId())
                                .build())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Rejects a tax withholding.
     */
    public RejectTaxWithholdingResponse rejectTaxWithholding(RejectTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = ValidateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getTaxWithholding().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setRoute(TaxWithholdingRouteAction.newBuilder()
                                .setWeight(request.getTaxWithholding().getRoute().getWeight())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ValidateTaxWithholdingResponse fetch(ValidateTaxWithholdingRequest input) {
                return payrollTaxWithholdingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateTaxWithholding(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return RejectTaxWithholdingResponse.newBuilder()
                .setOutcome(RejectTaxWithholdingResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .setVersion(request.getTaxWithholding().getVersion())
                        .build())
                .build();
    }

    /**
     * Escalates a tax withholding.
     */
    public EscalateTaxWithholdingResponse escalateTaxWithholding(EscalateTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = RecallObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallObligationRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getTaxWithholding().getOwner().getUserId())
                                .build())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getTaxWithholding().getOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getTaxWithholding().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(contractsObligationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallObligation(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CommentPremiumApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .addLines(PremiumLine.newBuilder()
                                .setTax(PremiumLineTax.newBuilder()
                                        .setAmount(request.getTaxWithholding().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> insurancePremiumApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPremiumApproval(value));
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return EscalateTaxWithholdingResponse.newBuilder()
                .setOutcome(EscalateTaxWithholdingResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setEffective(request.getTaxWithholding().getEffective())
                        .setPriority(reply0.getObligation().getPriority())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Delegates a tax withholding.
     */
    public DelegateTaxWithholdingApprovalResponse delegateTaxWithholdingApproval(DelegateTaxWithholdingApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateTaxWithholdingApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = WatchArticlesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchArticlesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getTaxWithholding().getAudit().getCreatedAt())
                        .build())
                .setSince(request.getTaxWithholding().getAudit().getUpdatedAt())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getTaxWithholding().getOwner().getEmail())
                        .build())
                .build();
        var stream0 = contentArticleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchArticles(outbound0);
        var reply0 = WatchArticlesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateTaxWithholdingApproval", request.getTaxWithholding().getTaxWithholdingId());
        return DelegateTaxWithholdingApprovalResponse.newBuilder()
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setEffective(request.getTaxWithholding().getEffective())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Recalls a tax withholding.
     */
    public RecallTaxWithholdingResponse recallTaxWithholding(RecallTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = (SummarizeNotificationsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply0 = notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeNotifications(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return RecallTaxWithholdingResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setOwner(request.getTaxWithholding().getOwner())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Returns a tax withholding.
     */
    public GetTaxWithholdingApprovalStatusResponse getTaxWithholdingApprovalStatus(GetTaxWithholdingApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetTaxWithholdingApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "GetTaxWithholdingApprovalStatus");
        return GetTaxWithholdingApprovalStatusResponse.newBuilder()
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Comments a tax withholding.
     */
    public CommentTaxWithholdingApprovalResponse commentTaxWithholdingApproval(CommentTaxWithholdingApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentTaxWithholdingApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var builder0 = RejectObligationRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(RejectObligationRequest.Details.newBuilder()
                .setApprover(UserRef.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build());
        builder0.setObligation(Obligation.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setDisplayName(request.getTaxWithholding().getOwner().getDisplayName())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = contractsObligationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectObligation(outbound0);

        var outbound1 = ListTaxWithholdingReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListTaxWithholdingReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getTaxWithholding().getAudit().getCreatedAt())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.payroll.v1.TaxWithholdingReportingService/ListTaxWithholdingReports", outbound1);
        var reply1 = payrollTaxWithholdingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listTaxWithholdingReports(command1.payload());

        var outbound2 = GetUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUserId(request.getTaxWithholding().getOwner().getUserId())
                .build();
        var channel2 = channels.lookup("acme.identity.v1.UserService");
        var selected2 = UserServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.getUser(outbound2);

        var outbound3 = RejectPayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getTaxWithholding().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        RpcPort<RejectPayStubRequest, RejectPayStubResponse> port3 = RpcPort.adapt(payrollPayStubApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::rejectPayStub);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentTaxWithholdingApproval", request.getTaxWithholding().getTaxWithholdingId());
        return CommentTaxWithholdingApprovalResponse.newBuilder()
                .setOutcome(CommentTaxWithholdingApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getUser().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getUser().getRelationshipManager().getDisplayName())
                                .setEmail(reply0.getObligation().getCustomer().getEmail())
                                .build())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .setVersion(request.getTaxWithholding().getVersion())
                        .build())
                .build();
    }
}
