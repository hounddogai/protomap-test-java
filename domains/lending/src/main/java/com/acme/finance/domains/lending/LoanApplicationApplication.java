package com.acme.finance.domains.lending;

import com.acme.audit.v1.ReconcileRetentionPoliciesRequest;
import com.acme.audit.v1.ReconcileRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ResolveClaimsRequest;
import com.acme.insurance.v1.ResolveClaimsResponse;
import com.acme.lending.v1.BatchGetLoanApplicationsRequest;
import com.acme.lending.v1.BatchGetLoanApplicationsResponse;
import com.acme.lending.v1.CheckCollateralEligibilityRequest;
import com.acme.lending.v1.CheckCollateralEligibilityResponse;
import com.acme.lending.v1.CloneLoanApplicationRequest;
import com.acme.lending.v1.CloneLoanApplicationResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.CreateLoanApplicationRequest;
import com.acme.lending.v1.CreateLoanApplicationResponse;
import com.acme.lending.v1.DeleteLoanApplicationRequest;
import com.acme.lending.v1.DeleteLoanApplicationResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.GetLoanApplicationRequest;
import com.acme.lending.v1.GetLoanApplicationResponse;
import com.acme.lending.v1.GetLoanApplicationRulesRequest;
import com.acme.lending.v1.GetLoanApplicationRulesResponse;
import com.acme.lending.v1.LintCollateralsRequest;
import com.acme.lending.v1.LintCollateralsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.SearchLoanApplicationsRequest;
import com.acme.lending.v1.SearchLoanApplicationsResponse;
import com.acme.notifications.v1.ExplainNotificationDecisionRequest;
import com.acme.notifications.v1.ExplainNotificationDecisionResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationSla;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Manages loan applications: creates, reads, changes, and lists them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class LoanApplicationApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanApplicationApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final ChannelRegistry channels;

    private final ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    public LoanApplicationApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            ChannelRegistry channels,
            ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.channels = channels;
        this.insuranceClaimQueryServiceBlocking = insuranceClaimQueryServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Creates a loan application.
     */
    public CreateLoanApplicationResponse createLoanApplication(CreateLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = LintCollateralsRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getLoanApplication().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getLoanApplication().getBillingAddress().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getLoanApplication().getAudit().getEtag())
                                .setUpdatedBy(request.getLoanApplication().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCollaterals(outbound0), rpcExecutor);

        var outbound1 = ResolveClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveClaims(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return CreateLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(request.getLoanApplication().getCounterparty())
                        .setTotal(request.getLoanApplication().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a loan application.
     */
    public GetLoanApplicationResponse getLoanApplication(GetLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getLoanApplicationId(), "GetLoanApplication");

        var outbound0 = ExplainNotificationDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainNotificationDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setSla(NotificationSla.newBuilder()
                                .setDueAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        Function<ExplainNotificationDecisionRequest, ExplainNotificationDecisionResponse> operation0 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainNotificationDecision;
        var reply0 = operation0.apply(outbound0);
        return GetLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getNotification().getAudit().getCreatedBy())
                                .build())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .build();
    }

    /**
     * Deletes a loan application.
     */
    public DeleteLoanApplicationResponse deleteLoanApplication(DeleteLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getLoanApplicationId(), "DeleteLoanApplication");

        var outbound0 = GetLoanApplicationRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplicationId(request.getLoanApplicationId())
                .build();
        var channel0 = channels.lookup("acme.lending.v1.LoanApplicationValidationService");
        var serviceName0 = "acme.lending.v1" + "." + "LoanApplicationValidationService";
        var methodName0 = "Get" + "LoanApplicationRules";
        var descriptor0 = MethodDescriptor.<GetLoanApplicationRulesRequest, GetLoanApplicationRulesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(GetLoanApplicationRulesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(GetLoanApplicationRulesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getReason()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteLoanApplication", request.getReason());
        return DeleteLoanApplicationResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several loan applications at once.
     */
    public BatchGetLoanApplicationsResponse batchGetLoanApplications(BatchGetLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getContext().getTraceId(), "BatchGetLoanApplications");

        var outbound0 = BeanCopies.copy(CheckCollateralEligibilityRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), CheckCollateralEligibilityRequest.class);
        var reply0 = lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCollateralEligibility(outbound0);
        return BatchGetLoanApplicationsResponse.newBuilder()
                .addLoanApplications(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCollateral().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists loan applications a page at a time.
     */
    public SearchLoanApplicationsResponse searchLoanApplications(SearchLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getContext().getTraceId(), "SearchLoanApplications");

        var outbound0 = CheckCollateralEligibilityRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            CheckCollateralEligibilityResponse fetch(CheckCollateralEligibilityRequest input) {
                return lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCollateralEligibility(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchLoanApplications", request.getPage().getPageToken());
        return SearchLoanApplicationsResponse.newBuilder()
                .addLoanApplications(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCollateral().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
    }

    /**
     * Creates a loan application.
     */
    public CloneLoanApplicationResponse cloneLoanApplication(CloneLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = BeanCopies.copy(ExplainLoanCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainLoanCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build(), ExplainLoanCalculationRequest.class);
        var reply0 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanCalculation(outbound0);

        var results1 = new ArrayList<ReconcileRetentionPoliciesResponse>();
        for (var element : request.getLoanApplication().getLinesList()) {
            ReconcileRetentionPoliciesRequest itemRequest = ReconcileRetentionPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileRetentionPoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setDescription(element.getDescription())
                        .build())
                .build();
            results1.add(auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileRetentionPolicies(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ReconcileRetentionPoliciesResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneLoanApplication", request.getLoanApplication().getNumber());
        return CloneLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setLoanApplicationId(request.getLoanApplication().getLoanApplicationId())
                        .setTotal(request.getLoanApplication().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }
}
