package com.acme.finance.domains.compliance;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.UpdateAccessPolicyRequest;
import com.acme.authorization.v1.UpdateAccessPolicyResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.ResolveBankAccountsRequest;
import com.acme.banking.v1.ResolveBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CheckSanctionEligibilityRequest;
import com.acme.compliance.v1.CheckSanctionEligibilityResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlRouteAction;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.compliance.v1.ExplainSanctionDecisionRequest;
import com.acme.compliance.v1.ExplainSanctionDecisionResponse;
import com.acme.compliance.v1.GetSanctionRulesRequest;
import com.acme.compliance.v1.GetSanctionRulesResponse;
import com.acme.compliance.v1.LintControlsRequest;
import com.acme.compliance.v1.LintControlsResponse;
import com.acme.compliance.v1.NormalizeSanctionRequest;
import com.acme.compliance.v1.NormalizeSanctionResponse;
import com.acme.compliance.v1.PreviewSanctionRequest;
import com.acme.compliance.v1.PreviewSanctionResponse;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.ScoreSanctionRequest;
import com.acme.compliance.v1.ScoreSanctionResponse;
import com.acme.compliance.v1.ValidateControlRequest;
import com.acme.compliance.v1.ValidateControlResponse;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.SearchPoliciesRequest;
import com.acme.insurance.v1.SearchPoliciesResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks sanctions against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class SanctionValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(SanctionValidationApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final SanctionValidationRequestMapper mapper;

    private final ComplianceRepository repository;

    public SanctionValidationApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            SanctionValidationRequestMapper mapper,
            ComplianceRepository repository) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceControlValidationServiceBlocking = complianceControlValidationServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    /**
     * Validates a sanction.
     */
    public ValidateSanctionResponse validateSanction(ValidateSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = BeanCopies.copy(CheckLoanApplicationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getSanction().getLocation().getRecipient())
                                .build())
                        .build())
                .build(), CheckLoanApplicationEligibilityRequest.class);
        var reply0 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkLoanApplicationEligibility(outbound0);

        var outbound1 = EstimateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getLoanApplication().getCounterparty().getDisplayName())
                                .setUserId(request.getSanction().getManager().getUserId())
                                .build())
                        .build())
                .setDetails(EstimateControlRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getSanction().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.compliance.v1.ControlCalculationService");
        var serviceName1 = "acme.compliance.v1" + "." + "ControlCalculationService";
        var methodName1 = "Est" + "imateControl";
        var descriptor1 = MethodDescriptor.<EstimateControlRequest, EstimateControlResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(EstimateControlRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EstimateControlResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateSanction", request.getSanction().getSanctionId());
        return ValidateSanctionResponse.newBuilder()
                .setSanction(Sanction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getControl().getAudit().getEtag())
                                .build())
                        .setDepth(request.getSanction().getDepth())
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getLoanApplication().getBillingAddress().getPostalCode())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Previews a sanction.
     */
    public PreviewSanctionResponse previewSanction(PreviewSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = QuoteComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getSanction().getManager().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getSanction().getAudit().getUpdatedBy())
                                .build())
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(request.getSanction().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteComplianceCaseRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getSanction().getLocation().getPostalCode())
                                .build())
                        .build())
                .build();
        Function<QuoteComplianceCaseRequest, QuoteComplianceCaseResponse> operation0 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::quoteComplianceCase;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = UpdateAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSanction().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            UpdateAccessPolicyResponse fetch(UpdateAccessPolicyRequest input) {
                return authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateAccessPolicy(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = mapper.previewSanctionStage2(request);
        var reply2 = bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveBankAccounts(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewSanction", request.getSanction().getSanctionId());
        return PreviewSanctionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(reply1.getAccessPolicy().getAudit().getUpdatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply1.getAccessPolicy().getOwner().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setDisplayName(reply0.getComplianceCase().getCustomer().getDisplayName())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .setStatus(request.getSanction().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a sanction.
     */
    public CheckSanctionEligibilityResponse checkSanctionEligibility(CheckSanctionEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckSanctionEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = StreamFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamFiscalPeriodExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSanction().getAudit().getCreatedAt())
                        .putAllLabels(request.getSanction().getAttributesMap())
                        .setOwnerId(request.getSanction().getSanctionId())
                        .setRegion(request.getSanction().getLocation().getRegion())
                        .build())
                .build();
        var stream0 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamFiscalPeriodExport(outbound0);
        var reply0 = StreamFiscalPeriodExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getSanction().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckSanctionEligibility", request.getSanction().getName());
        return CheckSanctionEligibilityResponse.newBuilder()
                .setSanction(Sanction.newBuilder()
                        .setDepth(request.getSanction().getDepth())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Normalizes a sanction.
     */
    public NormalizeSanctionResponse normalizeSanction(NormalizeSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = RpcSupport.unpack(Any.pack(LintControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSanction().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getSanction().getManager().getEmail())
                                .build())
                        .build())
                .setDetails(LintControlsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build()), LintControlsRequest.class);
        var reply0 = complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintControls(outbound0);

        var outbound1 = BeanCopies.copy(ValidateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setRoute(ControlRouteAction.newBuilder()
                                .setTarget(reply0.getControl().getRoute().getTarget())
                                .build())
                        .build())
                .build(), ValidateControlRequest.class);
        var reply1 = complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateControl(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeSanction", request.getSanction().getSanctionId());
        return NormalizeSanctionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setEtag(reply1.getControl().getAudit().getEtag())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setDisplayName(reply0.getControl().getOwner().getDisplayName())
                                .build())
                        .setParentId(request.getSanction().getParentId())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Explains a sanction.
     */
    public ExplainSanctionDecisionResponse explainSanctionDecision(ExplainSanctionDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainSanctionDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = ValidateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getSanction().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getSanction().getManager().getEmail())
                                .setUserId(request.getSanction().getManager().getUserId())
                                .build())
                        .build())
                .setDetails(ValidateControlRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        Function<ValidateControlRequest, ValidateControlResponse> operation0 = complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateControl;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainSanctionDecision", request.getSanction().getSanctionId());
        return ExplainSanctionDecisionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .putAllAttributes(request.getSanction().getAttributesMap())
                        .addChildren(Sanction.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedBy(reply0.getControl().getAudit().getUpdatedBy())
                                        .build())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Scores a sanction.
     */
    public ScoreSanctionResponse scoreSanction(ScoreSanctionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreSanction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSanction(request.getContext().getTenantId(), request.getSanction());

        var outbound0 = SearchPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchPoliciesRequest.Filter.newBuilder()
                        .setRegion(request.getSanction().getLocation().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.PolicyService");
        var serviceName0 = "acme.insurance.v1" + "." + "PolicyService";
        var methodName0 = "Sea" + "rchPolicies";
        var descriptor0 = MethodDescriptor.<SearchPoliciesRequest, SearchPoliciesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SearchPoliciesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SearchPoliciesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSanction().getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreSanction", request.getSanction().getSanctionId());
        return ScoreSanctionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setSanction(Sanction.newBuilder()
                        .putAllAttributes(request.getSanction().getAttributesMap())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Returns a sanction.
     */
    public GetSanctionRulesResponse getSanctionRules(GetSanctionRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetSanctionRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "GetSanctionRules");
        return GetSanctionRulesResponse.newBuilder()
                .setSanction(Sanction.newBuilder()
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }
}
