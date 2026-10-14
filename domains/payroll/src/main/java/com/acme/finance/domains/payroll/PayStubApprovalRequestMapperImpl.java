package com.acme.finance.domains.payroll;

import com.acme.audit.v1.LintRetentionPoliciesRequest;
import com.acme.audit.v1.LintRetentionPoliciesResponse;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ResolveAccessPoliciesRequest;
import com.acme.authorization.v1.ResolveAccessPoliciesResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.ValidateVarianceImportRequest;
import com.acme.budgeting.v1.ValidateVarianceImportResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.giftcards.v1.CompareGiftCardOptionsRequest;
import com.acme.giftcards.v1.CompareGiftCardOptionsResponse;
import com.acme.giftcards.v1.GiftCard;
import com.acme.giftcards.v1.GiftCardBankFunding;
import com.acme.giftcards.v1.GiftCardCalculationServiceGrpc;
import com.acme.giftcards.v1.GiftCardLimit;
import com.acme.identity.v1.UserRef;
import com.acme.patients.v1.BrowsePatientsRequest;
import com.acme.patients.v1.BrowsePatientsResponse;
import com.acme.patients.v1.PatientQueryServiceGrpc;
import com.acme.payroll.v1.ApprovePayStubRequest;
import com.acme.payroll.v1.ApprovePayStubResponse;
import com.acme.payroll.v1.CommentPayStubApprovalRequest;
import com.acme.payroll.v1.CommentPayStubApprovalResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.DelegatePayStubApprovalRequest;
import com.acme.payroll.v1.DelegatePayStubApprovalResponse;
import com.acme.payroll.v1.EscalatePayStubRequest;
import com.acme.payroll.v1.EscalatePayStubResponse;
import com.acme.payroll.v1.EstimatePayRunRequest;
import com.acme.payroll.v1.EstimatePayRunResponse;
import com.acme.payroll.v1.ExplainPayStubCalculationRequest;
import com.acme.payroll.v1.ExplainPayStubCalculationResponse;
import com.acme.payroll.v1.GetDeductionLifecycleRequest;
import com.acme.payroll.v1.GetDeductionLifecycleResponse;
import com.acme.payroll.v1.GetPayStubApprovalStatusRequest;
import com.acme.payroll.v1.GetPayStubApprovalStatusResponse;
import com.acme.payroll.v1.ListPendingPayRunsRequest;
import com.acme.payroll.v1.ListPendingPayRunsResponse;
import com.acme.payroll.v1.ListPendingPayStubsRequest;
import com.acme.payroll.v1.ListPendingPayStubsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunApprovalServiceGrpc;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubCardSettlement;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.RecalculateDeductionsRequest;
import com.acme.payroll.v1.RecalculateDeductionsResponse;
import com.acme.payroll.v1.RecallPayStubRequest;
import com.acme.payroll.v1.RecallPayStubResponse;
import com.acme.payroll.v1.RejectPayStubRequest;
import com.acme.payroll.v1.RejectPayStubResponse;
import com.acme.payroll.v1.ShareDeductionReportRequest;
import com.acme.payroll.v1.ShareDeductionReportResponse;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.SubmitPayStubRequest;
import com.acme.payroll.v1.SubmitPayStubResponse;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.search.v1.CountSearchIndexesRequest;
import com.acme.search.v1.CountSearchIndexesResponse;
import com.acme.suppliers.v1.ListPendingSupplierContractsRequest;
import com.acme.suppliers.v1.ListPendingSupplierContractsResponse;
import com.acme.suppliers.v1.SubmitSupplierRiskRequest;
import com.acme.suppliers.v1.SubmitSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRiskApprovalServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class PayStubApprovalRequestMapperImpl implements PayStubApprovalRequestMapper {
    @Override
    public NormalizeRetentionPolicyRequest delegatePayStubApprovalStage3(DelegatePayStubApprovalRequest request, PreviewPayRunResponse reply2) {
        return NormalizeRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getPayRun().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
    }
}
