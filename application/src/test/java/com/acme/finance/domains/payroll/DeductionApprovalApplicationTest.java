package com.acme.finance.domains.payroll;

import com.acme.assets.v1.CancelDepreciationWorkflowRequest;
import com.acme.assets.v1.CancelDepreciationWorkflowResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.CreateAuditEventRequest;
import com.acme.audit.v1.CreateAuditEventResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.devices.v1.EstimateManagedDeviceRequest;
import com.acme.devices.v1.EstimateManagedDeviceResponse;
import com.acme.devices.v1.ManagedDevice;
import com.acme.devices.v1.ManagedDeviceCalculationServiceGrpc;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.LintDocumentsRequest;
import com.acme.documents.v1.LintDocumentsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.CheckUserEligibilityRequest;
import com.acme.identity.v1.CheckUserEligibilityResponse;
import com.acme.identity.v1.LintUsersRequest;
import com.acme.identity.v1.LintUsersResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.ListPolicyWorkflowsRequest;
import com.acme.insurance.v1.ListPolicyWorkflowsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumeClaimWorkflowRequest;
import com.acme.insurance.v1.ResumeClaimWorkflowResponse;
import com.acme.insurance.v1.StartPolicyWorkflowRequest;
import com.acme.insurance.v1.StartPolicyWorkflowResponse;
import com.acme.labeling.v1.LabelingTaskServiceGrpc;
import com.acme.labeling.v1.ListLabelingTasksRequest;
import com.acme.labeling.v1.ListLabelingTasksResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesResponse;
import com.acme.ledger.v1.ValidateLedgerAccountRequest;
import com.acme.ledger.v1.ValidateLedgerAccountResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.WatchMetricSeriesesRequest;
import com.acme.metrics.v1.WatchMetricSeriesesResponse;
import com.acme.payroll.v1.ApproveDeductionRequest;
import com.acme.payroll.v1.ApproveDeductionResponse;
import com.acme.payroll.v1.CommentDeductionApprovalRequest;
import com.acme.payroll.v1.CommentDeductionApprovalResponse;
import com.acme.payroll.v1.CommentPayStubApprovalRequest;
import com.acme.payroll.v1.CommentPayStubApprovalResponse;
import com.acme.payroll.v1.CompareDeductionOptionsRequest;
import com.acme.payroll.v1.CompareDeductionOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionDiscountAction;
import com.acme.payroll.v1.DelegateDeductionApprovalRequest;
import com.acme.payroll.v1.DelegateDeductionApprovalResponse;
import com.acme.payroll.v1.EscalateDeductionRequest;
import com.acme.payroll.v1.EscalateDeductionResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.FindPayStubsRequest;
import com.acme.payroll.v1.FindPayStubsResponse;
import com.acme.payroll.v1.GetDeductionApprovalStatusRequest;
import com.acme.payroll.v1.GetDeductionApprovalStatusResponse;
import com.acme.payroll.v1.GetDeductionLifecycleRequest;
import com.acme.payroll.v1.GetDeductionLifecycleResponse;
import com.acme.payroll.v1.ListPendingDeductionsRequest;
import com.acme.payroll.v1.ListPendingDeductionsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubApprovalServiceGrpc;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.RecallDeductionRequest;
import com.acme.payroll.v1.RecallDeductionResponse;
import com.acme.payroll.v1.RejectDeductionRequest;
import com.acme.payroll.v1.RejectDeductionResponse;
import com.acme.payroll.v1.SubmitDeductionRequest;
import com.acme.payroll.v1.SubmitDeductionResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.risk.v1.CancelExposureWorkflowRequest;
import com.acme.risk.v1.CancelExposureWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class DeductionApprovalApplicationTest {
    @Test
    void escalateDeductionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new DeductionApprovalApplication(
                DepreciationWorkflowServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ManagedDeviceCalculationServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                ExpensePolicyLifecycleServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newFutureStub(channel),
                ClaimWorkflowServiceGrpc.newBlockingStub(channel),
                PolicyWorkflowServiceGrpc.newBlockingStub(channel),
                PolicyWorkflowServiceGrpc.newFutureStub(channel),
                LabelingTaskServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                new DeductionApprovalRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                DeductionCalculationServiceGrpc.newBlockingStub(channel),
                PayStubApprovalServiceGrpc.newFutureStub(channel),
                PayStubQueryServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureWorkflowServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(EscalateDeductionRequest.getDefaultInstance());
        var response = application.escalateDeduction(request);
        var sent0 = channel.request("acme.risk.v1.ExposureWorkflowService/CancelExposureWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "owner", "email"),
                ProbeSupport.value(sent0, "exposure", "holder", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "audit", "updated_by"),
                ProbeSupport.value(sent0, "exposure", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "exposure", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "audit", "etag"),
                ProbeSupport.value(sent0, "exposure", "audit", "etag"));
        var sent1 = channel.request("acme.documents.v1.DocumentValidationService/LintDocuments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "owner", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        var sent2 = channel.request("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/LintDocuments"), "document", "audit", "created_by"),
                ProbeSupport.value(sent2, "tax_withholding", "audit", "created_by"));
        var sent3 = channel.request("acme.insurance.v1.PolicyWorkflowService/ListPolicyWorkflows");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/LintDocuments"), "document", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/CancelExposureWorkflow"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/LintDocuments"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "tax_withholding", "audit", "updated_at"),
                ProbeSupport.value(response, "deduction", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "tax_withholding", "priority"),
                ProbeSupport.value(response, "deduction", "priority"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyWorkflowService/ListPolicyWorkflows"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyWorkflowService/ListPolicyWorkflows"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction_id"),
                ProbeSupport.value(response, "deduction", "deduction_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "owner"),
                ProbeSupport.value(response, "deduction", "owner"));
    }
}
