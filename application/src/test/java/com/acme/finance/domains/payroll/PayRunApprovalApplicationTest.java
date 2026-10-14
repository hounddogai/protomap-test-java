package com.acme.finance.domains.payroll;

import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseComment;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractWorkflowServiceGrpc;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.ResumeContractWorkflowRequest;
import com.acme.contracts.v1.ResumeContractWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.ListPendingClaimsRequest;
import com.acme.insurance.v1.ListPendingClaimsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.LintLedgerAccountsRequest;
import com.acme.ledger.v1.LintLedgerAccountsResponse;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.payroll.v1.ActivateDeductionRequest;
import com.acme.payroll.v1.ActivateDeductionResponse;
import com.acme.payroll.v1.ApprovePayRunRequest;
import com.acme.payroll.v1.ApprovePayRunResponse;
import com.acme.payroll.v1.CountPayStubsRequest;
import com.acme.payroll.v1.CountPayStubsResponse;
import com.acme.payroll.v1.CreatePayRunRequest;
import com.acme.payroll.v1.CreatePayRunResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.DeductionRouteAction;
import com.acme.payroll.v1.DelegatePayRunApprovalRequest;
import com.acme.payroll.v1.DelegatePayRunApprovalResponse;
import com.acme.payroll.v1.EscalatePayRunRequest;
import com.acme.payroll.v1.EscalatePayRunResponse;
import com.acme.payroll.v1.GenerateTaxWithholdingReportRequest;
import com.acme.payroll.v1.GenerateTaxWithholdingReportResponse;
import com.acme.payroll.v1.GetPayRunApprovalStatusRequest;
import com.acme.payroll.v1.GetPayRunApprovalStatusResponse;
import com.acme.payroll.v1.ListDeductionReportsRequest;
import com.acme.payroll.v1.ListDeductionReportsResponse;
import com.acme.payroll.v1.ListPendingPayRunsRequest;
import com.acme.payroll.v1.ListPendingPayRunsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.RecallPayRunRequest;
import com.acme.payroll.v1.RecallPayRunResponse;
import com.acme.payroll.v1.RejectPayRunRequest;
import com.acme.payroll.v1.RejectPayRunResponse;
import com.acme.payroll.v1.ScheduleDeductionExpiryRequest;
import com.acme.payroll.v1.ScheduleDeductionExpiryResponse;
import com.acme.payroll.v1.SubmitPayRunRequest;
import com.acme.payroll.v1.SubmitPayRunResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.TerminateDeductionRequest;
import com.acme.payroll.v1.TerminateDeductionResponse;
import com.acme.payroll.v1.ValidateTaxWithholdingRequest;
import com.acme.payroll.v1.ValidateTaxWithholdingResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.RecallPurchaseOrderRequest;
import com.acme.procurement.v1.RecallPurchaseOrderResponse;
import com.acme.workplaces.v1.BuildingServiceGrpc;
import com.acme.workplaces.v1.WatchBuildingsRequest;
import com.acme.workplaces.v1.WatchBuildingsResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PayRunApprovalApplicationTest {
    @Test
    void delegatePayRunApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var listener = new PayRunApprovalApplicationEvents(NotificationValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PayRunApprovalApplication.RejectPayRunStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PayRunApprovalApplication(
                CollectionCaseWorkflowServiceGrpc.newBlockingStub(channel),
                ContractWorkflowServiceGrpc.newBlockingStub(channel),
                events,
                ClaimApprovalServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                DeductionLifecycleServiceGrpc.newBlockingStub(channel),
                DeductionReportingServiceGrpc.newBlockingStub(channel),
                PayRunServiceGrpc.newBlockingStub(channel),
                PayStubQueryServiceGrpc.newBlockingStub(channel),
                TaxWithholdingReportingServiceGrpc.newBlockingStub(channel),
                TaxWithholdingValidationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderApprovalServiceGrpc.newBlockingStub(channel),
                repository,
                BuildingServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(DelegatePayRunApprovalRequest.getDefaultInstance());
        var response = application.delegatePayRunApproval(request);
        var sent0 = channel.request("acme.procurement.v1.PurchaseOrderApprovalService/RecallPurchaseOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "card", "last4"),
                ProbeSupport.value(sent0, "purchase_order", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "details", "approver", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "purchase_order", "counterparty", "display_name"));
        var sent1 = channel.request("acme.payroll.v1.TaxWithholdingValidationService/ValidateTaxWithholding");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "audit", "updated_by"),
                ProbeSupport.value(sent1, "tax_withholding", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "audit", "etag"),
                ProbeSupport.value(sent1, "tax_withholding", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        var sent2 = channel.request("acme.collections.v1.CollectionCaseWorkflowService/RetryCollectionCaseWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderApprovalService/RecallPurchaseOrder"), "purchase_order", "audit", "updated_by"),
                ProbeSupport.value(sent2, "collection_case", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderApprovalService/RecallPurchaseOrder"), "purchase_order", "audit", "created_at"),
                ProbeSupport.value(sent2, "collection_case", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingValidationService/ValidateTaxWithholding"), "tax_withholding", "audit", "etag"),
                ProbeSupport.value(sent2, "collection_case", "audit", "etag"));
        var sent3 = channel.request("acme.payroll.v1.DeductionReportingService/ListDeductionReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderApprovalService/RecallPurchaseOrder"), "purchase_order", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseWorkflowService/RetryCollectionCaseWorkflow"), "collection_case", "customer", "payment_promise_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseWorkflowService/RetryCollectionCaseWorkflow"), "collection_case", "custom_fields"),
                ProbeSupport.value(sent3, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderApprovalService/RecallPurchaseOrder"), "purchase_order", "billing_address", "country_code"),
                ProbeSupport.value(response, "pay_run", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingValidationService/ValidateTaxWithholding"), "tax_withholding", "discount", "amount"),
                ProbeSupport.value(response, "pay_run", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseWorkflowService/RetryCollectionCaseWorkflow"), "collection_case", "audit", "created_at"),
                ProbeSupport.value(response, "pay_run", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.DeductionReportingService/ListDeductionReports"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.DeductionReportingService/ListDeductionReports"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run_id"),
                ProbeSupport.value(response, "pay_run", "pay_run_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "metadata"),
                ProbeSupport.value(response, "pay_run", "metadata"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PayRunApprovalApplicationEvents(NotificationValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<PreviewNotificationResponse>();
        listener.on(new PayRunApprovalApplication.RejectPayRunStage0Requested(ProbeSupport.sample(PreviewNotificationRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
