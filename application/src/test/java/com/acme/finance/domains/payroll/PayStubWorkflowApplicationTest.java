package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.banking.v1.CommentMandateApprovalRequest;
import com.acme.banking.v1.CommentMandateApprovalResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalWorkflowServiceGrpc;
import com.acme.contracts.v1.ResumeRenewalWorkflowRequest;
import com.acme.contracts.v1.ResumeRenewalWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.notifications.v1.BatchGetNotificationsRequest;
import com.acme.notifications.v1.BatchGetNotificationsResponse;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.payroll.v1.AdvancePayStubWorkflowRequest;
import com.acme.payroll.v1.AdvancePayStubWorkflowResponse;
import com.acme.payroll.v1.CancelPayStubWorkflowRequest;
import com.acme.payroll.v1.CancelPayStubWorkflowResponse;
import com.acme.payroll.v1.CheckPayRunEligibilityRequest;
import com.acme.payroll.v1.CheckPayRunEligibilityResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.InspectPayStubWorkflowRequest;
import com.acme.payroll.v1.InspectPayStubWorkflowResponse;
import com.acme.payroll.v1.LookupPayStubRequest;
import com.acme.payroll.v1.LookupPayStubResponse;
import com.acme.payroll.v1.PausePayStubWorkflowRequest;
import com.acme.payroll.v1.PausePayStubWorkflowResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.RecalculatePayStubsRequest;
import com.acme.payroll.v1.RecalculatePayStubsResponse;
import com.acme.payroll.v1.ReinstateDeductionRequest;
import com.acme.payroll.v1.ReinstateDeductionResponse;
import com.acme.payroll.v1.ResumePayStubWorkflowRequest;
import com.acme.payroll.v1.ResumePayStubWorkflowResponse;
import com.acme.payroll.v1.RetryPayStubWorkflowRequest;
import com.acme.payroll.v1.RetryPayStubWorkflowResponse;
import com.acme.payroll.v1.ScheduleDeductionReportRequest;
import com.acme.payroll.v1.ScheduleDeductionReportResponse;
import com.acme.payroll.v1.SignalPayStubWorkflowRequest;
import com.acme.payroll.v1.SignalPayStubWorkflowResponse;
import com.acme.payroll.v1.StartPayStubWorkflowRequest;
import com.acme.payroll.v1.StartPayStubWorkflowResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionAttachment;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.risk.v1.RetryRiskRegisterWorkflowRequest;
import com.acme.risk.v1.RetryRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.tours.v1.CreateTourRequest;
import com.acme.tours.v1.CreateTourResponse;
import com.acme.tours.v1.Tour;
import com.acme.tours.v1.TourParticipant;
import com.acme.tours.v1.TourServiceGrpc;
import java.util.List;
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

class PayStubWorkflowApplicationTest {
    @Test
    void advancePayStubWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new PayStubWorkflowApplication(
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                MandateApprovalServiceGrpc.newBlockingStub(channel),
                RenewalWorkflowServiceGrpc.newBlockingStub(channel),
                new PayStubWorkflowRequestMapperImpl(),
                NotificationServiceGrpc.newBlockingStub(channel),
                DeductionLifecycleServiceGrpc.newBlockingStub(channel),
                DeductionReportingServiceGrpc.newBlockingStub(channel),
                PayRunValidationServiceGrpc.newBlockingStub(channel),
                PayStubCalculationServiceGrpc.newBlockingStub(channel),
                PayStubQueryServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterWorkflowServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                TourServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(AdvancePayStubWorkflowRequest.getDefaultInstance());
        var response = application.advancePayStubWorkflow(request);
        var sent0 = channel.request("acme.risk.v1.RiskRegisterWorkflowService/RetryRiskRegisterWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub", "lines", "attributes"),
                ProbeSupport.value(sent0, "risk_register", "children", "children", "attributes"));
        var sent1 = channel.request("acme.procurement.v1.PurchaseRequisitionWorkflowService/AdvancePurchaseRequisitionWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub", "attachments", "size_bytes"),
                ProbeSupport.value(sent1, "purchase_requisition", "attachments", "size_bytes"));
        var sent2 = channel.request("acme.payroll.v1.PayStubQueryService/LookupPayStub");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub_id"),
                ProbeSupport.value(sent2, "pay_stub_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubQueryService/LookupPayStub"), "pay_stub", "audit", "updated_by"),
                ProbeSupport.value(response, "pay_stub", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub_id"),
                ProbeSupport.value(response, "pay_stub", "pay_stub_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub", "counterparty"),
                ProbeSupport.value(response, "pay_stub", "counterparty"));
    }
}
