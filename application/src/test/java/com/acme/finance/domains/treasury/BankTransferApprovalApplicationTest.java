package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyDiscountAction;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityRequest;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityResponse;
import com.acme.authorization.v1.FindAccessPoliciesRequest;
import com.acme.authorization.v1.FindAccessPoliciesResponse;
import com.acme.authorization.v1.ScoreAccessPolicyRequest;
import com.acme.authorization.v1.ScoreAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.configuration.v1.ConfigSetServiceGrpc;
import com.acme.configuration.v1.SearchConfigSetsRequest;
import com.acme.configuration.v1.SearchConfigSetsResponse;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportAttachment;
import com.acme.expenses.v1.ExpenseReportWorkflowServiceGrpc;
import com.acme.expenses.v1.PauseExpenseReportWorkflowRequest;
import com.acme.expenses.v1.PauseExpenseReportWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCardSettlement;
import com.acme.payroll.v1.PayStubWorkflowServiceGrpc;
import com.acme.payroll.v1.SignalPayStubWorkflowRequest;
import com.acme.payroll.v1.SignalPayStubWorkflowResponse;
import com.acme.treasury.v1.ApproveBankTransferRequest;
import com.acme.treasury.v1.ApproveBankTransferResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferLifecycleServiceGrpc;
import com.acme.treasury.v1.BankTransferLine;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionApprovalServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CommentBankTransferApprovalRequest;
import com.acme.treasury.v1.CommentBankTransferApprovalResponse;
import com.acme.treasury.v1.DelegateBankTransferApprovalRequest;
import com.acme.treasury.v1.DelegateBankTransferApprovalResponse;
import com.acme.treasury.v1.DelegateCashPositionApprovalRequest;
import com.acme.treasury.v1.DelegateCashPositionApprovalResponse;
import com.acme.treasury.v1.EscalateBankTransferRequest;
import com.acme.treasury.v1.EscalateBankTransferResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.ListPendingBankTransfersRequest;
import com.acme.treasury.v1.ListPendingBankTransfersResponse;
import com.acme.treasury.v1.ReinstateBankTransferRequest;
import com.acme.treasury.v1.ReinstateBankTransferResponse;
import com.acme.treasury.v1.SubmitBankTransferRequest;
import com.acme.treasury.v1.SubmitBankTransferResponse;
import com.acme.treasury.v1.SyncLiquidityForecastsRequest;
import com.acme.treasury.v1.SyncLiquidityForecastsResponse;
import com.acme.yards.v1.SimulateYardRequest;
import com.acme.yards.v1.SimulateYardResponse;
import com.acme.yards.v1.Yard;
import com.acme.yards.v1.YardCalculationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class BankTransferApprovalApplicationTest {
    @Test
    void submitBankTransferPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new BankTransferApprovalApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ConfigSetServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                ExpenseReportWorkflowServiceGrpc.newBlockingStub(channel),
                PayStubWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                BankTransferLifecycleServiceGrpc.newBlockingStub(channel),
                CashPositionApprovalServiceGrpc.newBlockingStub(channel),
                LiquidityForecastSyncServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SubmitBankTransferRequest.getDefaultInstance());
        var response = application.submitBankTransfer(request);
        var sent0 = channel.request("acme.authorization.v1.AccessPolicyValidationService/ScoreAccessPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "audit", "updated_by"),
                ProbeSupport.value(sent0, "access_policy", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "billing_address", "postal_code"),
                ProbeSupport.value(sent0, "details", "destination", "postal_code"));
        var sent1 = channel.request("acme.treasury.v1.BankTransferLifecycleService/ReinstateBankTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "bank_transfer_id"),
                ProbeSupport.value(sent1, "bank_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "counterparty", "email"),
                ProbeSupport.value(sent1, "details", "new_owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "display_name"),
                ProbeSupport.value(sent1, "details", "new_owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyValidationService/ScoreAccessPolicy"), "access_policy", "owner", "email"),
                ProbeSupport.value(sent1, "details", "notify_contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyValidationService/ScoreAccessPolicy"), "access_policy", "owner", "user_id"),
                ProbeSupport.value(sent1, "details", "new_owner", "user_id"));
        var sent2 = channel.request("acme.audit.v1.AuditEventValidationService/ScoreAuditEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferLifecycleService/ReinstateBankTransfer"), "bank_transfer", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyValidationService/ScoreAccessPolicy"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferLifecycleService/ReinstateBankTransfer"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventValidationService/ScoreAuditEvent"), "audit_event", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventValidationService/ScoreAuditEvent"), "audit_event", "attributes"),
                ProbeSupport.value(response, "bank_transfer", "lines", "attributes"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer_id"),
                ProbeSupport.value(response, "bank_transfer", "bank_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "bank_transfer", "metadata"),
                ProbeSupport.value(response, "bank_transfer", "metadata"));
    }
}
