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
import com.acme.finance.testing.ProbeSupport;
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
import com.acme.payroll.v1.PayRun;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class TaxWithholdingApprovalApplicationTest {
    @Test
    void commentTaxWithholdingApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var listener = new TaxWithholdingApprovalApplicationEvents(UserServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof TaxWithholdingApprovalApplication.ApproveTaxWithholdingStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new TaxWithholdingApprovalApplication(
                ChannelRegistry.fixed(channel),
                ArticleServiceGrpc.newBlockingStub(channel),
                ObligationApprovalServiceGrpc.newBlockingStub(channel),
                events,
                PremiumApprovalServiceGrpc.newBlockingStub(channel),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                DeductionLifecycleServiceGrpc.newBlockingStub(channel),
                PayStubApprovalServiceGrpc.newBlockingStub(channel),
                TaxWithholdingReportingServiceGrpc.newBlockingStub(channel),
                TaxWithholdingValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(CommentTaxWithholdingApprovalRequest.getDefaultInstance());
        var response = application.commentTaxWithholdingApproval(request);
        var sent0 = channel.request("acme.contracts.v1.ObligationApprovalService/RejectObligation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "details", "approver", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "owner", "display_name"),
                ProbeSupport.value(sent0, "obligation", "assignee", "display_name"));
        var sent1 = channel.request("acme.payroll.v1.TaxWithholdingReportingService/ListTaxWithholdingReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.identity.v1.UserService/GetUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "owner", "user_id"),
                ProbeSupport.value(sent2, "user_id"));
        var sent3 = channel.request("acme.payroll.v1.PayStubApprovalService/RejectPayStub");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "audit", "updated_by"),
                ProbeSupport.value(sent3, "pay_stub", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationApprovalService/RejectObligation"), "obligation", "customer", "email"),
                ProbeSupport.value(response, "tax_withholding", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationApprovalService/RejectObligation"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingReportingService/ListTaxWithholdingReports"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingReportingService/ListTaxWithholdingReports"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/GetUser"), "user", "relationship_manager", "display_name"),
                ProbeSupport.value(response, "tax_withholding", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/GetUser"), "user", "audit", "etag"),
                ProbeSupport.value(response, "tax_withholding", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubApprovalService/RejectPayStub"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding_id"),
                ProbeSupport.value(response, "tax_withholding", "tax_withholding_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "version"),
                ProbeSupport.value(response, "tax_withholding", "version"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new TaxWithholdingApprovalApplicationEvents(UserServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<GetUserResponse>();
        listener.on(new TaxWithholdingApprovalApplication.ApproveTaxWithholdingStage1Requested(ProbeSupport.sample(GetUserRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
