package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.ScheduleDisposalReportRequest;
import com.acme.assets.v1.ScheduleDisposalReportResponse;
import com.acme.banking.v1.DelegateStandingOrderApprovalRequest;
import com.acme.banking.v1.DelegateStandingOrderApprovalResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.budgeting.v1.AdvanceBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.AdvanceBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowRequest;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetWorkflowServiceGrpc;
import com.acme.budgeting.v1.CancelBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.CancelBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastApprovalServiceGrpc;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.InspectBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.InspectBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.ListBudgetLineWorkflowsRequest;
import com.acme.budgeting.v1.ListBudgetLineWorkflowsResponse;
import com.acme.budgeting.v1.ListPendingForecastsRequest;
import com.acme.budgeting.v1.ListPendingForecastsResponse;
import com.acme.budgeting.v1.ListVarianceReportsRequest;
import com.acme.budgeting.v1.ListVarianceReportsResponse;
import com.acme.budgeting.v1.ResolveForecastConflictRequest;
import com.acme.budgeting.v1.ResolveForecastConflictResponse;
import com.acme.budgeting.v1.ResumeBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.ResumeBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.RetryBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.RetryBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.SignalBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.SignalBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.SignalBudgetWorkflowRequest;
import com.acme.budgeting.v1.SignalBudgetWorkflowResponse;
import com.acme.budgeting.v1.StartBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.StartBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.ScoreUserRequest;
import com.acme.identity.v1.ScoreUserResponse;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodAdminServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RepairFiscalPeriodRequest;
import com.acme.ledger.v1.RepairFiscalPeriodResponse;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralApprovalServiceGrpc;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.SubmitCollateralRequest;
import com.acme.lending.v1.SubmitCollateralResponse;
import com.acme.suppliers.v1.ExportSupplierContractsRequest;
import com.acme.suppliers.v1.ExportSupplierContractsResponse;
import com.acme.suppliers.v1.ExportSuppliersRequest;
import com.acme.suppliers.v1.ExportSuppliersResponse;
import com.acme.suppliers.v1.GetSupplierRequest;
import com.acme.suppliers.v1.GetSupplierResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class BudgetLineWorkflowApplicationTest {
    @Test
    void signalBudgetLineWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new BudgetLineWorkflowApplicationEvents(UserServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BudgetLineWorkflowApplication.AdvanceBudgetLineWorkflowStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BudgetLineWorkflowApplication(
                DisposalReportingServiceGrpc.newBlockingStub(channel),
                StandingOrderApprovalServiceGrpc.newBlockingStub(channel),
                BudgetWorkflowServiceGrpc.newBlockingStub(channel),
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                VarianceReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                UserValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodAdminServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                CollateralApprovalServiceGrpc.newBlockingStub(channel),
                repository,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SignalBudgetLineWorkflowRequest.getDefaultInstance());
        var response = application.signalBudgetLineWorkflow(request);
        var sent0 = channel.request("acme.suppliers.v1.SupplierContractQueryService/ExportSupplierContracts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierService/GetSupplier");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "counterparty", "supplier_id"),
                ProbeSupport.value(sent1, "supplier_id"));
        var sent2 = channel.request("acme.assets.v1.DisposalReportingService/ScheduleDisposalReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent2, "reason"));
        var sent3 = channel.request("acme.budgeting.v1.VarianceReportingService/ListVarianceReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/GetSupplier"), "supplier", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/GetSupplier"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalReportingService/ScheduleDisposalReport"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalReportingService/ScheduleDisposalReport"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceReportingService/ListVarianceReports"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceReportingService/ListVarianceReports"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line_id"),
                ProbeSupport.value(response, "budget_line", "budget_line_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "status"),
                ProbeSupport.value(response, "budget_line", "status"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new BudgetLineWorkflowApplicationEvents(UserServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<UpdateUserResponse>();
        listener.on(new BudgetLineWorkflowApplication.AdvanceBudgetLineWorkflowStage1Requested(ProbeSupport.sample(UpdateUserRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
