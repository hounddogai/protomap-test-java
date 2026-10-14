package com.acme.finance.domains.suppliers;

import com.acme.assets.v1.ApproveFixedAssetRequest;
import com.acme.assets.v1.ApproveFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.FindAccessPoliciesRequest;
import com.acme.authorization.v1.FindAccessPoliciesResponse;
import com.acme.authorization.v1.ResolveAccessPoliciesRequest;
import com.acme.authorization.v1.ResolveAccessPoliciesResponse;
import com.acme.banking.v1.ApproveMandateRequest;
import com.acme.banking.v1.ApproveMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ShareForecastReportRequest;
import com.acme.budgeting.v1.ShareForecastReportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.ScheduleAttestationReportRequest;
import com.acme.compliance.v1.ScheduleAttestationReportResponse;
import com.acme.contracts.v1.PauseRenewalWorkflowRequest;
import com.acme.contracts.v1.PauseRenewalWorkflowResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalWorkflowServiceGrpc;
import com.acme.contracts.v1.ResumeRenewalWorkflowRequest;
import com.acme.contracts.v1.ResumeRenewalWorkflowResponse;
import com.acme.customers.v1.CompareCustomerSegmentOptionsRequest;
import com.acme.customers.v1.CompareCustomerSegmentOptionsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.drivers.v1.CreateDriverRequest;
import com.acme.drivers.v1.CreateDriverResponse;
import com.acme.drivers.v1.Driver;
import com.acme.drivers.v1.DriverServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.risk.v1.ListPendingRiskRegistersRequest;
import com.acme.risk.v1.ListPendingRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.suppliers.v1.ApproveSupplierContractRequest;
import com.acme.suppliers.v1.ApproveSupplierContractResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.CommentSupplierContractApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierContractApprovalResponse;
import com.acme.suppliers.v1.DelegateSupplierContractApprovalRequest;
import com.acme.suppliers.v1.DelegateSupplierContractApprovalResponse;
import com.acme.suppliers.v1.DeleteSupplierRequest;
import com.acme.suppliers.v1.DeleteSupplierResponse;
import com.acme.suppliers.v1.EscalateSupplierContractRequest;
import com.acme.suppliers.v1.EscalateSupplierContractResponse;
import com.acme.suppliers.v1.GetSupplierContractApprovalStatusRequest;
import com.acme.suppliers.v1.GetSupplierContractApprovalStatusResponse;
import com.acme.suppliers.v1.ListPendingSupplierContractsRequest;
import com.acme.suppliers.v1.ListPendingSupplierContractsResponse;
import com.acme.suppliers.v1.LookupSupplierRequest;
import com.acme.suppliers.v1.LookupSupplierResponse;
import com.acme.suppliers.v1.PeekSupplierContractRequest;
import com.acme.suppliers.v1.PeekSupplierContractResponse;
import com.acme.suppliers.v1.RecallSupplierContractRequest;
import com.acme.suppliers.v1.RecallSupplierContractResponse;
import com.acme.suppliers.v1.RejectSupplierContractRequest;
import com.acme.suppliers.v1.RejectSupplierContractResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.SubmitSupplierContractRequest;
import com.acme.suppliers.v1.SubmitSupplierContractResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingBankSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
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

class SupplierContractApprovalApplicationTest {
    @Test
    void submitSupplierContractPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new SuppliersRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveSupplier(String tenant, Supplier value) {}
            public void saveSupplierContract(String tenant, SupplierContract value) {}
            public void saveSupplierRating(String tenant, SupplierRating value) {}
            public void saveSupplierRisk(String tenant, SupplierRisk value) {}
        };
        var listener = new SupplierContractApprovalApplicationEvents(MetricSeriesServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof SupplierContractApprovalApplication.GetSupplierContractApprovalStatusStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new SupplierContractApprovalApplication(
                FixedAssetApprovalServiceGrpc.newBlockingStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                MandateApprovalServiceGrpc.newBlockingStub(channel),
                ForecastReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                RenewalWorkflowServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                DriverServiceGrpc.newBlockingStub(channel),
                events,
                repository,
                RiskRegisterApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newFutureStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SubmitSupplierContractRequest.getDefaultInstance());
        var response = application.submitSupplierContract(request);
        var sent0 = channel.request("acme.risk.v1.RiskRegisterApprovalService/ListPendingRiskRegisters");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.authorization.v1.AccessPolicyQueryService/FindAccessPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "counterparty", "supplier_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "metadata"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.budgeting.v1.ForecastReportingService/ShareForecastReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent2, "annotations"));
        var sent3 = channel.request("acme.suppliers.v1.SupplierRatingValidationService/ValidateSupplierRating");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "counterparty", "email"),
                ProbeSupport.value(sent3, "supplier_rating", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "bank", "account_holder"),
                ProbeSupport.value(sent3, "supplier_rating", "bank", "account_holder"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterApprovalService/ListPendingRiskRegisters"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterApprovalService/ListPendingRiskRegisters"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyQueryService/FindAccessPolicies"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastReportingService/ShareForecastReport"), "forecast", "billing_address", "recipient"),
                ProbeSupport.value(response, "supplier_contract", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/ValidateSupplierRating"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract_id"),
                ProbeSupport.value(response, "supplier_contract", "supplier_contract_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "counterparty"),
                ProbeSupport.value(response, "supplier_contract", "counterparty"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new SupplierContractApprovalApplicationEvents(MetricSeriesServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ListMetricSeriesesResponse>();
        listener.on(new SupplierContractApprovalApplication.GetSupplierContractApprovalStatusStage0Requested(ProbeSupport.sample(ListMetricSeriesesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
