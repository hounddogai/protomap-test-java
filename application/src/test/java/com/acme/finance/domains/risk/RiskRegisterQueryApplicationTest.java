package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.CreateAuditEventRequest;
import com.acme.audit.v1.CreateAuditEventResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LintPostingBatchesRequest;
import com.acme.ledger.v1.LintPostingBatchesResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ScorePostingBatchRequest;
import com.acme.ledger.v1.ScorePostingBatchResponse;
import com.acme.risk.v1.BrowseRiskRegistersRequest;
import com.acme.risk.v1.BrowseRiskRegistersResponse;
import com.acme.risk.v1.CalculateExposureRequest;
import com.acme.risk.v1.CalculateExposureResponse;
import com.acme.risk.v1.CompareExposureOptionsRequest;
import com.acme.risk.v1.CompareExposureOptionsResponse;
import com.acme.risk.v1.EstimateExposureRequest;
import com.acme.risk.v1.EstimateExposureResponse;
import com.acme.risk.v1.ExportRiskRegistersRequest;
import com.acme.risk.v1.ExportRiskRegistersResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.FindRiskRegistersRequest;
import com.acme.risk.v1.FindRiskRegistersResponse;
import com.acme.risk.v1.LookupRiskRegisterRequest;
import com.acme.risk.v1.LookupRiskRegisterResponse;
import com.acme.risk.v1.PeekRiskRegisterRequest;
import com.acme.risk.v1.PeekRiskRegisterResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.SuggestRiskRegistersRequest;
import com.acme.risk.v1.SuggestRiskRegistersResponse;
import com.acme.risk.v1.SummarizeRiskRegistersRequest;
import com.acme.risk.v1.SummarizeRiskRegistersResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class RiskRegisterQueryApplicationTest {
    @Test
    void exportRiskRegistersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var application = new RiskRegisterQueryApplication(
                AuditEventServiceGrpc.newStub(channel),
                AuditEventServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newFutureStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                new RiskRegisterQueryRequestMapperImpl(),
                repository,
                ExposureCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportRiskRegistersRequest.getDefaultInstance());
        var response = application.exportRiskRegisters(request);
        var sent0 = channel.request("acme.audit.v1.AuditEventService/CreateAuditEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "audit_event", "measurements", "observed_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent0, "audit_event", "source"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "audit_event", "attributes"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "deadline"),
                ProbeSupport.value(sent0, "audit_event", "occurred_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventService/CreateAuditEvent"), "audit_event", "subject", "display_name"),
                ProbeSupport.value(response, "risk_register", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventService/CreateAuditEvent"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "risk_register", "location", "region"));
        var endpoint = new RiskRegisterQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportRiskRegistersResponse> observer = new StreamObserver<>() {
            public void onNext(ExportRiskRegistersResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportRiskRegisters(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeRiskRegistersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var application = new RiskRegisterQueryApplication(
                AuditEventServiceGrpc.newStub(channel),
                AuditEventServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newFutureStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                new RiskRegisterQueryRequestMapperImpl(),
                repository,
                ExposureCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SummarizeRiskRegistersRequest.getDefaultInstance());
        var response = application.summarizeRiskRegisters(request);
        var sent0 = channel.request("acme.risk.v1.ExposureCalculationService/CalculateExposure");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "exposure", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "deadline"),
                ProbeSupport.value(sent0, "exposure", "audit", "updated_at"));
        var sent1 = channel.request("acme.risk.v1.ExposureCalculationService/CompareExposureOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/CalculateExposure"), "exposure", "audit", "updated_by"),
                ProbeSupport.value(sent1, "exposure", "audit", "updated_by"));
        var sent2 = channel.request("acme.ledger.v1.PostingBatchValidationService/LintPostingBatches");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/CalculateExposure"), "exposure", "audit", "updated_at"),
                ProbeSupport.value(sent2, "posting_batch", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/CompareExposureOptions"), "exposure", "holder", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/CalculateExposure"), "exposure", "contact", "phone"),
                ProbeSupport.value(sent2, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/CalculateExposure"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/CompareExposureOptions"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/LintPostingBatches"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
