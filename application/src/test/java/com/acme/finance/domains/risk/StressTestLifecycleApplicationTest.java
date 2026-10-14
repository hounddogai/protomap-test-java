package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.SummarizeHouseholdsRequest;
import com.acme.customers.v1.SummarizeHouseholdsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.performance.v1.ListReviewsRequest;
import com.acme.performance.v1.ListReviewsResponse;
import com.acme.performance.v1.ReviewServiceGrpc;
import com.acme.performance.v1.WatchReviewsRequest;
import com.acme.performance.v1.WatchReviewsResponse;
import com.acme.prescriptions.v1.PrescriptionServiceGrpc;
import com.acme.prescriptions.v1.WatchPrescriptionsRequest;
import com.acme.prescriptions.v1.WatchPrescriptionsResponse;
import com.acme.risk.v1.ActivateStressTestRequest;
import com.acme.risk.v1.ActivateStressTestResponse;
import com.acme.risk.v1.BrowseRiskScenariosRequest;
import com.acme.risk.v1.BrowseRiskScenariosResponse;
import com.acme.risk.v1.CalculateStressTestRequest;
import com.acme.risk.v1.CalculateStressTestResponse;
import com.acme.risk.v1.ExpireStressTestRequest;
import com.acme.risk.v1.ExpireStressTestResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.ListRiskRegistersRequest;
import com.acme.risk.v1.ListRiskRegistersResponse;
import com.acme.risk.v1.ReinstateStressTestRequest;
import com.acme.risk.v1.ReinstateStressTestResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.ScheduleStressTestExpiryRequest;
import com.acme.risk.v1.ScheduleStressTestExpiryResponse;
import com.acme.risk.v1.SimulateExposureRequest;
import com.acme.risk.v1.SimulateExposureResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.SuspendStressTestRequest;
import com.acme.risk.v1.SuspendStressTestResponse;
import com.acme.risk.v1.TerminateStressTestRequest;
import com.acme.risk.v1.TerminateStressTestResponse;
import com.acme.risk.v1.UpdateRiskRegisterRequest;
import com.acme.risk.v1.UpdateRiskRegisterResponse;
import com.acme.suppliers.v1.SuggestSupplierContractsRequest;
import com.acme.suppliers.v1.SuggestSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class StressTestLifecycleApplicationTest {
    @Test
    void scheduleStressTestExpiryPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var listener = new StressTestLifecycleApplicationEvents(RiskRegisterServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof StressTestLifecycleApplication.ActivateStressTestStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new StressTestLifecycleApplication(
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                events,
                new StressTestLifecycleRequestMapperImpl(),
                ReviewServiceGrpc.newBlockingStub(channel),
                PrescriptionServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureCalculationServiceGrpc.newBlockingStub(channel),
                RiskRegisterServiceGrpc.newBlockingStub(channel),
                RiskScenarioQueryServiceGrpc.newBlockingStub(channel),
                StressTestCalculationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ScheduleStressTestExpiryRequest.getDefaultInstance());
        var response = application.scheduleStressTestExpiry(request);
        var sent0 = channel.request("acme.suppliers.v1.SupplierRatingValidationService/ValidateSupplierRating");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "billing_address", "recipient"),
                ProbeSupport.value(sent0, "supplier_rating", "billing_address", "recipient"));
        var sent1 = channel.request("acme.customers.v1.HouseholdQueryService/SummarizeHouseholds");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent1, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "metadata"),
                ProbeSupport.value(sent1, "details", "filters"));
        var sent2 = channel.request("acme.venues.v1.VenueQueryService/SuggestVenues");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/ValidateSupplierRating"), "supplier_rating", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        var sent3 = channel.request("acme.risk.v1.RiskScenarioQueryService/BrowseRiskScenarios");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/ValidateSupplierRating"), "supplier_rating", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "lines", "line_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "lines", "attributes"),
                ProbeSupport.value(sent3, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/ValidateSupplierRating"), "supplier_rating", "counterparty", "customer_id"),
                ProbeSupport.value(response, "stress_test", "counterparty", "customer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/ValidateSupplierRating"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdQueryService/SummarizeHouseholds"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueQueryService/SuggestVenues"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test_id"),
                ProbeSupport.value(response, "stress_test", "stress_test_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "metadata"),
                ProbeSupport.value(response, "stress_test", "metadata"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new StressTestLifecycleApplicationEvents(RiskRegisterServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<UpdateRiskRegisterResponse>();
        listener.on(new StressTestLifecycleApplication.ActivateStressTestStage1Requested(ProbeSupport.sample(UpdateRiskRegisterRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
