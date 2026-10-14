package com.acme.finance.domains.compliance;

import com.acme.cart.v1.Cart;
import com.acme.cart.v1.CartCalculationServiceGrpc;
import com.acme.cart.v1.CompareCartOptionsRequest;
import com.acme.cart.v1.CompareCartOptionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CalculateSanctionRequest;
import com.acme.compliance.v1.CalculateSanctionResponse;
import com.acme.compliance.v1.CountSanctionTotalsRequest;
import com.acme.compliance.v1.CountSanctionTotalsResponse;
import com.acme.compliance.v1.DeleteSanctionReportRequest;
import com.acme.compliance.v1.DeleteSanctionReportResponse;
import com.acme.compliance.v1.FetchSanctionReportRequest;
import com.acme.compliance.v1.FetchSanctionReportResponse;
import com.acme.compliance.v1.GenerateSanctionReportRequest;
import com.acme.compliance.v1.GenerateSanctionReportResponse;
import com.acme.compliance.v1.ListSanctionReportsRequest;
import com.acme.compliance.v1.ListSanctionReportsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.ScheduleSanctionReportRequest;
import com.acme.compliance.v1.ScheduleSanctionReportResponse;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.SuggestLedgerAccountsRequest;
import com.acme.ledger.v1.SuggestLedgerAccountsResponse;
import com.acme.risk.v1.CancelExposureWorkflowRequest;
import com.acme.risk.v1.CancelExposureWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.PauseExposureWorkflowRequest;
import com.acme.risk.v1.PauseExposureWorkflowResponse;
import com.acme.risk.v1.RejectRiskScenarioRequest;
import com.acme.risk.v1.RejectRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.SignalExposureWorkflowRequest;
import com.acme.risk.v1.SignalExposureWorkflowResponse;
import com.acme.search.v1.CheckSearchIndexEligibilityRequest;
import com.acme.search.v1.CheckSearchIndexEligibilityResponse;
import com.acme.subscriptions.v1.CreateSubscriptionRequest;
import com.acme.subscriptions.v1.CreateSubscriptionResponse;
import com.acme.subscriptions.v1.Subscription;
import com.acme.subscriptions.v1.SubscriptionServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class SanctionReportingRequestMapperImpl implements SanctionReportingRequestMapper {
    @Override
    public CompareCartOptionsRequest deleteSanctionReportStage1(DeleteSanctionReportRequest request, CalculateSanctionResponse reply0) {
        return CompareCartOptionsRequest.newBuilder()
                .setCart(Cart.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getSanction().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
    }
}
