package com.acme.finance.domains.insurance;

import com.acme.anomalies.v1.AnomalyQueryServiceGrpc;
import com.acme.anomalies.v1.ResolveAnomaliesRequest;
import com.acme.anomalies.v1.ResolveAnomaliesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionRequest;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionResponse;
import com.acme.authorization.v1.SummarizeAccessPoliciesRequest;
import com.acme.authorization.v1.SummarizeAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.CountFeatureFlagsRequest;
import com.acme.featureflags.v1.CountFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.CalculatePremiumRequest;
import com.acme.insurance.v1.CalculatePremiumResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.CompareClaimOptionsRequest;
import com.acme.insurance.v1.CompareClaimOptionsResponse;
import com.acme.insurance.v1.ComparePremiumOptionsRequest;
import com.acme.insurance.v1.ComparePremiumOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageRouteAction;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.EstimatePremiumRequest;
import com.acme.insurance.v1.EstimatePremiumResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.ExplainPremiumCalculationRequest;
import com.acme.insurance.v1.ExplainPremiumCalculationResponse;
import com.acme.insurance.v1.GetPremiumRatesRequest;
import com.acme.insurance.v1.GetPremiumRatesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.QuotePremiumRequest;
import com.acme.insurance.v1.QuotePremiumResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.insurance.v1.SimulatePremiumRequest;
import com.acme.insurance.v1.SimulatePremiumResponse;
import com.acme.insurance.v1.ValidateClaimRequest;
import com.acme.insurance.v1.ValidateClaimResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.acme.recruiting.v1.CalculateJobOpeningRequest;
import com.acme.recruiting.v1.CalculateJobOpeningResponse;
import com.acme.recruiting.v1.InterviewRef;
import com.acme.recruiting.v1.JobOpening;
import com.acme.shows.v1.CalculateShowRequest;
import com.acme.shows.v1.CalculateShowResponse;
import com.acme.shows.v1.QuoteShowRequest;
import com.acme.shows.v1.QuoteShowResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowCalculationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class PremiumCalculationRequestMapperImpl implements PremiumCalculationRequestMapper {
    @Override
    public EstimatePolicyRequest comparePremiumOptionsStage2(ComparePremiumOptionsRequest request) {
        return EstimatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    @Override
    public CompareClaimOptionsRequest simulatePremiumStage2(SimulatePremiumRequest request, CalculateCoverageResponse reply0) {
        return CompareClaimOptionsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getCoverage().getOwner().getDisplayName())
                                .build())
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(request.getPremium().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareClaimOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getPremium().getBillingAddress().getLocality())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
    }
}
