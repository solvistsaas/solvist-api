-- ============================================================
-- MIGRATION: F68 — BETA HARDENING
-- ============================================================

CREATE TABLE IF NOT EXISTS public.portfolio_scans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ip_address TEXT,
    systems_analyzed INTEGER,
    opportunities_detected INTEGER,
    total_opportunity_value FLOAT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_installations_company_id
    ON public.installations(company_id);

CREATE INDEX IF NOT EXISTS idx_opportunity_scores_company_month
    ON public.opportunity_scores(company_id, calculated_month DESC);

CREATE INDEX IF NOT EXISTS idx_clients_company_priority
    ON public.clients(company_id, priority_score DESC);

CREATE INDEX IF NOT EXISTS idx_clients_company_score
    ON public.clients(company_id, score DESC);

CREATE INDEX IF NOT EXISTS idx_clients_company_status
    ON public.clients(company_id, status);

CREATE INDEX IF NOT EXISTS idx_portal_leads_client_id
    ON public.portal_leads(client_id);

CREATE INDEX IF NOT EXISTS idx_execution_tracking_company_id
    ON public.execution_tracking(company_id);

CREATE INDEX IF NOT EXISTS idx_portfolio_scans_ip_created_at
    ON public.portfolio_scans(ip_address, created_at DESC);
