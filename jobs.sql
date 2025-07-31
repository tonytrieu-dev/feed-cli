CREATE TABLE IF NOT EXISTS public.jobs (
    hn_id bigint NOT NULL,
    posted_by character varying(255) NOT NULL,
    posted_at timestamp without time zone NOT NULL,
    text text NOT NULL,
    company character varying(255),
    role character varying(255),
    location character varying(255),
    is_remote boolean,
    is_internship boolean,
    is_new_grad boolean,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    parent_id bigint,
    html_text text,
    url text,
    salary_info text,
    keywords text[],
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hn_id)
);

CREATE INDEX IF NOT EXISTS idx_jobs_company ON public.jobs (company) WHERE company IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_internship ON public.jobs (is_internship) WHERE is_internship = true;
CREATE INDEX IF NOT EXISTS idx_jobs_internship_posted ON public.jobs (posted_at DESC) WHERE is_internship = true;
CREATE INDEX IF NOT EXISTS idx_jobs_keywords ON public.jobs USING gin (keywords);
CREATE INDEX IF NOT EXISTS idx_jobs_location ON public.jobs (location) WHERE location IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_new_grad ON public.jobs (is_new_grad) WHERE is_new_grad = true;
CREATE INDEX IF NOT EXISTS idx_jobs_new_grad_posted ON public.jobs (posted_at DESC) WHERE is_new_grad = true;
CREATE INDEX IF NOT EXISTS idx_jobs_parent_id ON public.jobs (parent_id);
