"""
PostgreSQL schema definitions for the crawler database.

This module contains the PostgreSQL equivalent of the SQLite schema,
with proper PostgreSQL data types, constraints, and optimizations.
"""

from typing import List, Dict, Any


# PostgreSQL schema for pages database
POSTGRES_PAGES_SCHEMA = """
-- Pages table - stores HTML content and metadata
CREATE TABLE IF NOT EXISTS pages (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL UNIQUE,
    headers_json TEXT,
    html_compressed BYTEA,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (url_id) REFERENCES urls (id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_pages_url_id ON pages(url_id);
CREATE INDEX IF NOT EXISTS idx_pages_created_at ON pages(created_at);
"""


# PostgreSQL schema for crawl database
POSTGRES_CRAWL_SCHEMA = """
-- URLs table - stores discovered URLs and their metadata
CREATE TABLE IF NOT EXISTS urls (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    kind TEXT CHECK (kind IN ('html','sitemap','sitemap_index','image','asset','other')),
    classification TEXT CHECK (classification IN ('internal','subdomain','network','external','social')),
    discovered_from_id INTEGER,
    first_seen INTEGER,
    last_seen INTEGER,
    headers_compressed BYTEA,
    FOREIGN KEY (discovered_from_id) REFERENCES urls (id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_urls_url ON urls(url);
CREATE INDEX IF NOT EXISTS idx_urls_kind ON urls(kind);
CREATE INDEX IF NOT EXISTS idx_urls_classification ON urls(classification);
CREATE INDEX IF NOT EXISTS idx_urls_first_seen ON urls(first_seen);
CREATE INDEX IF NOT EXISTS idx_urls_last_seen ON urls(last_seen);

-- HTML languages normalization table (referenced by content)
CREATE TABLE IF NOT EXISTS html_languages (
    id SERIAL PRIMARY KEY,
    language_code TEXT UNIQUE NOT NULL  -- e.g., 'en', 'en-US', 'fr-CA'
);

CREATE INDEX IF NOT EXISTS idx_html_languages_code ON html_languages(language_code);

-- Meta descriptions normalization table (referenced by content)
CREATE TABLE IF NOT EXISTS meta_descriptions (
    id SERIAL PRIMARY KEY,
    description TEXT UNIQUE NOT NULL  -- The actual meta description text
);

CREATE INDEX IF NOT EXISTS idx_meta_descriptions_text ON meta_descriptions(description);

-- Content table - stores extracted content from HTML pages
CREATE TABLE IF NOT EXISTS content (
    url_id INTEGER PRIMARY KEY,
    title TEXT,
    meta_description_id INTEGER,
    h1_tags TEXT,
    h2_tags TEXT,
    word_count INTEGER,
    html_lang_id INTEGER,
    internal_links_count INTEGER DEFAULT 0,
    external_links_count INTEGER DEFAULT 0,
    internal_links_unique_count INTEGER DEFAULT 0,
    external_links_unique_count INTEGER DEFAULT 0,
    crawl_depth INTEGER DEFAULT 0,
    inlinks_count INTEGER DEFAULT 0,
    inlinks_unique_count INTEGER DEFAULT 0,
    content_hash_sha256 TEXT,
    content_hash_simhash TEXT,
    content_length INTEGER,
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (meta_description_id) REFERENCES meta_descriptions (id),
    FOREIGN KEY (html_lang_id) REFERENCES html_languages (id)
);

-- Create indexes for content table
CREATE INDEX IF NOT EXISTS idx_content_title ON content(title);
CREATE INDEX IF NOT EXISTS idx_content_word_count ON content(word_count);
CREATE INDEX IF NOT EXISTS idx_content_content_hash_sha256 ON content(content_hash_sha256);
CREATE INDEX IF NOT EXISTS idx_content_content_hash_simhash ON content(content_hash_simhash);

-- Anchor texts normalization table
CREATE TABLE IF NOT EXISTS anchor_texts (
    id SERIAL PRIMARY KEY,
    text TEXT UNIQUE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anchor_texts_text ON anchor_texts(text);

-- Fragments normalization table
CREATE TABLE IF NOT EXISTS fragments (
    id SERIAL PRIMARY KEY,
    fragment TEXT UNIQUE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fragments_fragment ON fragments(fragment);


-- XPaths normalization table
CREATE TABLE IF NOT EXISTS xpaths (
    id SERIAL PRIMARY KEY,
    xpath TEXT UNIQUE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_xpaths_xpath ON xpaths(xpath);

-- Internal links table - stores links between pages
CREATE TABLE IF NOT EXISTS internal_links (
    id SERIAL PRIMARY KEY,
    source_url_id INTEGER NOT NULL,
    target_url_id INTEGER,  -- NULL if target doesn't exist in our DB yet
    anchor_text_id INTEGER,  -- Reference to normalized anchor text
    xpath_id INTEGER,  -- Reference to normalized xpath
    href_url_id INTEGER NOT NULL,
    fragment_id INTEGER,  -- Reference to normalized fragment
    url_parameters TEXT,
    discovered_at INTEGER NOT NULL,
    FOREIGN KEY (source_url_id) REFERENCES urls (id),
    FOREIGN KEY (target_url_id) REFERENCES urls (id),
    FOREIGN KEY (anchor_text_id) REFERENCES anchor_texts (id),
    FOREIGN KEY (xpath_id) REFERENCES xpaths (id),
    FOREIGN KEY (href_url_id) REFERENCES urls (id),
    FOREIGN KEY (fragment_id) REFERENCES fragments (id)
);

-- Create indexes for internal links
CREATE INDEX IF NOT EXISTS idx_internal_links_source ON internal_links(source_url_id);
CREATE INDEX IF NOT EXISTS idx_internal_links_target ON internal_links(target_url_id);
CREATE INDEX IF NOT EXISTS idx_internal_links_discovered_at ON internal_links(discovered_at);

-- Robots directive strings normalization table
CREATE TABLE IF NOT EXISTS robots_directive_strings (
    id SERIAL PRIMARY KEY,
    directive TEXT UNIQUE NOT NULL  -- e.g., 'noindex', 'nofollow', 'noarchive'
);

CREATE INDEX IF NOT EXISTS idx_robots_directive_strings_directive ON robots_directive_strings(directive);

-- Robots directives table - stores robots.txt and meta robots directives
CREATE TABLE IF NOT EXISTS robots_directives (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    source TEXT CHECK (source IN ('robots_txt', 'html_meta', 'http_header')) NOT NULL,
    directive_id INTEGER NOT NULL,  -- Reference to normalized directive
    value TEXT,  -- directive value if applicable
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (directive_id) REFERENCES robots_directive_strings (id)
);

-- Create indexes for robots directives
CREATE INDEX IF NOT EXISTS idx_robots_directives_url ON robots_directives(url_id);
CREATE INDEX IF NOT EXISTS idx_robots_directives_source ON robots_directives(source);

-- Canonical URLs table - stores canonical URL relationships
CREATE TABLE IF NOT EXISTS canonical_urls (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    canonical_url_id INTEGER NOT NULL,
    source TEXT CHECK (source IN ('html_head', 'http_header')) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (canonical_url_id) REFERENCES urls (id)
);

-- Create indexes for canonical URLs
CREATE INDEX IF NOT EXISTS idx_canonical_urls_url ON canonical_urls(url_id);
CREATE INDEX IF NOT EXISTS idx_canonical_urls_canonical ON canonical_urls(canonical_url_id);

-- Hreflang languages normalization table
CREATE TABLE IF NOT EXISTS hreflang_languages (
    id SERIAL PRIMARY KEY,
    language_code TEXT UNIQUE NOT NULL  -- e.g., 'en-us', 'fr-ca', 'x-default'
);

CREATE INDEX IF NOT EXISTS idx_hreflang_languages_code ON hreflang_languages(language_code);

-- Hreflang sitemap table - stores hreflang data from sitemaps
CREATE TABLE IF NOT EXISTS hreflang_sitemap (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    hreflang_id INTEGER NOT NULL,
    href_url_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (hreflang_id) REFERENCES hreflang_languages (id),
    FOREIGN KEY (href_url_id) REFERENCES urls (id)
);

-- Create indexes for hreflang sitemap
CREATE INDEX IF NOT EXISTS idx_hreflang_sitemap_url ON hreflang_sitemap(url_id);
CREATE INDEX IF NOT EXISTS idx_hreflang_sitemap_hreflang ON hreflang_sitemap(hreflang_id);

-- Hreflang HTTP header table - stores hreflang data from HTTP headers
CREATE TABLE IF NOT EXISTS hreflang_http_header (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    hreflang_id INTEGER NOT NULL,
    href_url_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (hreflang_id) REFERENCES hreflang_languages (id),
    FOREIGN KEY (href_url_id) REFERENCES urls (id)
);

-- Create indexes for hreflang HTTP header
CREATE INDEX IF NOT EXISTS idx_hreflang_http_header_url ON hreflang_http_header(url_id);
CREATE INDEX IF NOT EXISTS idx_hreflang_http_header_hreflang ON hreflang_http_header(hreflang_id);

-- Hreflang HTML head table - stores hreflang data from HTML head
CREATE TABLE IF NOT EXISTS hreflang_html_head (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    hreflang_id INTEGER NOT NULL,
    href_url_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (hreflang_id) REFERENCES hreflang_languages (id),
    FOREIGN KEY (href_url_id) REFERENCES urls (id)
);

-- Create indexes for hreflang HTML head
CREATE INDEX IF NOT EXISTS idx_hreflang_html_head_url ON hreflang_html_head(url_id);
CREATE INDEX IF NOT EXISTS idx_hreflang_html_head_hreflang ON hreflang_html_head(hreflang_id);

-- Page metadata table - stores HTTP status codes and redirect information
CREATE TABLE IF NOT EXISTS page_metadata (
    url_id INTEGER PRIMARY KEY,
    initial_status_code INTEGER,
    final_status_code INTEGER,
    final_url_id INTEGER,
    redirect_destination_url_id INTEGER,
    fetched_at INTEGER,
    etag TEXT,
    last_modified TEXT,
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (final_url_id) REFERENCES urls (id),
    FOREIGN KEY (redirect_destination_url_id) REFERENCES urls (id)
);

-- Create indexes for page metadata
CREATE INDEX IF NOT EXISTS idx_page_metadata_initial_status ON page_metadata(initial_status_code);
CREATE INDEX IF NOT EXISTS idx_page_metadata_final_status ON page_metadata(final_status_code);
CREATE INDEX IF NOT EXISTS idx_page_metadata_etag ON page_metadata(etag);
CREATE INDEX IF NOT EXISTS idx_page_metadata_last_modified ON page_metadata(last_modified);

-- Indexability table - stores robots.txt and meta robots indexability information
CREATE TABLE IF NOT EXISTS indexability (
    url_id INTEGER PRIMARY KEY,
    robots_txt_allows BOOLEAN,
    html_meta_allows BOOLEAN,
    http_header_allows BOOLEAN,
    overall_indexable BOOLEAN,
    robots_txt_directives TEXT,
    html_meta_directives TEXT,
    http_header_directives TEXT,
    robots_txt_reason TEXT,  -- Reason why robots.txt disallows (if applicable)
    html_meta_reason TEXT,   -- Reason why HTML meta disallows (if applicable)
    http_header_reason TEXT, -- Reason why HTTP header disallows (if applicable)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (url_id) REFERENCES urls (id)
);

-- Create indexes for indexability
CREATE INDEX IF NOT EXISTS idx_indexability_overall ON indexability(overall_indexable);
CREATE INDEX IF NOT EXISTS idx_indexability_robots_txt ON indexability(robots_txt_allows);

-- Redirects table - stores redirect chain information
CREATE TABLE IF NOT EXISTS redirects (
    id SERIAL PRIMARY KEY,
    source_url_id INTEGER NOT NULL,  -- Original URL that was requested
    target_url_id INTEGER NOT NULL,  -- Final URL after redirects
    redirect_chain TEXT NOT NULL,    -- JSON array of [{"url": "...", "status": 301, "headers": {...}}, ...]
    chain_length INTEGER NOT NULL,   -- Number of redirects in the chain
    final_status_code INTEGER NOT NULL,  -- Final HTTP status code
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_url_id) REFERENCES urls (id),
    FOREIGN KEY (target_url_id) REFERENCES urls (id)
);

-- Create indexes for redirects
CREATE INDEX IF NOT EXISTS idx_redirects_source ON redirects(source_url_id);
CREATE INDEX IF NOT EXISTS idx_redirects_target ON redirects(target_url_id);
CREATE INDEX IF NOT EXISTS idx_redirects_chain_length ON redirects(chain_length);
CREATE INDEX IF NOT EXISTS idx_redirects_final_status ON redirects(final_status_code);
-- Ensure ON CONFLICT (source_url_id) is valid
CREATE UNIQUE INDEX IF NOT EXISTS idx_redirects_source_unique ON redirects(source_url_id);

-- Frontier table - manages the crawl queue
CREATE TABLE IF NOT EXISTS frontier (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    depth INTEGER NOT NULL,
    parent_id INTEGER,
    status TEXT NOT NULL CHECK (status IN ('queued','pending','done')),
    enqueued_at INTEGER,
    updated_at INTEGER,
    priority_score REAL DEFAULT 0.0,
    sitemap_priority REAL DEFAULT 0.5,
    inlinks_count INTEGER DEFAULT 0,
    content_type_score REAL DEFAULT 1.0,
    reset_count INTEGER DEFAULT 0,
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (parent_id) REFERENCES urls (id),
    UNIQUE(url_id)
);

-- Create indexes for frontier
CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
CREATE INDEX IF NOT EXISTS idx_frontier_depth ON frontier(depth);
CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier(priority_score DESC);
CREATE INDEX IF NOT EXISTS idx_frontier_enqueued_at ON frontier(enqueued_at);

-- Sitemaps table - stores discovered sitemaps
CREATE TABLE IF NOT EXISTS sitemaps (
    id SERIAL PRIMARY KEY,
    sitemap_url TEXT UNIQUE NOT NULL,
    discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_crawled_at TIMESTAMP WITH TIME ZONE,
    total_urls_found INTEGER DEFAULT 0,
    is_sitemap_index BOOLEAN DEFAULT FALSE,
    parent_sitemap_id INTEGER,
    FOREIGN KEY (parent_sitemap_id) REFERENCES sitemaps (id)
);

-- Create indexes for sitemaps
CREATE INDEX IF NOT EXISTS idx_sitemaps_url ON sitemaps(sitemap_url);
CREATE INDEX IF NOT EXISTS idx_sitemaps_discovered_at ON sitemaps(discovered_at);
CREATE INDEX IF NOT EXISTS idx_sitemaps_last_crawled ON sitemaps(last_crawled_at);

-- URL-Sitemap relationships table
CREATE TABLE IF NOT EXISTS url_sitemaps (
    url_id INTEGER NOT NULL,
    sitemap_id INTEGER NOT NULL,
    position INTEGER,  -- Position in sitemap (if available)
    discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (url_id, sitemap_id),
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (sitemap_id) REFERENCES sitemaps (id)
);

-- Create indexes for url_sitemaps
CREATE INDEX IF NOT EXISTS idx_url_sitemaps_url ON url_sitemaps(url_id);
CREATE INDEX IF NOT EXISTS idx_url_sitemaps_sitemap ON url_sitemaps(sitemap_id);

-- Failed URLs table - stores URLs that failed to crawl for retry
CREATE TABLE IF NOT EXISTS failed_urls (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    status_code INTEGER NOT NULL,  -- The HTTP status code that caused the failure
    failure_reason TEXT,  -- Additional failure details (timeout, connection error, etc.)
    retry_count INTEGER DEFAULT 0,  -- Number of retry attempts made
    next_retry_at TIMESTAMP WITH TIME ZONE,  -- When to retry this URL
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (url_id) REFERENCES urls (id)
);

-- Create indexes for failed URLs
CREATE INDEX IF NOT EXISTS idx_failed_urls_url ON failed_urls(url_id);
CREATE INDEX IF NOT EXISTS idx_failed_urls_status ON failed_urls(status_code);
CREATE INDEX IF NOT EXISTS idx_failed_urls_next_retry ON failed_urls(next_retry_at);
CREATE INDEX IF NOT EXISTS idx_failed_urls_retry_count ON failed_urls(retry_count);

-- Schema types normalization table
CREATE TABLE IF NOT EXISTS schema_types (
    id SERIAL PRIMARY KEY,
    type_name TEXT UNIQUE NOT NULL  -- e.g., 'Article', 'Product', 'Organization', 'BreadcrumbList'
);

CREATE INDEX IF NOT EXISTS idx_schema_types_name ON schema_types(type_name);

-- Schema instances table - stores unique schema instances with content hashing
CREATE TABLE IF NOT EXISTS schema_instances (
    id SERIAL PRIMARY KEY,
    content_hash TEXT UNIQUE NOT NULL,  -- SHA256 hash of normalized content
    schema_type_id INTEGER NOT NULL,
    format TEXT CHECK (format IN ('json-ld', 'microdata', 'rdfa')) NOT NULL,
    raw_data TEXT NOT NULL,  -- The original structured data
    parsed_data TEXT,  -- The parsed/normalized structured data (JSON)
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    validation_errors TEXT,  -- JSON array of validation errors
    severity TEXT CHECK (severity IN ('info', 'warning', 'error', 'critical')) DEFAULT 'info',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (schema_type_id) REFERENCES schema_types (id)
);

-- Create indexes for schema instances
CREATE INDEX IF NOT EXISTS idx_schema_instances_content_hash ON schema_instances(content_hash);
CREATE INDEX IF NOT EXISTS idx_schema_instances_type ON schema_instances(schema_type_id);
CREATE INDEX IF NOT EXISTS idx_schema_instances_format ON schema_instances(format);
CREATE INDEX IF NOT EXISTS idx_schema_instances_is_valid ON schema_instances(is_valid);

-- Page-Schema references table - links pages to schema instances
CREATE TABLE IF NOT EXISTS page_schema_references (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    schema_instance_id INTEGER NOT NULL,
    position INTEGER,  -- Position on page (for multiple instances of same type)
    property_name TEXT,  -- For nested properties (e.g., 'image', 'breadcrumb', 'video')
    is_main_entity BOOLEAN DEFAULT FALSE,  -- Whether this is the main entity for the page
    parent_entity_id INTEGER,  -- Reference to parent entity if this is a nested property
    discovered_at INTEGER NOT NULL,
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (schema_instance_id) REFERENCES schema_instances (id),
    FOREIGN KEY (parent_entity_id) REFERENCES page_schema_references (id)
);

-- Create indexes for page schema references
CREATE INDEX IF NOT EXISTS idx_page_schema_references_url ON page_schema_references(url_id);
CREATE INDEX IF NOT EXISTS idx_page_schema_references_schema ON page_schema_references(schema_instance_id);
CREATE INDEX IF NOT EXISTS idx_page_schema_references_position ON page_schema_references(position);

-- Legacy schema_data table (for backward compatibility)
CREATE TABLE IF NOT EXISTS schema_data (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    schema_type_id INTEGER NOT NULL,
    format TEXT CHECK (format IN ('json-ld', 'microdata', 'rdfa')) NOT NULL,
    raw_data TEXT NOT NULL,  -- The original structured data (JSON for JSON-LD, HTML for microdata/rdfa)
    parsed_data TEXT,  -- The parsed/normalized structured data (JSON)
    position INTEGER,  -- Position on page (for multiple instances of same type)
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    validation_errors TEXT,  -- JSON array of validation errors
    severity TEXT CHECK (severity IN ('info', 'warning', 'error', 'critical')) DEFAULT 'info',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (url_id) REFERENCES urls (id),
    FOREIGN KEY (schema_type_id) REFERENCES schema_types (id)
);

-- Create indexes for legacy schema_data
CREATE INDEX IF NOT EXISTS idx_schema_data_url ON schema_data(url_id);
CREATE INDEX IF NOT EXISTS idx_schema_data_type ON schema_data(schema_type_id);
CREATE INDEX IF NOT EXISTS idx_schema_data_format ON schema_data(format);
CREATE INDEX IF NOT EXISTS idx_schema_data_is_valid ON schema_data(is_valid);
"""


def get_postgres_schema_statements() -> List[str]:
    """Get PostgreSQL schema statements as a list."""
    # Combine both schemas - crawl schema first (contains urls table), then pages schema
    full_schema = POSTGRES_CRAWL_SCHEMA + "\n" + POSTGRES_PAGES_SCHEMA
    
    # Remove comments and split by semicolon
    import re
    # Remove single-line comments
    schema_clean = re.sub(r'--.*$', '', full_schema, flags=re.MULTILINE)
    # Remove multi-line comments
    schema_clean = re.sub(r'/\*.*?\*/', '', schema_clean, flags=re.DOTALL)
    
    # Split by semicolon and clean up
    statements = []
    for statement in schema_clean.split(';'):
        statement = statement.strip()
        if statement:
            statements.append(statement)
    
    return statements


def get_postgres_pages_schema_statements() -> List[str]:
    """Get PostgreSQL pages schema statements as a list."""
    statements = []
    for statement in POSTGRES_PAGES_SCHEMA.split(';'):
        statement = statement.strip()
        if statement and not statement.startswith('--'):
            statements.append(statement)
    
    return statements


def get_postgres_crawl_schema_statements() -> List[str]:
    """Get PostgreSQL crawl schema statements as a list."""
    statements = []
    for statement in POSTGRES_CRAWL_SCHEMA.split(';'):
        statement = statement.strip()
        if statement and not statement.startswith('--'):
            statements.append(statement)
    
    return statements
