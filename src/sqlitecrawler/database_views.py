"""
Database views for SQLiteCrawler.

This module contains all the analysis views that provide convenient access
to crawled data in a structured format.
"""

# SQLite database views
SQLITE_DATABASE_VIEWS = """
-- Main crawl overview view
CREATE VIEW IF NOT EXISTS view_crawl_overview AS
SELECT 
    u.url,
    COALESCE(f.status, 'unknown') as crawl_status,
    pm.initial_status_code as status_code,
    i.overall_indexable as indexable,
    -- Add indexability reason when not indexable
    CASE 
        WHEN i.overall_indexable = 0 THEN
            CASE 
                WHEN i.robots_txt_allows = 0 THEN 'blocked by robots.txt'
                WHEN i.html_meta_allows = 0 THEN 'blocked by meta robots'
                WHEN i.http_header_allows = 0 THEN 'blocked by HTTP headers'
                WHEN pm.initial_status_code != 200 THEN 'not 200 status'
                WHEN canonical_urls_table.url IS NOT NULL AND canonical_urls_table.url != u.url THEN 'not self canonical'
                ELSE 'unknown reason'
            END
        ELSE NULL
    END as indexability_reason,
    u.kind as type,
    u.classification,
    c.title,
    md.description as meta_description,
    -- Split h1_tags into h1-1 and h1-2, drop others, add count
    (SELECT h1_text FROM (
        SELECT h1_text, ROW_NUMBER() OVER (ORDER BY h1_text) as rn
        FROM (
            SELECT TRIM(value) as h1_text 
            FROM json_each(CASE 
                WHEN c.h1_tags IS NULL OR c.h1_tags = '' THEN '[]'
                WHEN c.h1_tags LIKE '[%' THEN c.h1_tags
                ELSE '["' || REPLACE(REPLACE(c.h1_tags, '"', '\"'), ',', '","') || '"]'
            END)
            WHERE TRIM(value) != ''
        )
    ) WHERE rn = 1) as h1_1,
    (SELECT h1_text FROM (
        SELECT h1_text, ROW_NUMBER() OVER (ORDER BY h1_text) as rn
        FROM (
            SELECT TRIM(value) as h1_text 
            FROM json_each(CASE 
                WHEN c.h1_tags IS NULL OR c.h1_tags = '' THEN '[]'
                WHEN c.h1_tags LIKE '[%' THEN c.h1_tags
                ELSE '["' || REPLACE(REPLACE(c.h1_tags, '"', '\"'), ',', '","') || '"]'
            END)
            WHERE TRIM(value) != ''
        )
    ) WHERE rn = 2) as h1_2,
    (SELECT COUNT(*) FROM (
        SELECT TRIM(value) as h1_text 
        FROM json_each(CASE 
            WHEN c.h1_tags IS NULL OR c.h1_tags = '' THEN '[]'
            WHEN c.h1_tags LIKE '[%' THEN c.h1_tags
            ELSE '["' || REPLACE(REPLACE(c.h1_tags, '"', '\"'), ',', '","') || '"]'
        END)
        WHERE TRIM(value) != ''
    )) as h1_count,
    -- Convert h2_tags to CSV string and add count
    (SELECT GROUP_CONCAT(TRIM(value)) FROM (
        SELECT TRIM(value) as value
        FROM json_each(CASE 
            WHEN c.h2_tags IS NULL OR c.h2_tags = '' THEN '[]'
            WHEN c.h2_tags LIKE '[%' THEN c.h2_tags
            ELSE '["' || REPLACE(REPLACE(c.h2_tags, '"', '\"'), ',', '","') || '"]'
        END)
        WHERE TRIM(value) != ''
    )) as h2_tags,
    (SELECT COUNT(*) FROM (
        SELECT TRIM(value) as h2_text 
        FROM json_each(CASE 
            WHEN c.h2_tags IS NULL OR c.h2_tags = '' THEN '[]'
            WHEN c.h2_tags LIKE '[%' THEN c.h2_tags
            ELSE '["' || REPLACE(REPLACE(c.h2_tags, '"', '\"'), ',', '","') || '"]'
        END)
        WHERE TRIM(value) != ''
    )) as h2_count,
    c.word_count,
    hl.language_code as html_lang,
    c.internal_links_count,
    c.external_links_count,
    c.internal_links_unique_count,
    c.external_links_unique_count,
    c.crawl_depth,
    c.inlinks_count,
    c.inlinks_unique_count,
    i.robots_txt_allows,
    i.html_meta_allows,
    i.http_header_allows,
    COALESCE(i.robots_txt_directives, '') as robots_txt_directives,
    COALESCE(i.html_meta_directives, '') as html_meta_directives,
    COALESCE(i.http_header_directives, '') as http_header_directives,
    GROUP_CONCAT(DISTINCT canonical_urls_table.url) as canonical_urls,
    GROUP_CONCAT(DISTINCT cu.source) as canonical_sources,
    redirect_dest.url as redirect_destination_url,
    -- Find the hreflang language that points to this page itself (excluding x-default)
    (SELECT hl_self.language_code 
     FROM hreflang_sitemap hs_self 
     JOIN hreflang_languages hl_self ON hs_self.hreflang_id = hl_self.id 
     JOIN urls href_self ON hs_self.href_url_id = href_self.id 
     WHERE hs_self.url_id = u.id 
       AND href_self.url = u.url 
       AND hl_self.language_code != 'x-default') as self_hreflang_language,
    -- Count total hreflang entries for this page
    (SELECT COUNT(*) 
     FROM hreflang_sitemap hs_count 
     WHERE hs_count.url_id = u.id) as hreflang_count,
    -- Get the x-default hreflang URL if it exists
    (SELECT href_xdefault.url 
     FROM hreflang_sitemap hs_xdefault 
     JOIN hreflang_languages hl_xdefault ON hs_xdefault.hreflang_id = hl_xdefault.id 
     JOIN urls href_xdefault ON hs_xdefault.href_url_id = href_xdefault.id 
     WHERE hs_xdefault.url_id = u.id 
       AND hl_xdefault.language_code = 'x-default') as x_default_hreflang_url,
    -- Schema data summary
    (SELECT GROUP_CONCAT(DISTINCT st.type_name) 
     FROM page_schema_references psr 
     JOIN schema_instances si ON psr.schema_instance_id = si.id 
     JOIN schema_types st ON si.schema_type_id = st.id 
     WHERE psr.url_id = u.id AND psr.is_main_entity = 1) as main_schema_types,
    (SELECT COUNT(*) 
     FROM page_schema_references psr 
     WHERE psr.url_id = u.id AND psr.is_main_entity = 1) as main_schema_count,
    (SELECT COUNT(*) 
     FROM page_schema_references psr 
     WHERE psr.url_id = u.id) as total_schema_count,
    -- Content hash information
    c.content_hash_sha256,
    c.content_hash_simhash,
    c.content_length,
    -- Timestamps
    u.first_seen,
    u.last_seen,
    pm.fetched_at,
    f.enqueued_at,
    f.updated_at
FROM urls u
LEFT JOIN frontier f ON u.id = f.url_id
LEFT JOIN content c ON u.id = c.url_id
LEFT JOIN meta_descriptions md ON c.meta_description_id = md.id
LEFT JOIN html_languages hl ON c.html_lang_id = hl.id
LEFT JOIN page_metadata pm ON u.id = pm.url_id
LEFT JOIN indexability i ON u.id = i.url_id
LEFT JOIN canonical_urls cu ON u.id = cu.url_id
LEFT JOIN urls canonical_urls_table ON cu.canonical_url_id = canonical_urls_table.id
LEFT JOIN urls redirect_dest ON pm.redirect_destination_url_id = redirect_dest.id
WHERE u.classification IN ('internal', 'network')
  AND u.url NOT LIKE '%#%'
GROUP BY u.id;

-- Internal links view
CREATE VIEW IF NOT EXISTS view_links_internal AS
SELECT DISTINCT
    source.url as source_url,
    target.url as target_url,
    at.text as anchor_text,
    x.xpath,
    f.fragment,
    il.url_parameters,
    MIN(il.discovered_at) as discovered_at,
    CASE WHEN at.text IS NULL THEN 1 ELSE 0 END as is_image,
    CASE WHEN f.fragment IS NOT NULL THEN 1 ELSE 0 END as has_fragment,
    CASE WHEN il.url_parameters IS NOT NULL THEN 1 ELSE 0 END as has_parameters,
    COALESCE(
        pm.initial_status_code,
        (SELECT pm_redirect.initial_status_code FROM page_metadata pm_redirect WHERE pm_redirect.final_url_id = target.id LIMIT 1)
    ) as target_status_code,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM canonical_urls cu 
            JOIN urls canonical_target ON cu.canonical_url_id = canonical_target.id 
            WHERE cu.url_id = target.id AND canonical_target.url = target.url
        ) THEN 1 
        ELSE 0 
    END as target_is_self_canonical,
    CASE 
        WHEN pm.url_id IS NOT NULL AND (pm.final_url_id IS NULL OR pm.final_url_id = target.id) THEN 1
        ELSE 0
    END as target_resolves
FROM internal_links il
JOIN urls source ON il.source_url_id = source.id
LEFT JOIN urls target ON il.target_url_id = target.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id
LEFT JOIN fragments f ON il.fragment_id = f.id
LEFT JOIN page_metadata pm ON target.id = pm.url_id
WHERE source.classification = 'internal' AND (target.classification = 'internal' OR target.classification IS NULL)
GROUP BY source.url, target.url, at.text, x.xpath, f.fragment, il.url_parameters, target_status_code, target_resolves;

-- Network links view
CREATE VIEW IF NOT EXISTS view_links_network AS
SELECT 
    source.url as source_url,
    target.url as target_url,
    at.text as anchor_text,
    x.xpath,
    f.fragment,
    il.url_parameters,
    il.discovered_at,
    CASE WHEN at.text IS NULL THEN 1 ELSE 0 END as is_image,
    CASE WHEN f.fragment IS NOT NULL THEN 1 ELSE 0 END as has_fragment,
    CASE WHEN il.url_parameters IS NOT NULL THEN 1 ELSE 0 END as has_parameters
FROM internal_links il
JOIN urls source ON il.source_url_id = source.id
LEFT JOIN urls target ON il.target_url_id = target.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id
LEFT JOIN fragments f ON il.fragment_id = f.id
WHERE source.classification = 'internal' AND target.classification = 'network';

-- External links view
CREATE VIEW IF NOT EXISTS view_links_external AS
SELECT DISTINCT
    source.url as source_url,
    target.url as target_url,
    at.text as anchor_text,
    x.xpath,
    f.fragment,
    il.url_parameters,
    MIN(il.discovered_at) as discovered_at,
    CASE WHEN at.text IS NULL THEN 1 ELSE 0 END as is_image,
    CASE WHEN f.fragment IS NOT NULL THEN 1 ELSE 0 END as has_fragment,
    CASE WHEN il.url_parameters IS NOT NULL THEN 1 ELSE 0 END as has_parameters,
    COALESCE(
        pm.initial_status_code,
        (SELECT pm_redirect.initial_status_code FROM page_metadata pm_redirect WHERE pm_redirect.final_url_id = target.id LIMIT 1)
    ) as target_status_code,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM canonical_urls cu 
            JOIN urls canonical_target ON cu.canonical_url_id = canonical_target.id 
            WHERE cu.url_id = target.id AND canonical_target.url = target.url
        ) THEN 1 
        ELSE 0 
    END as target_is_self_canonical,
    CASE 
        WHEN pm.url_id IS NOT NULL AND (pm.final_url_id IS NULL OR pm.final_url_id = target.id) THEN 1
        ELSE 0
    END as target_resolves
FROM internal_links il
JOIN urls source ON il.source_url_id = source.id
LEFT JOIN urls target ON il.target_url_id = target.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id
LEFT JOIN fragments f ON il.fragment_id = f.id
LEFT JOIN page_metadata pm ON target.id = pm.url_id
WHERE source.classification = 'internal' AND target.classification = 'external'
GROUP BY source.url, target.url, at.text, x.xpath, f.fragment, il.url_parameters, target_status_code, target_resolves;

-- Invalid content links view (redirects, non-canonical, or non-200 status)
CREATE VIEW IF NOT EXISTS view_links_invalid_content AS
SELECT 
    source_url,
    target_url,
    anchor_text,
    xpath,
    fragment,
    url_parameters,
    discovered_at,
    is_image,
    has_fragment,
    has_parameters,
    target_status_code,
    target_is_self_canonical,
    target_resolves,
    CASE 
        WHEN target_resolves = 0 THEN 'redirects'
        WHEN target_is_self_canonical = 0 THEN 'not self-canonical'
        WHEN target_status_code IS NOT NULL AND target_status_code != 200 THEN 'non-200 status'
        ELSE 'unknown issue'
    END as invalid_reason
FROM view_links_internal
WHERE target_resolves = 0 
   OR target_is_self_canonical = 0 
   OR (target_status_code IS NOT NULL AND target_status_code != 200)
UNION ALL
SELECT 
    source_url,
    target_url,
    anchor_text,
    xpath,
    fragment,
    url_parameters,
    discovered_at,
    is_image,
    has_fragment,
    has_parameters,
    target_status_code,
    target_is_self_canonical,
    target_resolves,
    CASE 
        WHEN target_resolves = 0 THEN 'redirects'
        WHEN target_is_self_canonical = 0 THEN 'not self-canonical'
        WHEN target_status_code IS NOT NULL AND target_status_code != 200 THEN 'non-200 status'
        ELSE 'unknown issue'
    END as invalid_reason
FROM view_links_external
WHERE target_resolves = 0 
   OR target_is_self_canonical = 0 
   OR (target_status_code IS NOT NULL AND target_status_code != 200);

-- Subdomain links view
CREATE VIEW IF NOT EXISTS view_links_subdomain AS
SELECT 
    source.url as source_url,
    target.url as target_url,
    at.text as anchor_text,
    x.xpath,
    f.fragment,
    il.url_parameters,
    il.discovered_at,
    CASE WHEN at.text IS NULL THEN 1 ELSE 0 END as is_image,
    CASE WHEN f.fragment IS NOT NULL THEN 1 ELSE 0 END as has_fragment,
    CASE WHEN il.url_parameters IS NOT NULL THEN 1 ELSE 0 END as has_parameters
FROM internal_links il
JOIN urls source ON il.source_url_id = source.id
LEFT JOIN urls target ON il.target_url_id = target.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id
LEFT JOIN fragments f ON il.fragment_id = f.id
WHERE source.classification = 'internal' AND target.classification = 'subdomain';

-- Sitemap statistics view
CREATE VIEW IF NOT EXISTS view_sitemap_statistics AS
SELECT 
    s.sitemap_url,
    s.total_urls_found,
    s.is_sitemap_index,
    s.discovered_at,
    s.last_crawled_at,
    COUNT(us.url_id) as urls_processed,
    COUNT(DISTINCT u.classification) as classification_count,
    GROUP_CONCAT(DISTINCT u.classification) as classifications
FROM sitemaps s
LEFT JOIN url_sitemaps us ON s.id = us.sitemap_id
LEFT JOIN urls u ON us.url_id = u.id
GROUP BY s.id;

-- Schema analysis view
CREATE VIEW IF NOT EXISTS view_schema_analysis AS
SELECT 
    u.url,
    st.type_name as schema_type,
    si.format,
    si.is_valid,
    si.severity,
    si.validation_errors,
    psr.position,
    psr.property_name,
    psr.is_main_entity,
    psr.parent_entity_id,
    psr.discovered_at
FROM page_schema_references psr
JOIN urls u ON psr.url_id = u.id
JOIN schema_instances si ON psr.schema_instance_id = si.id
JOIN schema_types st ON si.schema_type_id = st.id
ORDER BY u.url, psr.position;

-- Schema hierarchy view
CREATE VIEW IF NOT EXISTS view_schema_hierarchy AS
SELECT 
    u.url,
    st.type_name as schema_type,
    psr.position,
    psr.property_name,
    psr.is_main_entity,
    parent_psr.position as parent_position,
    parent_st.type_name as parent_schema_type,
    psr.discovered_at
FROM page_schema_references psr
JOIN urls u ON psr.url_id = u.id
JOIN schema_instances si ON psr.schema_instance_id = si.id
JOIN schema_types st ON si.schema_type_id = st.id
LEFT JOIN page_schema_references parent_psr ON psr.parent_entity_id = parent_psr.id
LEFT JOIN schema_instances parent_si ON parent_psr.schema_instance_id = parent_si.id
LEFT JOIN schema_types parent_st ON parent_si.schema_type_id = parent_st.id
ORDER BY u.url, psr.position;

-- Crawl status view
CREATE VIEW IF NOT EXISTS view_crawl_status AS
SELECT 
    f.status,
    COUNT(*) as count,
    AVG(f.priority_score) as avg_priority,
    MIN(f.enqueued_at) as oldest_enqueued,
    MAX(f.updated_at) as last_updated
FROM frontier f
GROUP BY f.status;

-- UTM links view
CREATE VIEW IF NOT EXISTS view_utm_links AS
SELECT 
    source.url as source_url,
    target.url as target_url,
    at.text as anchor_text,
    il.url_parameters,
    il.discovered_at
FROM internal_links il
JOIN urls source ON il.source_url_id = source.id
LEFT JOIN urls target ON il.target_url_id = target.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
WHERE il.url_parameters LIKE '%utm_%';

-- Hubs view (pages with many outbound links)
CREATE VIEW IF NOT EXISTS view_hubs AS
SELECT 
    u.url,
    c.title,
    c.internal_links_count,
    c.external_links_count,
    c.internal_links_count + c.external_links_count as total_links,
    c.word_count,
    c.crawl_depth
FROM urls u
JOIN content c ON u.id = c.url_id
WHERE c.internal_links_count + c.external_links_count > 50
ORDER BY total_links DESC;

-- Exact duplicates view
CREATE VIEW IF NOT EXISTS view_exact_duplicates AS
SELECT 
    content_hash_sha256,
    COUNT(*) as duplicate_count,
    GROUP_CONCAT(u.url) as urls
FROM content c
JOIN urls u ON c.url_id = u.id
WHERE content_hash_sha256 IS NOT NULL
GROUP BY content_hash_sha256
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Near duplicates view (using simhash)
CREATE VIEW IF NOT EXISTS view_near_duplicates AS
SELECT 
    c1.url_id as url1_id,
    u1.url as url1,
    c2.url_id as url2_id,
    u2.url as url2,
    c1.content_hash_simhash,
    c2.content_hash_simhash,
    c1.word_count as word_count1,
    c2.word_count as word_count2
FROM content c1
JOIN urls u1 ON c1.url_id = u1.id
JOIN content c2 ON c1.content_hash_simhash = c2.content_hash_simhash
JOIN urls u2 ON c2.url_id = u2.id
WHERE c1.url_id < c2.url_id
  AND c1.content_hash_simhash IS NOT NULL
  AND c2.content_hash_simhash IS NOT NULL;

-- Content hash statistics view
CREATE VIEW IF NOT EXISTS view_content_hash_stats AS
SELECT 
    COUNT(*) as total_pages,
    COUNT(content_hash_sha256) as pages_with_sha256,
    COUNT(content_hash_simhash) as pages_with_simhash,
    COUNT(DISTINCT content_hash_sha256) as unique_sha256_hashes,
    COUNT(DISTINCT content_hash_simhash) as unique_simhash_hashes,
    AVG(word_count) as avg_word_count,
    MIN(word_count) as min_word_count,
    MAX(word_count) as max_word_count
FROM content;
"""

# PostgreSQL database views (simplified versions)
POSTGRES_DATABASE_VIEWS = """
-- Main crawl overview view (PostgreSQL version) - aligned with SQLite fields
CREATE OR REPLACE VIEW view_crawl_overview AS
SELECT 
    u.url,
    COALESCE(f.status, 'unknown') as crawl_status,
    pm.initial_status_code as status_code,
    i.overall_indexable as indexable,
    CASE 
        WHEN i.overall_indexable = FALSE THEN
            CASE 
                WHEN i.robots_txt_allows = FALSE THEN 'blocked by robots.txt'
                WHEN i.html_meta_allows = FALSE THEN 'blocked by meta robots'
                WHEN i.http_header_allows = FALSE THEN 'blocked by HTTP headers'
                WHEN pm.initial_status_code != 200 THEN 'not 200 status'
                WHEN EXISTS (
                    SELECT 1 FROM canonical_urls cu_check 
                    JOIN urls canonical_urls_table_check ON cu_check.canonical_url_id = canonical_urls_table_check.id 
                    WHERE cu_check.url_id = u.id AND canonical_urls_table_check.url != u.url
                ) THEN 'not self canonical'
                ELSE 'unknown reason'
            END
        ELSE NULL
    END as indexability_reason,
    u.kind as type,
    u.classification,
    c.title,
    md.description as meta_description,
    -- h1 tags: first and second, plus count
    (SELECT elem FROM jsonb_array_elements_text(COALESCE(NULLIF(c.h1_tags, ''), '[]')::jsonb) WITH ORDINALITY t(elem, ord) WHERE btrim(elem) <> '' ORDER BY ord LIMIT 1) as h1_1,
    (SELECT elem FROM jsonb_array_elements_text(COALESCE(NULLIF(c.h1_tags, ''), '[]')::jsonb) WITH ORDINALITY t(elem, ord) WHERE btrim(elem) <> '' ORDER BY ord OFFSET 1 LIMIT 1) as h1_2,
    (SELECT COUNT(*) FROM jsonb_array_elements_text(COALESCE(NULLIF(c.h1_tags, ''), '[]')::jsonb) AS t(elem) WHERE btrim(elem) <> '') as h1_count,
    -- h2 tags collapsed and counted
    (SELECT string_agg(btrim(elem), ', ') FROM jsonb_array_elements_text(COALESCE(NULLIF(c.h2_tags, ''), '[]')::jsonb) AS t(elem) WHERE btrim(elem) <> '') as h2_tags,
    (SELECT COUNT(*) FROM jsonb_array_elements_text(COALESCE(NULLIF(c.h2_tags, ''), '[]')::jsonb) AS t(elem) WHERE btrim(elem) <> '') as h2_count,
    c.word_count,
    hl.language_code as html_lang,
    c.internal_links_count,
    c.external_links_count,
    c.internal_links_unique_count,
    c.external_links_unique_count,
    c.crawl_depth,
    c.inlinks_count,
    c.inlinks_unique_count,
    i.robots_txt_allows,
    i.html_meta_allows,
    i.http_header_allows,
    COALESCE(i.robots_txt_directives, '') as robots_txt_directives,
    COALESCE(i.html_meta_directives, '') as html_meta_directives,
    COALESCE(i.http_header_directives, '') as http_header_directives,
    (SELECT array_agg(DISTINCT canonical_urls_table.url) FROM canonical_urls cu_agg JOIN urls canonical_urls_table ON cu_agg.canonical_url_id = canonical_urls_table.id WHERE cu_agg.url_id = u.id) as canonical_urls,
    (SELECT array_agg(DISTINCT cu_agg.source) FROM canonical_urls cu_agg WHERE cu_agg.url_id = u.id) as canonical_sources,
    redirect_dest.url as redirect_destination_url,
    -- Find the hreflang language that points to this page itself (excluding x-default)
    (SELECT hl_self.language_code 
     FROM hreflang_html_head hs_self 
     JOIN hreflang_languages hl_self ON hs_self.hreflang_id = hl_self.id 
     JOIN urls href_self ON hs_self.href_url_id = href_self.id 
     WHERE hs_self.url_id = u.id 
       AND href_self.url = u.url 
       AND hl_self.language_code != 'x-default'
     LIMIT 1) as self_hreflang_language,
    -- Count total hreflang entries for this page
    (SELECT COUNT(*) 
     FROM hreflang_html_head hs_count 
     WHERE hs_count.url_id = u.id) as hreflang_count,
    -- Get the x-default hreflang URL if it exists
    (SELECT href_xdefault.url 
     FROM hreflang_html_head hs_xdefault 
     JOIN hreflang_languages hl_xdefault ON hs_xdefault.hreflang_id = hl_xdefault.id 
     JOIN urls href_xdefault ON hs_xdefault.href_url_id = href_xdefault.id 
     WHERE hs_xdefault.url_id = u.id 
       AND hl_xdefault.language_code = 'x-default'
     LIMIT 1) as x_default_hreflang_url,
    -- Schema data summary
    (SELECT array_agg(DISTINCT st.type_name) 
     FROM page_schema_references psr 
     JOIN schema_instances si ON psr.schema_instance_id = si.id 
     JOIN schema_types st ON si.schema_type_id = st.id 
     WHERE psr.url_id = u.id AND psr.is_main_entity = TRUE) as main_schema_types,
    (SELECT COUNT(*) 
     FROM page_schema_references psr 
     WHERE psr.url_id = u.id AND psr.is_main_entity = TRUE) as main_schema_count,
    (SELECT COUNT(*) 
     FROM page_schema_references psr 
     WHERE psr.url_id = u.id) as total_schema_count,
    -- Content hash information
    c.content_hash_sha256,
    c.content_hash_simhash,
    c.content_length,
    -- Timestamps
    u.first_seen,
    u.last_seen,
    pm.fetched_at,
    f.enqueued_at,
    f.updated_at
FROM urls u
LEFT JOIN frontier f ON u.id = f.url_id
LEFT JOIN content c ON u.id = c.url_id
LEFT JOIN meta_descriptions md ON c.meta_description_id = md.id
LEFT JOIN html_languages hl ON c.html_lang_id = hl.id
LEFT JOIN page_metadata pm ON u.id = pm.url_id
LEFT JOIN indexability i ON u.id = i.url_id
LEFT JOIN urls redirect_dest ON pm.redirect_destination_url_id = redirect_dest.id
WHERE u.classification IN ('internal', 'network')
  AND u.url NOT LIKE '%#%'
GROUP BY u.id, f.status, pm.initial_status_code, i.overall_indexable, u.kind, u.classification, c.title, md.description, c.h1_tags, c.h2_tags, c.word_count, hl.language_code, c.internal_links_count, c.external_links_count, c.internal_links_unique_count, c.external_links_unique_count, c.crawl_depth, c.inlinks_count, c.inlinks_unique_count, i.robots_txt_allows, i.html_meta_allows, i.http_header_allows, i.robots_txt_directives, i.html_meta_directives, i.http_header_directives, redirect_dest.url, c.content_hash_sha256, c.content_hash_simhash, c.content_length, u.first_seen, u.last_seen, pm.fetched_at, f.enqueued_at, f.updated_at;

-- Internal links view (PostgreSQL version)
CREATE OR REPLACE VIEW view_links_internal AS
SELECT DISTINCT ON (source.url, target.url, at.text, x.xpath, f.fragment, il.url_parameters)
    source.url as source_url,
    target.url as target_url,
    at.text as anchor_text,
    x.xpath,
    f.fragment,
    il.url_parameters,
    il.discovered_at,
    CASE WHEN at.text IS NULL THEN 1 ELSE 0 END as is_image,
    CASE WHEN f.fragment IS NOT NULL THEN 1 ELSE 0 END as has_fragment,
    CASE WHEN il.url_parameters IS NOT NULL THEN 1 ELSE 0 END as has_parameters,
    COALESCE(
        pm.initial_status_code,
        (SELECT pm_redirect.initial_status_code FROM page_metadata pm_redirect WHERE pm_redirect.final_url_id = target.id LIMIT 1)
    ) as target_status_code,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM canonical_urls cu 
            JOIN urls canonical_target ON cu.canonical_url_id = canonical_target.id 
            WHERE cu.url_id = target.id AND canonical_target.url = target.url
        ) THEN 1 
        ELSE 0 
    END as target_is_self_canonical,
    CASE 
        WHEN pm.url_id IS NOT NULL AND (pm.final_url_id IS NULL OR pm.final_url_id = target.id) THEN 1
        ELSE 0
    END as target_resolves
FROM internal_links il
JOIN urls source ON il.source_url_id = source.id
LEFT JOIN urls target ON il.target_url_id = target.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id
LEFT JOIN fragments f ON il.fragment_id = f.id
LEFT JOIN page_metadata pm ON target.id = pm.url_id
WHERE source.classification = 'internal' AND (target.classification = 'internal' OR target.classification IS NULL)
ORDER BY source.url, target.url, at.text, x.xpath, f.fragment, il.url_parameters, il.discovered_at;

-- External links view (PostgreSQL version)
CREATE OR REPLACE VIEW view_links_external AS
SELECT DISTINCT ON (source.url, target.url, at.text, x.xpath, f.fragment, il.url_parameters)
    source.url as source_url,
    target.url as target_url,
    at.text as anchor_text,
    x.xpath,
    f.fragment,
    il.url_parameters,
    il.discovered_at,
    CASE WHEN at.text IS NULL THEN 1 ELSE 0 END as is_image,
    CASE WHEN f.fragment IS NOT NULL THEN 1 ELSE 0 END as has_fragment,
    CASE WHEN il.url_parameters IS NOT NULL THEN 1 ELSE 0 END as has_parameters,
    COALESCE(
        pm.initial_status_code,
        (SELECT pm_redirect.initial_status_code FROM page_metadata pm_redirect WHERE pm_redirect.final_url_id = target.id LIMIT 1)
    ) as target_status_code,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM canonical_urls cu 
            JOIN urls canonical_target ON cu.canonical_url_id = canonical_target.id 
            WHERE cu.url_id = target.id AND canonical_target.url = target.url
        ) THEN 1 
        ELSE 0 
    END as target_is_self_canonical,
    CASE 
        WHEN pm.url_id IS NOT NULL AND (pm.final_url_id IS NULL OR pm.final_url_id = target.id) THEN 1
        ELSE 0
    END as target_resolves
FROM internal_links il
JOIN urls source ON il.source_url_id = source.id
LEFT JOIN urls target ON il.target_url_id = target.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id
LEFT JOIN fragments f ON il.fragment_id = f.id
LEFT JOIN page_metadata pm ON target.id = pm.url_id
WHERE source.classification = 'internal' AND target.classification = 'external'
ORDER BY source.url, target.url, at.text, x.xpath, f.fragment, il.url_parameters, il.discovered_at;

-- Invalid content links view (PostgreSQL version) - redirects, non-canonical, or non-200 status
CREATE OR REPLACE VIEW view_links_invalid_content AS
SELECT 
    source_url,
    target_url,
    anchor_text,
    xpath,
    fragment,
    url_parameters,
    discovered_at,
    is_image,
    has_fragment,
    has_parameters,
    target_status_code,
    target_is_self_canonical,
    target_resolves,
    CASE 
        WHEN target_resolves = 0 THEN 'redirects'
        WHEN target_is_self_canonical = 0 THEN 'not self-canonical'
        WHEN target_status_code IS NOT NULL AND target_status_code != 200 THEN 'non-200 status'
        ELSE 'unknown issue'
    END as invalid_reason
FROM view_links_internal
WHERE target_resolves = 0 
   OR target_is_self_canonical = 0 
   OR (target_status_code IS NOT NULL AND target_status_code != 200)
UNION ALL
SELECT 
    source_url,
    target_url,
    anchor_text,
    xpath,
    fragment,
    url_parameters,
    discovered_at,
    is_image,
    has_fragment,
    has_parameters,
    target_status_code,
    target_is_self_canonical,
    target_resolves,
    CASE 
        WHEN target_resolves = 0 THEN 'redirects'
        WHEN target_is_self_canonical = 0 THEN 'not self-canonical'
        WHEN target_status_code IS NOT NULL AND target_status_code != 200 THEN 'non-200 status'
        ELSE 'unknown issue'
    END as invalid_reason
FROM view_links_external
WHERE target_resolves = 0 
   OR target_is_self_canonical = 0 
   OR (target_status_code IS NOT NULL AND target_status_code != 200);

-- Schema analysis view (PostgreSQL version)
CREATE OR REPLACE VIEW view_schema_analysis AS
SELECT 
    u.url,
    st.type_name as schema_type,
    si.format,
    si.is_valid,
    si.severity,
    si.validation_errors,
    psr.position,
    psr.property_name,
    psr.is_main_entity,
    psr.discovered_at
FROM page_schema_references psr
JOIN urls u ON psr.url_id = u.id
JOIN schema_instances si ON psr.schema_instance_id = si.id
JOIN schema_types st ON si.schema_type_id = st.id
ORDER BY u.url, psr.position;

-- Crawl status view (PostgreSQL version)
CREATE OR REPLACE VIEW view_crawl_status AS
SELECT 
    f.status,
    COUNT(*) as count,
    AVG(f.priority_score) as avg_priority,
    MIN(f.enqueued_at) as oldest_enqueued,
    MAX(f.updated_at) as last_updated
FROM frontier f
GROUP BY f.status;
"""


def get_sqlite_views():
    """Get SQLite database views as a list of statements."""
    import re
    # Split by CREATE VIEW statements
    views = re.split(r'CREATE VIEW IF NOT EXISTS', SQLITE_DATABASE_VIEWS)
    statements = []
    for view in views[1:]:  # Skip the first empty split
        statement = "CREATE VIEW IF NOT EXISTS " + view.strip()
        if not statement.endswith(';'):
            statement += ';'
        statements.append(statement)
    return statements


def get_postgres_views():
    """Get PostgreSQL database views as a list of statements."""
    import re
    # Split by CREATE OR REPLACE VIEW statements
    views = re.split(r'CREATE OR REPLACE VIEW', POSTGRES_DATABASE_VIEWS)
    statements = []
    for view in views[1:]:  # Skip the first empty split
        statement = "CREATE OR REPLACE VIEW " + view.strip()
        if not statement.endswith(';'):
            statement += ';'
        statements.append(statement)
    return statements
