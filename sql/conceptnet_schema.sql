-- ============================================================================
-- ConceptNet (typed subgraph) — MySQL schema
-- ============================================================================
-- Design: single `edges` table with `edge_class` as a column, so that
-- k-hop traversals are one self-join per hop (no 9-branch UNION per hop).
-- The type system is enforced declaratively:
--   * nodes : pos <-> node_type consistency (CHECK chk_nodes_pos_type)
--   * edges : endpoint types (composite FKs to nodes(node_id, node_type)),
--             edge_class provably derived from the endpoint types (CHECK
--             chk_edges_class), relation must exist (FK)
--   * relation -> class permission is DATA (relation_class_perms), exposed
--             as the `permitted` flag of v_edges. Variant B below can turn
--             it into an enforced FK.
--
-- CLASS-PARTITION NOTE (design decision, post-incident): the 3x3 endpoint
-- type partition is COMPLETE BY DESIGN. Classes with zero population in a
-- given slice (P2P, P2E, P2A, A2E here) are empirical findings that query
-- C2 reports, not redundancy. Removing "empty" classes desynchronizes the
-- contract's five materializations (dict, CSV, relation_edge_classes,
-- relation_class_perms, :Schema meta-graph) and produces phantom
-- violations in v_edges (61,416 in the incident of record). Don't.
--
-- RELATION-VOCABULARY NOTE: ConceptNet 5.7's documented relation list
-- includes PropertyOf and LocationOf, but neither is materialized in the
-- assertions dump (verified by zgrep against the full dump and by the
-- load logs: zero edges realize them). Both were removed from the
-- contract after verification. Classes P2E/P2A remain in the partition
-- as permitted-but-unmapped, mirroring A2E.
--
-- Companion artifacts (keep in sync):
--   * label_concepts.py — its RELATION_TO_EDGE_CLASSES must match the
--     relation_edge_classes seed below (41 grants over 38 relations);
--     its typing rule matches chk_nodes_pos_type; its output columns
--     match the loader contract:
--       typed_nodes.csv : uri, name, pos, node_type, label_source
--       typed_edges.csv : relation, subject, object, weight, edge_class,
--                         permitted
--   * build_memgraph.py — consumes the SAME two files; identity rules are
--     mirrored by its MERGE clauses.
--
-- Identity rules:
--   node = uri
--   edge = (subject, relation, object); duplicate triples keep the max
--   weight (loader uses INSERT ... ON DUPLICATE KEY UPDATE, the SQL image
--   of the Cypher MERGE in build_memgraph.py).
--
-- Requires MySQL 8.0.16+ (CHECK enforcement); loader-side row aliases for
-- ON DUPLICATE KEY UPDATE need 8.0.20+ (older: use VALUES(weight)).
-- ============================================================================

DROP DATABASE IF EXISTS conceptnet;
CREATE DATABASE conceptnet
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
USE conceptnet;

-- ---------------------------------------------------------------------------
-- Reference tables
-- ---------------------------------------------------------------------------

CREATE TABLE node_types (
    node_type VARCHAR(16) NOT NULL PRIMARY KEY
) ENGINE=InnoDB;

INSERT INTO node_types (node_type) VALUES
    ('EntityNode'), ('ActionEventNode'), ('PropertyNode');

-- Provenance of the POS label: 'uri' = ConceptNet's own sense annotation,
-- the rest = tiers of label_concepts.py's cascade. Must match the values
-- that script writes into typed_nodes.csv.
CREATE TABLE label_sources (
    label_source VARCHAR(20) NOT NULL PRIMARY KEY
) ENGINE=InnoDB;

INSERT INTO label_sources (label_source) VALUES
    ('uri'), ('conceptnet_dump'), ('conceptnet_api'),
    ('spacy'), ('nltk'), ('wordnet'), ('heuristic');

-- Relation IDs 1..38 follow relations.csv verbatim. PropertyOf and
-- LocationOf (the documented-vocabulary extras) were removed after dump
-- verification — see the RELATION-VOCABULARY NOTE above.
CREATE TABLE relations (
    relation_id   INT         NOT NULL PRIMARY KEY,
    relation_name VARCHAR(64) NOT NULL,
    UNIQUE KEY uq_relation_name (relation_name)
) ENGINE=InnoDB;

INSERT INTO relations (relation_id, relation_name) VALUES
    ( 1,'Antonym'),( 2,'AtLocation'),( 3,'CapableOf'),( 4,'Causes'),
    ( 5,'CausesDesire'),( 6,'CreatedBy'),( 7,'DefinedAs'),( 8,'DerivedFrom'),
    ( 9,'Desires'),(10,'DistinctFrom'),(11,'Entails'),
    (12,'EtymologicallyDerivedFrom'),(13,'EtymologicallyRelatedTo'),
    (14,'FormOf'),(15,'HasA'),(16,'HasContext'),(17,'HasFirstSubevent'),
    (18,'HasLastSubevent'),(19,'HasPrerequisite'),(20,'HasProperty'),
    (21,'HasSubevent'),(22,'InstanceOf'),(23,'IsA'),(24,'LocatedNear'),
    (25,'MadeOf'),(26,'MannerOf'),(27,'MotivatedByGoal'),(28,'NotCapableOf'),
    (29,'NotDesires'),(30,'NotHasProperty'),(31,'PartOf'),(32,'ReceivesAction'),
    (33,'RelatedTo'),(34,'SimilarTo'),(35,'SymbolOf'),(36,'Synonym'),
    (37,'UsedFor'),(38,'dbpedia');

-- ALL is a permission marker referenced by relation_edge_classes, not a
-- concrete class. This table mirrors EDGE_CLASS_TYPES in label_concepts.py:
-- the complete 3x3 partition (see the CLASS-PARTITION NOTE above).
CREATE TABLE edge_classes (
    edge_class   VARCHAR(4)  NOT NULL PRIMARY KEY,
    subject_type VARCHAR(16) NULL,
    object_type  VARCHAR(16) NULL,
    CONSTRAINT fk_ec_s FOREIGN KEY (subject_type) REFERENCES node_types (node_type),
    CONSTRAINT fk_ec_o FOREIGN KEY (object_type)  REFERENCES node_types (node_type)
) ENGINE=InnoDB;

INSERT INTO edge_classes (edge_class, subject_type, object_type) VALUES
    ('E2E','EntityNode','EntityNode'),
    ('E2P','EntityNode','PropertyNode'),
    ('E2A','EntityNode','ActionEventNode'),
    ('P2P','PropertyNode','PropertyNode'),       -- zero-population in this slice;
    ('P2E','PropertyNode','EntityNode'),         -- kept BY DESIGN: the partition
    ('P2A','PropertyNode','ActionEventNode'),    -- is the model, populations are data
    ('A2A','ActionEventNode','ActionEventNode'),
    ('A2P','ActionEventNode','PropertyNode'),
    ('A2E','ActionEventNode','EntityNode'),      -- zero-population in this slice
    ('ALL',NULL,NULL);

-- MUST match RELATION_TO_EDGE_CLASSES in label_concepts.py (41 rows over
-- 38 relations: 31 specific grants + 10 wildcards).
CREATE TABLE relation_edge_classes (
    relation_id INT        NOT NULL,
    edge_class  VARCHAR(4) NOT NULL,
    PRIMARY KEY (relation_id, edge_class),
    CONSTRAINT fk_rec_rel   FOREIGN KEY (relation_id) REFERENCES relations (relation_id),
    CONSTRAINT fk_rec_class FOREIGN KEY (edge_class)  REFERENCES edge_classes (edge_class)
) ENGINE=InnoDB;

INSERT INTO relation_edge_classes (relation_id, edge_class) VALUES
    -- E2E
    ( 2,'E2E'),   -- AtLocation
    ( 8,'E2E'),   -- DerivedFrom
    (12,'E2E'),   -- EtymologicallyDerivedFrom
    (13,'E2E'),   -- EtymologicallyRelatedTo
    (15,'E2E'),   -- HasA
    (22,'E2E'),   -- InstanceOf
    (23,'E2E'),   -- IsA
    (24,'E2E'),   -- LocatedNear
    (25,'E2E'),   -- MadeOf
    (31,'E2E'),   -- PartOf
    (35,'E2E'),   -- SymbolOf
    -- E2P / A2P
    ( 7,'E2E'),( 7,'E2P'),   -- DefinedAs
    (20,'E2P'),(20,'A2P'),   -- HasProperty
    (30,'E2P'),(30,'A2P'),   -- NotHasProperty
    -- E2A
    ( 3,'E2A'),   -- CapableOf
    ( 6,'E2A'),   -- CreatedBy
    ( 9,'E2A'),   -- Desires
    (28,'E2A'),   -- NotCapableOf
    (29,'E2A'),   -- NotDesires
    (32,'E2A'),   -- ReceivesAction
    (37,'E2A'),   -- UsedFor
    -- A2A
    (11,'A2A'),   -- Entails
    (17,'A2A'),   -- HasFirstSubevent
    (18,'A2A'),   -- HasLastSubevent
    (19,'A2A'),   -- HasPrerequisite
    (21,'A2A'),   -- HasSubevent
    (26,'A2A'),   -- MannerOf
    (27,'A2A'),   -- MotivatedByGoal
    -- Wildcard
    ( 1,'ALL'),   -- Antonym
    ( 4,'ALL'),   -- Causes
    ( 5,'ALL'),   -- CausesDesire
    (10,'ALL'),   -- DistinctFrom
    (14,'ALL'),   -- FormOf
    (16,'ALL'),   -- HasContext
    (33,'ALL'),   -- RelatedTo
    (34,'ALL'),   -- SimilarTo
    (36,'ALL'),   -- Synonym
    (38,'ALL');   -- dbpedia

-- The ALL wildcard EXPANDED into the 9 concrete classes, unioned with the
-- specific grants: 10 wildcards x 9 + 31 specific = 121 rows expected.
-- This is the lookup/enforcement target for `permitted` (what v_edges
-- reads). NOTE: built ONCE here, at schema-creation time — this is the
-- table whose staleness caused the 61,416 phantom violations, which is
-- why build_mysql.py now verifies it against relation_edge_classes at
-- every startup (check_perms_expansion).
CREATE TABLE relation_class_perms (
    relation_id INT        NOT NULL,
    edge_class  VARCHAR(4) NOT NULL,
    PRIMARY KEY (relation_id, edge_class),
    CONSTRAINT fk_rcp_rel   FOREIGN KEY (relation_id) REFERENCES relations (relation_id),
    CONSTRAINT fk_rcp_class FOREIGN KEY (edge_class)  REFERENCES edge_classes (edge_class)
) ENGINE=InnoDB;

INSERT INTO relation_class_perms (relation_id, edge_class)
SELECT rec.relation_id, ec.edge_class
FROM relation_edge_classes rec
JOIN edge_classes ec ON ec.edge_class <> 'ALL'
WHERE rec.edge_class = 'ALL'
UNION
SELECT rec.relation_id, rec.edge_class
FROM relation_edge_classes rec
WHERE rec.edge_class <> 'ALL';

-- ---------------------------------------------------------------------------
-- Nodes — identity is the ConceptNet URI (sense level), not the surface term
-- ---------------------------------------------------------------------------
CREATE TABLE nodes (
    node_id      INT          NOT NULL,
    -- Exact-match collation: ConceptNet identity is byte-level. The database
    -- default (utf8mb4_unicode_ci) is case- AND accent-insensitive, so it
    -- folds '/c/en/oogenetic' = '/c/en/oögenetic' and the unique key rejects
    -- one of them (error 1062). With utf8mb4_bin, DB equality is identical
    -- to Python string equality, so the loader's preflight dedup becomes a
    -- complete guarantee.
    uri          VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    name         VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    pos          CHAR(1)      NOT NULL,          -- n | v | a | r | s
    node_type    VARCHAR(16)  NOT NULL,
    label_source VARCHAR(20)  NOT NULL,
    PRIMARY KEY (node_id),
    UNIQUE KEY uq_nodes_uri (uri),
    UNIQUE KEY uq_nodes_id_type (node_id, node_type),
    KEY idx_nodes_name (name),
    KEY idx_nodes_type (node_type),
    KEY idx_nodes_source (label_source),
    CONSTRAINT fk_nodes_type   FOREIGN KEY (node_type)    REFERENCES node_types (node_type),
    CONSTRAINT fk_nodes_source FOREIGN KEY (label_source) REFERENCES label_sources (label_source),
    CONSTRAINT chk_nodes_pos_type CHECK (
        (pos = 'n' AND node_type = 'EntityNode') OR
        (pos = 'v' AND node_type = 'ActionEventNode') OR
        (pos IN ('a','r','s') AND node_type = 'PropertyNode')
    )
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Edges — the single edge store
-- ---------------------------------------------------------------------------
-- `edge_class` is asserted by the loader and verified by the engine:
--   * CHECK: it must equal the derivation of the endpoint types, so it can
--     never disagree with the endpoints it claims to summarize;
--   * composite FKs: subject_type / object_type must match the actual type
--     of the referenced node — an E2A row whose subject is not an EntityNode
--     fails with error 1452;
--   * relation FK: the relation must exist.
-- D2 live-demo failure modes:
--   * wrong endpoint type claimed            -> error 1452 (composite FK)
--   * edge_class inconsistent with endpoints -> error 3819 (CHECK)
--   * truthful class, disallowed combination (e.g. HasProperty between two
--     Entities, class E2E)                    -> loads fine, permitted=0 in
--     v_edges (Variant A) / error 1452 (Variant B)
-- ---------------------------------------------------------------------------
CREATE TABLE edges (
    edge_id      BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    subject_id   INT          NOT NULL,
    subject_type VARCHAR(16)  NOT NULL,
    object_id    INT          NOT NULL,
    object_type VARCHAR(16)  NOT NULL,
    relation_id  INT          NOT NULL,
    edge_class   VARCHAR(4)   NOT NULL,
    weight       DOUBLE       NOT NULL DEFAULT 1.0,
    UNIQUE KEY uq_edge_triple (subject_id, relation_id, object_id),
    KEY ix_fk_s (subject_id, subject_type),      -- FK + forward traversal
    KEY ix_obj  (object_id, object_type),        -- FK + reverse traversal
    KEY idx_rel_class (relation_id, edge_class), -- FK + relation/class filters
    CONSTRAINT chk_edges_class CHECK (
        edge_class = CONCAT(LEFT(subject_type, 1), '2', LEFT(object_type, 1))
    ),
    CONSTRAINT fk_edges_s   FOREIGN KEY (subject_id, subject_type)
        REFERENCES nodes (node_id, node_type),
    CONSTRAINT fk_edges_o   FOREIGN KEY (object_id, object_type)
        REFERENCES nodes (node_id, node_type),
    CONSTRAINT fk_edges_rel FOREIGN KEY (relation_id)
        REFERENCES relations (relation_id)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Views
-- ---------------------------------------------------------------------------
-- v_edges: one table + one LEFT JOIN; `permitted` is derived, never stored.
CREATE OR REPLACE VIEW v_edges AS
SELECT e.edge_id, e.edge_class, e.subject_id, e.object_id,
       e.relation_id, e.weight,
       (p.relation_id IS NOT NULL) AS permitted
FROM edges e
LEFT JOIN relation_class_perms p
       ON p.relation_id = e.relation_id
      AND p.edge_class  = e.edge_class;

-- v_triples: the human-readable triple view (subject/relation/object with
-- types, class, weight and permission flag).
CREATE OR REPLACE VIEW v_triples AS
SELECT s.uri AS subject_uri, s.name AS subject, s.node_type AS subject_type,
       r.relation_name AS relation,
       o.uri AS object_uri, o.name AS object, o.node_type AS object_type,
       e.edge_class, e.weight, e.permitted
FROM v_edges e
JOIN nodes s     ON s.node_id = e.subject_id
JOIN nodes o     ON o.node_id = e.object_id
JOIN relations r ON r.relation_id = e.relation_id;

-- v_relation_contract: the static schema, readable at a glance (report aid).
CREATE OR REPLACE VIEW v_relation_contract AS
SELECT r.relation_id, r.relation_name,
       GROUP_CONCAT(rec.edge_class ORDER BY rec.edge_class) AS allowed_classes
FROM relations r
JOIN relation_edge_classes rec ON rec.relation_id = r.relation_id
GROUP BY r.relation_id, r.relation_name;

-- ---------------------------------------------------------------------------
-- OPTIONAL Variant B — enforce the relation->class permission at load time.
-- With this FK, a contract-violating edge (e.g. HasProperty between two
-- Entities with truthful class E2E) is REJECTED (error 1452) instead of
-- being loaded with permitted=0. If enabled, the loader must route
-- violating edges into a quarantine table with the same columns but without
-- this FK.
-- Trade-off: strongest integrity, but the main-table multiset then differs
-- from Memgraph's (which loads everything). Default: keep Variant A.
-- ---------------------------------------------------------------------------
-- ALTER TABLE edges ADD CONSTRAINT fk_edges_perm
--     FOREIGN KEY (relation_id, edge_class)
--     REFERENCES relation_class_perms (relation_id, edge_class);

-- ---------------------------------------------------------------------------
-- Post-load verification (run after the loader; Memgraph counterparts are
-- printed by build_memgraph.py's report)
-- ---------------------------------------------------------------------------
-- Isomorphism (must match pipeline_stats.json):
--   SELECT (SELECT COUNT(*) FROM nodes) AS nodes,
--          (SELECT COUNT(*) FROM edges) AS edges;
--   nodes == nodes_kept
--   edges == edges_kept - edges_duplicate_triples   (max-weight merges)
--
-- Permission parity (must be 0 in STRICT mode; equal to the permitted=0
-- count in typed_edges.csv in faithful mode):
--   SELECT COUNT(*) FROM v_edges WHERE NOT permitted;
--
-- Optimizer statistics before any benchmarking:
--   ANALYZE TABLE nodes, edges;
-- ---------------------------------------------------------------------------