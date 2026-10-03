//! Installation registry and explicit, identity-preserving source relocation.
//!
//! All provider reads use the existing snapshot adapter. Registry assignments
//! and locator changes participate in the catalog's durable activation path.

use super::*;
use agent_session_grep_application::relocation::{
    alias_expiry_ms, decode_plan, installation_root, issue_plan, legacy_installation_namespace,
    normalize_absolute_path, path_is_within, remap_source_path, validate_root_mapping, verify_plan,
};
use agent_session_grep_domain::{SessionIdentityNamespace, Stability};
use agent_session_grep_ports::SourceSnapshot;
use agent_session_grep_ports::relocation::{
    RelocationResult, RelocationStatus, validate_alias_ttl_days,
};

/// Explicit inventory: historical outbox JSON is deliberately not rewritten.
const LIVE_SOURCE_TABLES: &[&str] = &[
    "source_scans",
    "source_membership",
    "source_entity_projections",
    "source_placement_membership",
    "source_relation_scans",
    "source_session_resume_claims",
    "tool_activity_membership",
    "usage_event_membership",
    "source_installations",
];
const SOURCE_PAGE_SIZE: usize = 128;
const MAX_RELOCATION_SOURCES: usize = 65_536;

fn invalid(message: &str) -> PortError {
    PortError::InvalidRequest(message.into())
}
fn source_error(error: PortError) -> PortError {
    match error {
        PortError::SnapshotChanged(_) => {
            PortError::SnapshotChanged("relocation source changed; create a new preview".into())
        }
        _ => PortError::SourceIo("relocation source could not be verified".into()),
    }
}
fn digest_fields(domain: &[u8], fields: &[&str]) -> String {
    let mut hash = blake3::Hasher::new();
    hash_field(&mut hash, domain);
    for field in fields {
        hash_field(&mut hash, field.as_bytes());
    }
    hash.finalize().to_hex().to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InstallationAssignment {
    namespace_id: String,
    provider_id: String,
    namespace_input: String,
    origin: String,
    root_key: String,
    root_locator: String,
    created_at_ms: i64,
}
impl InstallationAssignment {
    pub(super) fn canonical_value(&self) -> serde_json::Value {
        serde_json::json!({
            "namespace_id": self.namespace_id, "provider_id": self.provider_id,
            "namespace_input": self.namespace_input, "origin": self.origin,
            "root_key": self.root_key, "root_locator": self.root_locator,
            "created_at_ms": self.created_at_ms,
        })
    }
}

impl SqliteStore {
    pub(super) fn migrate_v17_to_v18(conn: &Connection) -> PortResult<()> {
        Self::migrate_v17_to_v18_inner(conn, false)
    }

    fn migrate_v17_to_v18_inner(conn: &Connection, inject_failure: bool) -> PortResult<()> {
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "CREATE TABLE installation_namespaces (
                namespace_id TEXT PRIMARY KEY,
                provider_id TEXT NOT NULL,
                namespace_input TEXT NOT NULL,
                origin TEXT NOT NULL CHECK(origin IN ('legacy-v1', 'allocated-v1')),
                created_at_ms INTEGER NOT NULL,
                UNIQUE(provider_id, namespace_input)
             );
             CREATE TABLE installation_locations (
                provider_id TEXT NOT NULL,
                root_key TEXT NOT NULL,
                root_locator TEXT NOT NULL,
                namespace_id TEXT NOT NULL REFERENCES installation_namespaces(namespace_id),
                state TEXT NOT NULL CHECK(state IN ('current', 'retired')),
                retired_until_ms INTEGER,
                PRIMARY KEY(provider_id, root_key),
                CHECK((state = 'current' AND retired_until_ms IS NULL)
                   OR (state = 'retired' AND retired_until_ms IS NOT NULL))
             );
             CREATE UNIQUE INDEX installation_current_namespace
                ON installation_locations(namespace_id) WHERE state = 'current';
             CREATE TABLE source_installations (
                source_path TEXT PRIMARY KEY,
                source_key TEXT NOT NULL UNIQUE,
                namespace_id TEXT NOT NULL REFERENCES installation_namespaces(namespace_id)
             );
             CREATE INDEX source_installations_namespace
                ON source_installations(namespace_id, source_path);
             CREATE TABLE installation_relocations (
                operation_id TEXT PRIMARY KEY REFERENCES index_batches(operation_id),
                mapping_key TEXT NOT NULL,
                mapping_digest TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                from_key TEXT NOT NULL,
                to_key TEXT NOT NULL,
                base_generation INTEGER NOT NULL,
                target_generation INTEGER NOT NULL,
                alias_ttl_days INTEGER NOT NULL,
                source_count INTEGER NOT NULL,
                session_count INTEGER NOT NULL,
                namespace_count INTEGER NOT NULL,
                manifest_json TEXT NOT NULL
             );
             CREATE INDEX installation_relocations_mapping
                ON installation_relocations(mapping_key, target_generation DESC);
             ALTER TABLE index_batches ADD COLUMN relocation_json TEXT NOT NULL DEFAULT 'null';",
        )
        .map_err(backend)?;
        Self::bootstrap_legacy_installations(&tx)?;
        if inject_failure {
            return Err(PortError::Backend(
                "injected installation migration failure".into(),
            ));
        }
        tx.execute_batch("PRAGMA user_version = 18;")
            .map_err(backend)?;
        tx.commit().map_err(backend)
    }

    fn bootstrap_legacy_installations(tx: &rusqlite::Transaction<'_>) -> PortResult<()> {
        // Read provenance only. Canonical payloads and IDs are never rekeyed.
        let mut stmt = tx.prepare("SELECT source_path, provider_id FROM source_scans WHERE len_bytes IS NOT NULL OR fingerprint IS NOT NULL OR EXISTS(SELECT 1 FROM source_membership sm WHERE sm.source_path=source_scans.source_path) ORDER BY source_path").map_err(backend)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .map_err(backend)?;
        let mut candidates = Vec::new();
        let mut root_seeds: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
        let mut source_keys: BTreeMap<String, usize> = BTreeMap::new();
        for row in rows {
            let (path, provider) = row.map_err(backend)?;
            let Some(assignment) = Self::legacy_assignment(tx, &path, provider.as_deref())? else {
                continue;
            };
            let key = normalize_absolute_path(&path)?;
            *source_keys.entry(key.clone()).or_default() += 1;
            root_seeds
                .entry((assignment.provider_id.clone(), assignment.root_key.clone()))
                .or_default()
                .insert(assignment.namespace_input.clone());
            candidates.push((path, key, assignment));
        }
        drop(stmt);
        for (path, key, assignment) in candidates {
            if source_keys[&key] != 1
                || root_seeds[&(assignment.provider_id.clone(), assignment.root_key.clone())].len()
                    != 1
            {
                continue; // Ambiguous provenance remains readable but ineligible.
            }
            Self::persist_installation_in_tx(tx, &path, &assignment, unix_ms()?, false)?;
        }
        Ok(())
    }

    fn legacy_assignment(
        conn: &Connection,
        path: &str,
        scan_provider: Option<&str>,
    ) -> PortResult<Option<InstallationAssignment>> {
        if normalize_absolute_path(path).is_err() {
            return Ok(None);
        }
        let mut providers: BTreeSet<String> = BTreeSet::new();
        if let Some(provider) = scan_provider {
            providers.insert(provider.to_string());
        }
        let mut stmt = conn.prepare("SELECT DISTINCT provider_id FROM source_session_resume_claims WHERE source_path = ?1").map_err(backend)?;
        for row in stmt
            .query_map([path], |row| row.get::<_, String>(0))
            .map_err(backend)?
        {
            providers.insert(row.map_err(backend)?);
        }
        if providers.len() != 1 {
            return Ok(None);
        }
        let provider = providers.into_iter().next().expect("one provider");
        if agent_session_grep_ports::capability::ProviderCapabilityMatrix::current()
            .find(&provider)
            .is_none()
        {
            return Ok(None);
        }
        let namespace_input = legacy_installation_namespace(path, &provider);
        let root_locator = installation_root(path, &provider)?;
        if !Self::legacy_identity_matches(conn, path, &provider, &namespace_input)? {
            return Ok(None);
        }
        Ok(Some(InstallationAssignment {
            namespace_id: format!(
                "ins_v1_{}",
                digest_fields(
                    b"asg-legacy-installation-v1",
                    &[&provider, &namespace_input]
                )
            ),
            provider_id: provider,
            namespace_input,
            origin: "legacy-v1".into(),
            root_key: normalize_absolute_path(&root_locator)?,
            root_locator,
            created_at_ms: unix_ms()?,
        }))
    }

    fn legacy_identity_matches(
        conn: &Connection,
        path: &str,
        provider: &str,
        namespace: &str,
    ) -> PortResult<bool> {
        let mut stmt = conn
            .prepare(
                "SELECT sm.message_id, rc.provider_session_id, rc.provider_session_id_state
             FROM source_membership sm LEFT JOIN source_session_resume_claims rc
               ON rc.source_path = sm.source_path AND rc.session_id = sm.message_id
             WHERE sm.source_path = ?1 AND sm.message_id GLOB 'ses_v1_*'",
            )
            .map_err(backend)?;
        let rows = stmt
            .query_map([path], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(backend)?;
        for row in rows {
            let (session, native, state) = row.map_err(backend)?;
            let identity = Self::stable_id_from_store(conn, &session)?;
            if state.as_deref() == Some("resolved") {
                let Some(native) = native else {
                    return Ok(false);
                };
                if StableId::native_session_scoped(
                    &SessionIdentityNamespace {
                        provider_id: provider,
                        installation_namespace: namespace,
                    },
                    &native,
                )
                .as_str()
                    != session
                {
                    return Ok(false);
                }
            } else if identity.stability() == Stability::Native {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn assignment_for_source(
        conn: &Connection,
        source_path: &str,
    ) -> PortResult<Option<InstallationAssignment>> {
        let key = match normalize_absolute_path(source_path) {
            Ok(key) => key,
            Err(_) => return Ok(None),
        };
        conn.query_row(
            "SELECT n.namespace_id,n.provider_id,n.namespace_input,n.origin,l.root_key,l.root_locator,n.created_at_ms
             FROM source_installations s JOIN installation_namespaces n USING(namespace_id)
             JOIN installation_locations l USING(namespace_id)
             WHERE s.source_key = ?1 AND l.state = 'current'",[key],assignment_row
        ).optional().map_err(backend)
    }

    fn validate_unbound_source_locator(
        conn: &Connection,
        source_path: &str,
        source_key: &str,
    ) -> PortResult<()> {
        // Exact text still needs the ordinary legacy proof. Any other unbound
        // spelling of the same lexical key makes that provenance ambiguous.
        let mut statement = conn
            .prepare(
                "SELECT ss.source_path FROM source_scans ss
                 LEFT JOIN source_installations si USING(source_path)
                 WHERE si.source_path IS NULL AND ss.source_path <> ?1",
            )
            .map_err(backend)?;
        for recorded in statement
            .query_map([source_path], |row| row.get::<_, String>(0))
            .map_err(backend)?
        {
            let recorded = recorded.map_err(backend)?;
            if normalize_absolute_path(&recorded).is_ok_and(|key| key == source_key) {
                return Err(invalid(
                    "existing source locator needs explicit provenance resolution",
                ));
            }
        }
        Ok(())
    }

    /// Proof that an installation binding is the derived placeholder left by
    /// the historical first-empty-source path, and therefore carries no real
    /// identity. Accepts only a zero-byte scan, no message/placement/activity/
    /// usage claims, Reconstructed document/session placeholders, no resolved
    /// resume identity and no relocation history. Health checks that cannot be
    /// proven return `false` (fail closed); this is a one-way repair
    /// precondition, never a licence to reassign real source identities.
    pub(super) fn repairable_empty_placeholder(
        conn: &Connection,
        source_path: &str,
        namespace_id: &str,
    ) -> PortResult<bool> {
        let provider: Option<String> = conn
            .query_row(
                "SELECT provider_id FROM installation_namespaces WHERE namespace_id=?1",
                [namespace_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(backend)?;
        if provider.as_deref() != Some("empty") {
            return Ok(false);
        }
        let scan: Option<(Option<i64>, Option<String>, Option<String>)> = conn
            .query_row(
                "SELECT len_bytes, fingerprint, provider_id FROM source_scans WHERE source_path=?1",
                [source_path],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(backend)?;
        let Some((Some(0), Some(fingerprint), scanned_provider)) = scan else {
            return Ok(false);
        };
        if fingerprint != blake3::hash(b"").to_hex().to_string()
            || scanned_provider.is_some_and(|provider| provider != "empty")
        {
            return Ok(false);
        }
        // Verify the historical empty derivation, not merely a stability label:
        // real documents and sessions can also be Reconstructed. These values
        // only check persisted proof; they never replace or infer native IDs.
        let document = StableId::derive(
            IdKind::Document,
            Stability::Reconstructed,
            &[b"empty", b"empty", fingerprint.as_bytes()],
        );
        let session = StableId::derive(
            IdKind::Session,
            Stability::Reconstructed,
            &[document.as_str().as_bytes()],
        );
        let document_payload = serde_json::json!({
            "provider": "empty", "variant": "empty", "fingerprint": fingerprint, "len": 0,
        });
        let session_payload = serde_json::json!({
            "document": document.as_str(), "documents": [document.as_str()], "messages": [],
        });
        // Any message-level or relation-level claim proves the source observed
        // real content and must keep its identity.
        let message_claims: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_membership
                 WHERE source_path=?1 AND message_id GLOB 'msg_v1_*'",
                [source_path],
                |r| r.get(0),
            )
            .map_err(backend)?;
        if message_claims != 0 {
            return Ok(false);
        }
        for table in [
            "source_placement_membership",
            "tool_activity_membership",
            "usage_event_membership",
        ] {
            let claims: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE source_path=?1"),
                    [source_path],
                    |r| r.get(0),
                )
                .map_err(backend)?;
            if claims != 0 {
                return Ok(false);
            }
        }
        // Check the full sidecar, payload and document attribution of every
        // surviving claim. Missing or contradictory evidence is not permission
        // to retire a real container just because its scan currently says zero.
        let mut statement = conn
            .prepare(
                "SELECT sm.message_id, f.id_json, c.payload, sm.document_id
                 FROM source_membership sm
                 LEFT JOIN fts_ids f ON f.wire_id = sm.message_id
                 LEFT JOIN catalog c ON c.id = sm.message_id
                 WHERE sm.source_path=?1",
            )
            .map_err(backend)?;
        let mut rows = statement.query([source_path]).map_err(backend)?;
        while let Some(row) = rows.next().map_err(backend)? {
            let wire: String = row.get(0).map_err(backend)?;
            let id_json: Option<String> = row.get(1).map_err(backend)?;
            let payload: Option<Vec<u8>> = row.get(2).map_err(backend)?;
            let document_id: Option<String> = row.get(3).map_err(backend)?;
            let (expected_id, expected_payload) = if wire == document.as_str() {
                (&document, &document_payload)
            } else if wire == session.as_str() {
                (&session, &session_payload)
            } else {
                return Ok(false);
            };
            let (Some(id_json), Some(payload)) = (id_json, payload) else {
                return Ok(false);
            };
            let Ok(identity) = serde_json::from_str::<StableId>(&id_json) else {
                return Ok(false);
            };
            let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&payload) else {
                return Ok(false);
            };
            if identity != *expected_id
                || payload != *expected_payload
                || document_id.as_deref() != Some(document.as_str())
            {
                return Ok(false);
            }
        }
        // Missing-state metadata is not native proof, but it must belong to
        // this exact placeholder. Any cwd, pair, foreign identity or unknown
        // state is contradictory evidence and keeps the binding fail-closed.
        let identity_claims: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_session_resume_claims
                 WHERE source_path=?1
                   AND (session_id IS NOT ?2 OR provider_id IS NOT 'empty'
                        OR provider_session_id IS NOT NULL
                        OR provider_session_id_state IS NOT 'missing'
                        OR original_working_directory IS NOT NULL
                        OR original_working_directory_state IS NOT 'missing'
                        OR pair_observed IS NOT 0)",
                rusqlite::params![source_path, session.as_str()],
                |r| r.get(0),
            )
            .map_err(backend)?;
        if identity_claims != 0 {
            return Ok(false);
        }
        // A retired location or a recorded relocation means the namespace took
        // part in an identity rewrite; rebinding it here would be silent.
        let retired: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM installation_locations
                 WHERE namespace_id=?1 AND state='retired'",
                [namespace_id],
                |r| r.get(0),
            )
            .map_err(backend)?;
        if retired != 0 {
            return Ok(false);
        }
        let relocated: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM installation_relocations r
                 WHERE r.provider_id = (SELECT provider_id FROM installation_namespaces
                                        WHERE namespace_id = ?1)",
                [namespace_id],
                |r| r.get(0),
            )
            .map_err(backend)?;
        if relocated != 0 {
            return Ok(false);
        }
        Ok(true)
    }

    /// Source paths in this batch whose persisted binding is a provable empty
    /// placeholder and may therefore be rebound to the staged installation.
    /// Evaluated inside the write transaction, before the batch replaces the
    /// scan row, memberships and catalog identities that prove it.
    pub(super) fn authorized_placeholder_rebinds(
        tx: &rusqlite::Transaction<'_>,
        sources: &[SourceReplacementManifest],
    ) -> PortResult<std::collections::BTreeSet<String>> {
        let mut authorized = std::collections::BTreeSet::new();
        for source in sources {
            let Some(installation) = &source.installation else {
                continue;
            };
            let prior: Option<String> = tx
                .query_row(
                    "SELECT namespace_id FROM source_installations WHERE source_path=?1",
                    [&source.source_path],
                    |r| r.get(0),
                )
                .optional()
                .map_err(backend)?;
            let Some(prior) = prior.filter(|id| id != &installation.namespace_id) else {
                continue;
            };
            if Self::repairable_empty_placeholder(tx, &source.source_path, &prior)? {
                authorized.insert(source.source_path.clone());
            }
        }
        Ok(authorized)
    }

    /// Resolve identity before parsing. New values are in-memory reservations;
    /// only a successful source activation can persist them.
    pub fn resolve_or_allocate_installation_namespace(
        &self,
        provider_id: &str,
        source_path: &str,
        legacy_namespace_input: &str,
    ) -> PortResult<String> {
        let source_key = normalize_absolute_path(source_path)?;
        let conn = self.conn.borrow();
        let mut replaces_empty_placeholder = false;
        if let Some(existing) = Self::assignment_for_source(&conn, source_path)? {
            if existing.provider_id == provider_id {
                return Ok(existing.namespace_input);
            }
            // A source first seen at zero bytes has no provider evidence; its
            // `empty` placeholder binding may be replaced exactly once, and
            // only through the ordinary transactional activation of a real
            // provider. Everything else stays a hard conflict.
            if !Self::repairable_empty_placeholder(&conn, source_path, &existing.namespace_id)? {
                return Err(invalid("source belongs to another provider installation"));
            }
            replaces_empty_placeholder = true;
        }
        Self::validate_unbound_source_locator(&conn, source_path, &source_key)?;
        let now = (self.relocation_clock)()?;
        let scanned: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM source_scans WHERE source_path=?1)",
                [source_path],
                |r| r.get(0),
            )
            .map_err(backend)?;
        // A repairable placeholder holds no identity to reconstruct, so the
        // legacy provenance recovery below must not run for it.
        let legacy_existing = if scanned && !replaces_empty_placeholder {
            match Self::legacy_assignment(&conn, source_path, Some(provider_id))? {
                Some(assignment) => Some(assignment),
                None => {
                    // A legacy source may contain several Native Session rows
                    // without resume claims. Only that explicit multi-session
                    // re-scan path can be admitted provisionally: the later
                    // source commit must prove the complete set with claims.
                    // A single unresolved Native row has no independent proof
                    // of its installation boundary and stays unregistered.
                    let native_count: i64 = conn
                        .query_row(
                            "SELECT COUNT(*) FROM source_membership
                             WHERE source_path=?1 AND message_id GLOB 'ses_v1_*'",
                            [source_path],
                            |row| row.get(0),
                        )
                        .map_err(backend)?;
                    if native_count < 2
                        || legacy_namespace_input
                            != legacy_installation_namespace(source_path, provider_id)
                    {
                        None
                    } else {
                        let root_locator = installation_root(source_path, provider_id)?;
                        Some(InstallationAssignment {
                            namespace_id: format!(
                                "ins_v1_{}",
                                digest_fields(
                                    b"asg-legacy-installation-v1",
                                    &[provider_id, legacy_namespace_input],
                                )
                            ),
                            provider_id: provider_id.into(),
                            namespace_input: legacy_namespace_input.into(),
                            origin: "legacy-v1".into(),
                            root_key: normalize_absolute_path(&root_locator)?,
                            root_locator,
                            created_at_ms: now,
                        })
                    }
                }
            }
        } else {
            None
        };
        let mut stmt=conn.prepare(
            "SELECT n.namespace_id,n.provider_id,n.namespace_input,n.origin,l.root_key,l.root_locator,n.created_at_ms,l.state,l.retired_until_ms
             FROM installation_locations l JOIN installation_namespaces n USING(namespace_id)
             WHERE l.provider_id = ?1 ORDER BY l.root_key"
        ).map_err(backend)?;
        let mut found = None;
        for row in stmt
            .query_map([provider_id], |r| {
                Ok((
                    assignment_row(r)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, Option<i64>>(8)?,
                ))
            })
            .map_err(backend)?
        {
            let (assignment, state, expiry) = row.map_err(backend)?;
            if !path_is_within(source_path, &assignment.root_locator)? {
                continue;
            }
            if state == "retired" {
                if expiry.is_some_and(|expiry| now < expiry) {
                    return Err(invalid(
                        "source location is retired; use the current installation location",
                    ));
                }
                continue;
            }
            if found.is_some() {
                return Err(invalid(
                    "source location has ambiguous installation ownership",
                ));
            }
            found = Some(assignment);
        }
        drop(stmt);
        if found.is_none() {
            let root_locator = installation_root(source_path, provider_id)?;
            let root_key = normalize_absolute_path(&root_locator)?;
            for assignment in self.pending_installations.borrow().values() {
                if assignment.provider_id == provider_id && assignment.root_key == root_key {
                    found = Some(assignment.clone());
                    break;
                }
            }
            if found.is_none() {
                // Existing indexed sources must prove the exact historical seed;
                // a new opaque namespace must never silently replace an old ID.
                let legacy_group = Self::unbound_legacy_root(&conn, provider_id, &root_key)?;
                if let Some(legacy) = legacy_existing.clone().or(legacy_group) {
                    found = Some(legacy);
                } else if scanned && !replaces_empty_placeholder {
                    return Err(invalid(
                        "source installation provenance is unresolved; identity cannot be reconstructed",
                    ));
                } else {
                    if legacy_namespace_input
                        != legacy_installation_namespace(source_path, provider_id)
                    {
                        return Err(invalid(
                            "legacy namespace input does not match the source provenance",
                        ));
                    }
                    let allocated: String = conn
                        .query_row("SELECT lower(hex(randomblob(32)))", [], |r| r.get(0))
                        .map_err(backend)?;
                    found = Some(InstallationAssignment {
                        namespace_id: format!("ins_v1_{allocated}"),
                        provider_id: provider_id.into(),
                        namespace_input: format!("installation-v1:{allocated}"),
                        origin: "allocated-v1".into(),
                        root_key,
                        root_locator,
                        created_at_ms: now,
                    });
                }
            }
        }
        let assignment = found.ok_or_else(|| invalid("installation could not be resolved"))?;
        if legacy_existing
            .as_ref()
            .is_some_and(|legacy| legacy.namespace_input != assignment.namespace_input)
            || Self::unbound_legacy_root(&conn, provider_id, &assignment.root_key)?
                .as_ref()
                .is_some_and(|legacy| legacy.namespace_input != assignment.namespace_input)
        {
            return Err(invalid(
                "normalized location would merge distinct legacy installation identities",
            ));
        }
        let namespace = assignment.namespace_input.clone();
        self.pending_installations
            .borrow_mut()
            .insert(source_key, assignment);
        Ok(namespace)
    }

    pub(super) fn installation_for_source_commit(
        &self,
        source: &SourceBatch,
    ) -> PortResult<Option<InstallationAssignment>> {
        let Ok(key) = normalize_absolute_path(&source.source_path) else {
            return Ok(None);
        };
        if source.entries.is_empty() && source.fingerprint.is_none() && source.len_bytes.is_none() {
            return Ok(None);
        }
        let conn = self.conn.borrow();
        let now = (self.relocation_clock)()?;
        let assignment = self
            .pending_installations
            .borrow()
            .get(&key)
            .cloned()
            .or(Self::assignment_for_source(&conn, &source.source_path)?);
        let mut providers = BTreeSet::new();
        if let Some(assignment) = &assignment {
            providers.insert(assignment.provider_id.as_str());
        }
        providers.extend(source.provider_id.as_deref());
        providers.extend(
            source
                .resume_claims
                .iter()
                .map(|claim| claim.provider_id.as_str()),
        );
        if providers.len() > 1 {
            return Err(invalid(
                "source provider differs from its installation binding",
            ));
        }
        let provider = providers.into_iter().next();
        // Unknown provenance remains fail-closed; verified providers only consult
        // their own location records, even when another provider shares the root.
        let mut statement = conn
            .prepare(
                "SELECT root_locator FROM installation_locations
             WHERE state='retired' AND retired_until_ms>?1
               AND (?2 IS NULL OR provider_id=?2)",
            )
            .map_err(backend)?;
        for root in statement
            .query_map(rusqlite::params![now, provider], |r| r.get::<_, String>(0))
            .map_err(backend)?
        {
            if path_is_within(&source.source_path, &root.map_err(backend)?)? {
                return Err(invalid(
                    "source location is retired; use the current installation location",
                ));
            }
        }
        if let Some(assignment) = &assignment {
            if assignment.origin == "legacy-v1" {
                Self::validate_legacy_source_proof(&conn, source, assignment)?;
            }
            let current_root: Option<String> = conn.query_row("SELECT root_key FROM installation_locations WHERE namespace_id=?1 AND state='current'",[&assignment.namespace_id],|r|r.get(0)).optional().map_err(backend)?;
            if current_root
                .as_ref()
                .is_some_and(|root| root != &assignment.root_key)
            {
                return Err(invalid(
                    "installation moved after source staging; re-scan before committing",
                ));
            }
            for claim in &source.resume_claims {
                if claim.provider_session_id_state == "resolved" {
                    let native = claim
                        .provider_session_id
                        .as_deref()
                        .ok_or_else(|| invalid("resolved native identity is missing"))?;
                    let expected = StableId::native_session_scoped(
                        &SessionIdentityNamespace {
                            provider_id: &assignment.provider_id,
                            installation_namespace: &assignment.namespace_input,
                        },
                        native,
                    );
                    if expected.as_str() != claim.session_id {
                        return Err(invalid(
                            "staged session does not match its installation namespace",
                        ));
                    }
                }
            }
        }
        Ok(assignment)
    }

    fn validate_legacy_source_proof(
        conn: &Connection,
        source: &SourceBatch,
        assignment: &InstallationAssignment,
    ) -> PortResult<()> {
        let existing_sessions: BTreeSet<String> = conn
            .prepare(
                "SELECT message_id FROM source_membership
                 WHERE source_path=?1 AND message_id GLOB 'ses_v1_*'",
            )
            .map_err(backend)?
            .query_map([&source.source_path], |row| row.get::<_, String>(0))
            .map_err(backend)?
            .collect::<rusqlite::Result<BTreeSet<_>>>()
            .map_err(backend)?;
        let native_sessions: BTreeSet<String> = source
            .entries
            .iter()
            .filter(|(id, _, _)| {
                id.kind() == IdKind::Session && id.stability() == Stability::Native
            })
            .map(|(id, _, _)| id.as_str().to_owned())
            .collect();
        let claimed_sessions: BTreeSet<String> = source
            .resume_claims
            .iter()
            .filter(|claim| {
                claim.provider_id == assignment.provider_id
                    && claim.provider_session_id_state == "resolved"
            })
            .map(|claim| claim.session_id.clone())
            .collect();
        if !native_sessions.is_subset(&claimed_sessions)
            || (!existing_sessions.is_empty() && existing_sessions != claimed_sessions)
        {
            return Err(invalid(
                "legacy source must explicitly prove every native session before identity registration",
            ));
        }
        for claim in &source.resume_claims {
            if claim.provider_id != assignment.provider_id
                || claim.provider_session_id_state != "resolved"
            {
                continue;
            }
            let native = claim
                .provider_session_id
                .as_deref()
                .ok_or_else(|| invalid("resolved native identity is missing"))?;
            let expected = StableId::native_session_scoped(
                &SessionIdentityNamespace {
                    provider_id: &assignment.provider_id,
                    installation_namespace: &assignment.namespace_input,
                },
                native,
            );
            if expected.as_str() != claim.session_id {
                return Err(invalid(
                    "legacy resume claim does not match the frozen installation namespace",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn installation_is_current(
        &self,
        conn: &Connection,
        source: &SourceBatch,
    ) -> PortResult<bool> {
        let Ok(key) = normalize_absolute_path(&source.source_path) else {
            return Ok(true);
        };
        if source.entries.is_empty() && source.fingerprint.is_none() && source.len_bytes.is_none() {
            return Ok(Self::assignment_for_source(conn, &source.source_path)?.is_none());
        }
        match self.pending_installations.borrow().get(&key) {
            Some(expected) => Ok(
                Self::assignment_for_source(conn, &source.source_path)?.as_ref() == Some(expected),
            ),
            None => Ok(true),
        }
    }

    pub(super) fn persist_installation_in_tx(
        tx: &rusqlite::Transaction<'_>,
        path: &str,
        assignment: &InstallationAssignment,
        now_ms: i64,
        authorized_placeholder_rebind: bool,
    ) -> PortResult<()> {
        let retired: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM installation_locations WHERE provider_id=?1 AND root_key=?2 AND state='retired' AND retired_until_ms>?3)", rusqlite::params![assignment.provider_id,assignment.root_key,now_ms], |r|r.get(0)).map_err(backend)?;
        if retired {
            return Err(invalid(
                "source location is retired; use the current installation location",
            ));
        }
        let existing:Option<String>=tx.query_row("SELECT namespace_id FROM installation_locations WHERE provider_id = ?1 AND root_key = ?2 AND state = 'current'",rusqlite::params![assignment.provider_id,assignment.root_key],|r|r.get(0)).optional().map_err(backend)?;
        if existing
            .as_ref()
            .is_some_and(|id| id != &assignment.namespace_id)
        {
            return Err(invalid("installation location is already occupied"));
        }
        tx.execute("INSERT INTO installation_namespaces(namespace_id,provider_id,namespace_input,origin,created_at_ms) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(namespace_id) DO NOTHING",rusqlite::params![assignment.namespace_id,assignment.provider_id,assignment.namespace_input,assignment.origin,assignment.created_at_ms]).map_err(backend)?;
        let matches:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM installation_namespaces WHERE namespace_id=?1 AND provider_id=?2 AND namespace_input=?3 AND origin=?4)",rusqlite::params![assignment.namespace_id,assignment.provider_id,assignment.namespace_input,assignment.origin],|r|r.get(0)).map_err(backend)?;
        if !matches {
            return Err(invalid(
                "installation namespace conflicts with persisted identity",
            ));
        }
        tx.execute("INSERT INTO installation_locations(provider_id,root_key,root_locator,namespace_id,state,retired_until_ms) VALUES(?1,?2,?3,?4,'current',NULL) ON CONFLICT(provider_id,root_key) DO UPDATE SET root_locator=excluded.root_locator,namespace_id=excluded.namespace_id,state='current',retired_until_ms=NULL",rusqlite::params![assignment.provider_id,assignment.root_key,assignment.root_locator,assignment.namespace_id]).map_err(backend)?;
        let key = normalize_absolute_path(path)?;
        let prior: Option<String> = tx
            .query_row(
                "SELECT namespace_id FROM source_installations WHERE source_key=?1",
                [&key],
                |r| r.get(0),
            )
            .optional()
            .map_err(backend)?;
        if let Some(prior_id) = prior.as_ref().filter(|id| *id != &assignment.namespace_id) {
            // The placeholder proof is evaluated once at the top of this write
            // transaction (against pre-batch state); the persist step only
            // re-checks the authorization flag and the prior provider. The
            // pre-parse reservation alone never authorizes a rebind.
            let prior_provider: Option<String> = tx
                .query_row(
                    "SELECT provider_id FROM installation_namespaces WHERE namespace_id=?1",
                    [prior_id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(backend)?;
            if !authorized_placeholder_rebind || prior_provider.as_deref() != Some("empty") {
                return Err(invalid(
                    "source installation binding conflicts with persisted identity",
                ));
            }
        }
        tx.execute("INSERT INTO source_installations(source_path,source_key,namespace_id) VALUES(?1,?2,?3) ON CONFLICT(source_path) DO UPDATE SET source_key=excluded.source_key,namespace_id=excluded.namespace_id",rusqlite::params![path,key,assignment.namespace_id]).map_err(backend)?;
        Ok(())
    }

    /// Registered current roots supplement explicit provider discovery.
    pub fn active_installation_roots(&self, provider_id: &str) -> PortResult<Vec<String>> {
        let conn = self.conn.borrow();
        let mut stmt=conn.prepare("SELECT root_locator FROM installation_locations WHERE provider_id=?1 AND state='current' ORDER BY root_key").map_err(backend)?;
        stmt.query_map([provider_id], |r| r.get(0))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)
    }

    /// Deterministic clock injection for relocation policy and retirement tests.
    pub fn with_relocation_clock(mut self, clock: fn() -> PortResult<i64>) -> Self {
        self.relocation_clock = clock;
        self
    }
}

/// Cleanup is restricted to this operation's newly created private backup.
/// Connections are scoped inside the guard's lifetime and always close first.
struct BackupSidecars(std::path::PathBuf);
impl Drop for BackupSidecars {
    fn drop(&mut self) {
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut path = self.0.as_os_str().to_os_string();
            path.push(suffix);
            let _ = std::fs::remove_file(Path::new(&path));
        }
    }
}

fn backup_destination_exists(path: &Path) -> bool {
    ["", "-wal", "-shm", "-journal"].iter().any(|suffix| {
        let mut candidate = path.as_os_str().to_os_string();
        candidate.push(suffix);
        Path::new(&candidate).symlink_metadata().is_ok()
    })
}

fn location_blocks_relocation(
    state: &str,
    retired_until_ms: Option<i64>,
    same_owner: bool,
    operation_at_ms: i64,
) -> bool {
    state == "current"
        || (!same_owner && retired_until_ms.is_some_and(|expiry| operation_at_ms < expiry))
}

fn assignment_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<InstallationAssignment> {
    Ok(InstallationAssignment {
        namespace_id: row.get(0)?,
        provider_id: row.get(1)?,
        namespace_input: row.get(2)?,
        origin: row.get(3)?,
        root_key: row.get(4)?,
        root_locator: row.get(5)?,
        created_at_ms: row.get(6)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SourceMapping {
    from_path: String,
    to_path: String,
    namespace_id: String,
    len_bytes: u64,
    fingerprint: String,
    mtime_ms: i64,
}
impl SourceMapping {
    fn snapshot(&self) -> SourceSnapshot {
        SourceSnapshot {
            path: self.to_path.clone(),
            len: self.len_bytes,
            mtime_ms: self.mtime_ms,
            fingerprint: self.fingerprint.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct LocationMapping {
    namespace_id: String,
    from_key: String,
    from_locator: String,
    to_key: String,
    to_locator: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct RelocationManifest {
    provider_id: String,
    from_key: String,
    to_key: String,
    mapping_key: String,
    mapping_digest: String,
    generation: u64,
    alias_ttl_days: u32,
    operation_at_ms: i64,
    retired_until_ms: i64,
    session_count: u64,
    locations: Vec<LocationMapping>,
    unmoved_locations: Vec<LocationMapping>,
    sources: Vec<SourceMapping>,
}
impl RelocationManifest {
    pub(super) fn canonical_value(&self) -> serde_json::Value {
        serde_json::json!(self)
    }

    pub(super) fn validate(&self) -> PortResult<()> {
        validate_alias_ttl_days(self.alias_ttl_days).map_err(invalid)?;
        if self.retired_until_ms != alias_expiry_ms(self.operation_at_ms, self.alias_ttl_days)? {
            return Err(invalid(
                "relocation alias expiry disagrees with its operation time",
            ));
        }
        if self.sources.is_empty()
            || self.sources.len() > MAX_RELOCATION_SOURCES
            || self.locations.is_empty()
        {
            return Err(invalid(
                "relocation source set is empty or exceeds the supported bound",
            ));
        }
        validate_root_mapping(&self.from_key, &self.to_key)?;
        let mut old = BTreeSet::new();
        let mut new = BTreeSet::new();
        let namespaces: BTreeSet<_> = self
            .locations
            .iter()
            .map(|l| l.namespace_id.as_str())
            .collect();
        if namespaces.len() != self.locations.len() {
            return Err(invalid("relocation has duplicate installation mappings"));
        }
        for source in &self.sources {
            if !old.insert(normalize_absolute_path(&source.from_path)?)
                || !new.insert(normalize_absolute_path(&source.to_path)?)
                || !namespaces.contains(source.namespace_id.as_str())
            {
                return Err(invalid("relocation has conflicting source mappings"));
            }
        }
        Ok(())
    }

    pub(super) fn verify_snapshots(&self) -> PortResult<()> {
        for source in &self.sources {
            verify_snapshot(Path::new(&source.to_path), &source.snapshot())
                .map_err(source_error)?;
        }
        Ok(())
    }

    fn result(
        &self,
        status: RelocationStatus,
        generation: u64,
        plan: Option<String>,
    ) -> RelocationResult {
        RelocationResult {
            status,
            plan,
            source_count: self.sources.len() as u64,
            session_count: self.session_count,
            installation_count: self.locations.len() as u64,
            namespace_count: self.locations.len() as u64,
            generation,
            previous_generation: self.generation,
            alias_ttl_days: self.alias_ttl_days,
        }
    }
}

impl SqliteStore {
    fn relocation_inputs(
        provider: &str,
        from: &str,
        to: &str,
        ttl: u32,
    ) -> PortResult<(String, String, String)> {
        validate_alias_ttl_days(ttl).map_err(invalid)?;
        if agent_session_grep_ports::capability::ProviderCapabilityMatrix::current()
            .find(provider)
            .is_none()
        {
            return Err(invalid(
                "relocation requires a canonical supported provider",
            ));
        }
        let from_key = normalize_absolute_path(from)?;
        let to_key = normalize_absolute_path(to)?;
        if !Path::new(to).is_absolute() {
            return Err(invalid(
                "relocation destination must be an absolute host-local root",
            ));
        }
        validate_root_mapping(&from_key, &to_key)?;
        let mapping_key = digest_fields(
            b"asg-relocation-mapping-v1",
            &[provider, &from_key, &to_key, &ttl.to_string()],
        );
        Ok((from_key, to_key, mapping_key))
    }

    fn prepare_relocation(
        &self,
        provider: &str,
        from: &str,
        to: &str,
        ttl: u32,
        now: i64,
    ) -> PortResult<RelocationManifest> {
        let (from_key, to_key, mapping_key) = Self::relocation_inputs(provider, from, to, ttl)?;
        let metadata = std::fs::metadata(to)
            .map_err(|_| PortError::SourceIo("relocation destination is unavailable".into()))?;
        if !metadata.is_dir() {
            return Err(invalid("relocation destination must be a directory"));
        }
        // Resolve symlinks for overlap checks only; recorded locator spelling
        // remains authoritative, including a source root that no longer exists.
        let canonical_to = std::fs::canonicalize(to).map_err(|_| {
            PortError::SourceIo("relocation destination could not be resolved".into())
        })?;
        let canonical_to = canonical_to
            .to_str()
            .ok_or_else(|| invalid("relocation destination is not valid Unicode"))?;
        validate_root_mapping(from, canonical_to)?;
        let conn = self.conn.borrow();
        let tx = conn.unchecked_transaction().map_err(backend)?;
        let generation: i64 = tx
            .query_row(
                "SELECT active_generation FROM store_metadata WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(backend)?;
        let mut manifest = RelocationManifest {
            provider_id: provider.into(),
            from_key: from_key.clone(),
            to_key: to_key.clone(),
            mapping_key,
            mapping_digest: String::new(),
            generation: u64::try_from(generation).map_err(backend)?,
            alias_ttl_days: ttl,
            operation_at_ms: now,
            retired_until_ms: alias_expiry_ms(now, ttl)?,
            session_count: 0,
            locations: Vec::new(),
            unmoved_locations: Vec::new(),
            sources: Vec::new(),
        };
        let mut assignments = BTreeMap::<String, InstallationAssignment>::new();
        let mut stmt=tx.prepare(
            "SELECT n.namespace_id,n.provider_id,n.namespace_input,n.origin,l.root_key,l.root_locator,n.created_at_ms,l.state,l.retired_until_ms
             FROM installation_locations l JOIN installation_namespaces n USING(namespace_id)
             WHERE l.provider_id=?1 ORDER BY l.root_key"
        ).map_err(backend)?;
        let rows = stmt
            .query_map([provider], |r| {
                Ok((
                    assignment_row(r)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, Option<i64>>(8)?,
                ))
            })
            .map_err(backend)?;
        let mut locations = Vec::new();
        for row in rows {
            locations.push(row.map_err(backend)?);
        }
        drop(stmt);
        for (assignment, state, _) in &locations {
            if state == "current" && path_is_within(&assignment.root_locator, from)? {
                let to_locator = remap_source_path(&assignment.root_locator, from, to)?;
                manifest.locations.push(LocationMapping {
                    namespace_id: assignment.namespace_id.clone(),
                    from_key: assignment.root_key.clone(),
                    from_locator: assignment.root_locator.clone(),
                    to_key: normalize_absolute_path(&to_locator)?,
                    to_locator,
                });
                assignments.insert(assignment.namespace_id.clone(), assignment.clone());
            }
        }
        if manifest.locations.is_empty() {
            return Err(invalid(
                "no verifiable installation is registered under the requested root",
            ));
        }
        // Occupied target ownership (including unexpired retired locations)
        // cannot be claimed by a different installation or an overlapping root.
        for mapped in &manifest.locations {
            for (existing, state, expiry) in &locations {
                if assignments.contains_key(&existing.namespace_id) && state == "current" {
                    continue;
                }
                let overlaps = path_is_within(&mapped.to_locator, &existing.root_locator)?
                    || path_is_within(&existing.root_locator, &mapped.to_locator)?;
                if !overlaps {
                    continue;
                }
                let same_owner = existing.namespace_id == mapped.namespace_id
                    && existing.root_key == mapped.to_key;
                if location_blocks_relocation(state, *expiry, same_owner, manifest.operation_at_ms)
                {
                    return Err(invalid(
                        "relocation target has conflicting installation ownership",
                    ));
                }
            }
        }
        let mut last = String::new();
        let mut sessions = BTreeSet::new();
        let mut hash = blake3::Hasher::new();
        hash_field(&mut hash, b"asg-relocation-source-plan-v1");
        hash_field(&mut hash, manifest.mapping_key.as_bytes());
        for assignment in assignments.values() {
            hash_field(&mut hash, assignment.namespace_id.as_bytes());
            hash_field(&mut hash, assignment.namespace_input.as_bytes());
            hash_field(&mut hash, assignment.root_key.as_bytes());
        }
        loop {
            let mut stmt=tx.prepare(
                "SELECT ss.source_path,ss.len_bytes,ss.fingerprint,ss.provider_id,si.namespace_id
                 FROM source_scans ss LEFT JOIN source_installations si USING(source_path)
                 WHERE ss.source_path > ?1 ORDER BY ss.source_path LIMIT ?2"
            ).map_err(backend)?;
            let batch = stmt
                .query_map(rusqlite::params![last, SOURCE_PAGE_SIZE as i64], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, Option<String>>(4)?,
                    ))
                })
                .map_err(backend)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(backend)?;
            if batch.is_empty() {
                break;
            }
            for (path, len, fingerprint, scan_provider, namespace_id) in batch {
                last = path.clone();
                let Ok(path_key) = normalize_absolute_path(&path) else {
                    continue;
                };
                let in_from = path_is_within(&path_key, &from_key)?;
                let in_to = path_is_within(&path_key, &to_key)?;
                if in_to && !in_from {
                    // A known target source cannot be merged, even when not yet
                    // associated with a namespace or scanned by another provider.
                    if len.is_some() || fingerprint.is_some() || namespace_id.is_some() {
                        return Err(invalid(
                            "relocation target already contains an indexed source",
                        ));
                    }
                }
                if !in_from {
                    if namespace_id
                        .as_ref()
                        .is_some_and(|id| assignments.contains_key(id))
                    {
                        return Err(invalid(
                            "relocation must include every source of an installation",
                        ));
                    }
                    continue;
                }
                if scan_provider.as_deref().is_some_and(|p| p != provider) {
                    continue;
                }
                let Some(namespace_id) = namespace_id else {
                    let has_claims: bool = tx
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM source_membership WHERE source_path=?1)",
                            [&path],
                            |r| r.get(0),
                        )
                        .map_err(backend)?;
                    if len.is_none() && fingerprint.is_none() && !has_claims {
                        continue;
                    }
                    return Err(invalid(
                        "source installation provenance is unresolved; re-scan before relocation",
                    ));
                };
                let Some(assignment) = assignments.get(&namespace_id) else {
                    if let Some(other) = Self::assignment_for_source(&tx, &path)?
                        && other.provider_id != provider
                    {
                        continue;
                    }
                    return Err(invalid(
                        "requested root does not contain the complete installation boundary",
                    ));
                };
                if !path_is_within(&path, &assignment.root_locator)? {
                    return Err(invalid(
                        "source binding disagrees with its installation location",
                    ));
                }
                let len = len
                    .and_then(|n| u64::try_from(n).ok())
                    .ok_or_else(|| invalid("source fingerprint provenance is incomplete"))?;
                let fingerprint = fingerprint
                    .ok_or_else(|| invalid("source fingerprint provenance is incomplete"))?;
                let to_path = remap_source_path(&path, from, to)?;
                let snapshot = capture(Path::new(&to_path)).map_err(source_error)?;
                if snapshot.len != len || snapshot.fingerprint != fingerprint {
                    return Err(PortError::SnapshotChanged(
                        "relocation destination differs from the indexed source".into(),
                    ));
                }
                hash_field(&mut hash, path.as_bytes());
                hash_field(&mut hash, to_path.as_bytes());
                hash_field(&mut hash, namespace_id.as_bytes());
                hash_field(&mut hash, &len.to_le_bytes());
                hash_field(&mut hash, fingerprint.as_bytes());
                manifest.sources.push(SourceMapping {
                    from_path: path.clone(),
                    to_path,
                    namespace_id,
                    len_bytes: len,
                    fingerprint,
                    mtime_ms: snapshot.mtime_ms,
                });
                if manifest.sources.len() > MAX_RELOCATION_SOURCES {
                    return Err(invalid(
                        "relocation exceeds 65536 sources; select a smaller installation root",
                    ));
                }
                let mut session_query=tx.prepare("SELECT message_id FROM source_membership WHERE source_path=?1 AND message_id GLOB 'ses_v1_*'").map_err(backend)?;
                for row in session_query
                    .query_map([path], |r| r.get::<_, String>(0))
                    .map_err(backend)?
                {
                    sessions.insert(row.map_err(backend)?);
                }
            }
        }
        // An empty registered namespace may remain after source deletion. Only
        // move locations that own a live, fingerprinted source in this plan.
        let used: BTreeSet<_> = manifest
            .sources
            .iter()
            .map(|s| s.namespace_id.as_str())
            .collect();
        let (moving, unmoved): (Vec<_>, Vec<_>) = std::mem::take(&mut manifest.locations)
            .into_iter()
            .partition(|location| used.contains(location.namespace_id.as_str()));
        manifest.locations = moving;
        manifest.unmoved_locations = unmoved;
        manifest.session_count = sessions.len() as u64;
        manifest.mapping_digest = hash.finalize().to_hex().to_string();
        manifest.validate()?;
        tx.commit().map_err(backend)?;
        Ok(manifest)
    }

    /// Compute a verifiable plan without changing catalog, provider or backup.
    pub fn relocation_preview(
        &self,
        provider_id: &str,
        from_root: &str,
        to_root: &str,
        alias_ttl_days: u32,
    ) -> PortResult<RelocationResult> {
        let now = (self.relocation_clock)()?;
        let (_, _, key) = Self::relocation_inputs(provider_id, from_root, to_root, alias_ttl_days)?;
        if let Some(manifest) = self.applied_relocation(&key, None)? {
            return Ok(manifest.result(
                RelocationStatus::Unchanged,
                self.active_generation()?,
                None,
            ));
        }
        let manifest =
            self.prepare_relocation(provider_id, from_root, to_root, alias_ttl_days, now)?;
        if manifest.from_key == manifest.to_key {
            return Ok(manifest.result(RelocationStatus::Unchanged, manifest.generation, None));
        }
        let plan = issue_plan(
            SCHEMA_VERSION as u32,
            manifest.generation,
            &manifest.mapping_digest,
            alias_ttl_days,
            now,
        )?;
        Ok(manifest.result(RelocationStatus::Planned, manifest.generation, Some(plan)))
    }
}

impl SqliteStore {
    fn applied_relocation(
        &self,
        mapping_key: &str,
        claims: Option<&agent_session_grep_application::relocation::PlanClaims>,
    ) -> PortResult<Option<RelocationManifest>> {
        let conn = self.conn.borrow();
        let tx = conn.unchecked_transaction().map_err(backend)?;
        let receipt:Option<(String,String,String)>=tx.query_row(
            "SELECT r.manifest_json,b.relocation_json,b.operation_digest
             FROM installation_relocations r JOIN index_batches b USING(operation_id)
             WHERE r.mapping_key=?1 AND b.state='activated' ORDER BY r.target_generation DESC LIMIT 1",
            [mapping_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))
        ).optional().map_err(backend)?;
        let Some((json, durable_json, durable_digest)) = receipt else {
            return Ok(None);
        };
        let manifest: RelocationManifest =
            serde_json::from_str(&json).map_err(|_| invalid("relocation receipt is invalid"))?;
        manifest.validate()?;
        if let Some(claims) = claims
            && (claims.schema_version != SCHEMA_VERSION as u32
                || claims.generation != manifest.generation
                || claims.mapping_digest != manifest.mapping_digest
                || claims.alias_ttl_days != manifest.alias_ttl_days)
        {
            return Ok(None);
        }
        let actual = batch_manifest(
            &[],
            &[],
            &RelationManifests {
                relocation: Some(manifest.clone()),
                ..Default::default()
            },
        )?;
        if durable_json != json || actual.operation_digest != durable_digest {
            return Err(invalid(
                "relocation receipt does not match its durable intent",
            ));
        }
        if manifest.mapping_key != mapping_key {
            return Err(invalid("relocation receipt has an invalid mapping"));
        }
        // Replays must describe the entire selected scope, not just the owners
        // that happened to be present in the original operation. Keep these
        // checks in the receipt's read transaction so sibling installations and
        // unregistered legacy sources cannot hide behind a historical no-op.
        let expected_locations: BTreeMap<_, _> = manifest
            .locations
            .iter()
            .map(|mapped| (mapped.to_key.as_str(), mapped.namespace_id.as_str()))
            .chain(
                manifest
                    .unmoved_locations
                    .iter()
                    .map(|unmoved| (unmoved.from_key.as_str(), unmoved.namespace_id.as_str())),
            )
            .collect();
        let mut scoped_location_count = 0;
        let mut locations = tx
            .prepare(
                "SELECT root_key,namespace_id FROM installation_locations
             WHERE provider_id=?1 AND state='current'",
            )
            .map_err(backend)?;
        for row in locations
            .query_map([&manifest.provider_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .map_err(backend)?
        {
            let (root, namespace) = row.map_err(backend)?;
            let in_scope = path_is_within(&root, &manifest.from_key)?
                || path_is_within(&manifest.from_key, &root)?
                || path_is_within(&root, &manifest.to_key)?
                || path_is_within(&manifest.to_key, &root)?;
            if in_scope {
                if expected_locations.get(root.as_str()).copied() != Some(namespace.as_str()) {
                    return Ok(None);
                }
                scoped_location_count += 1;
            }
        }
        if scoped_location_count != expected_locations.len() {
            return Ok(None);
        }
        drop(locations);
        let expected_sources: BTreeMap<_, _> = manifest
            .sources
            .iter()
            .map(|source| {
                Ok((
                    normalize_absolute_path(&source.to_path)?,
                    source.namespace_id.as_str(),
                ))
            })
            .collect::<PortResult<_>>()?;
        let mut sources = tx
            .prepare(
                "SELECT ss.source_path,COALESCE(n.provider_id,ss.provider_id),si.namespace_id
             FROM source_scans ss LEFT JOIN source_installations si USING(source_path)
             LEFT JOIN installation_namespaces n USING(namespace_id)
             WHERE ss.len_bytes IS NOT NULL OR ss.fingerprint IS NOT NULL
                OR si.namespace_id IS NOT NULL OR EXISTS(
                    SELECT 1 FROM source_membership sm WHERE sm.source_path=ss.source_path)",
            )
            .map_err(backend)?;
        for row in sources
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(backend)?
        {
            let (path, provider, namespace) = row.map_err(backend)?;
            let Ok(key) = normalize_absolute_path(&path) else {
                continue;
            };
            if path_is_within(&key, &manifest.from_key)?
                && provider
                    .as_deref()
                    .is_none_or(|provider| provider == manifest.provider_id)
            {
                return Ok(None);
            }
            if path_is_within(&key, &manifest.to_key)?
                && expected_sources
                    .get(&key)
                    .is_none_or(|expected| namespace.as_deref() != Some(*expected))
            {
                return Ok(None);
            }
        }
        drop(sources);
        for mapped in &manifest.locations {
            let current:bool=tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM installation_locations WHERE provider_id=?1 AND root_key=?2 AND namespace_id=?3 AND state='current')",
                rusqlite::params![manifest.provider_id,mapped.to_key,mapped.namespace_id],|r|r.get(0)
            ).map_err(backend)?;
            if !current {
                return Ok(None);
            }
            let old_current:bool=tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM installation_locations WHERE provider_id=?1 AND root_key=?2 AND state='current')",
                rusqlite::params![manifest.provider_id,mapped.from_key],|r|r.get(0)
            ).map_err(backend)?;
            if old_current {
                return Ok(None);
            }
            let expected = manifest
                .sources
                .iter()
                .filter(|s| s.namespace_id == mapped.namespace_id)
                .count() as i64;
            let count: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM source_installations WHERE namespace_id=?1",
                    [&mapped.namespace_id],
                    |r| r.get(0),
                )
                .map_err(backend)?;
            if count != expected {
                return Ok(None);
            }
        }
        for source in &manifest.sources {
            let matches:bool=tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM source_installations si JOIN source_scans ss USING(source_path)
                 WHERE si.source_key=?1 AND si.namespace_id=?2 AND ss.len_bytes=?3 AND ss.fingerprint=?4)",
                rusqlite::params![normalize_absolute_path(&source.to_path)?,source.namespace_id,source.len_bytes as i64,source.fingerprint],|r|r.get(0)
            ).map_err(backend)?;
            if !matches {
                return Ok(None);
            }
            let snapshot = capture(Path::new(&source.to_path)).map_err(source_error)?;
            if snapshot.len != source.len_bytes || snapshot.fingerprint != source.fingerprint {
                return Err(PortError::SnapshotChanged(
                    "relocation source changed after the recorded operation".into(),
                ));
            }
        }
        tx.commit().map_err(backend)?;
        Ok(Some(manifest))
    }

    /// Acquire a lease for an existing current-schema catalog without migrating
    /// or rebuilding any live projection before the relocation backup exists.
    pub fn open_for_relocation(path: &str) -> PortResult<Self> {
        let path = Path::new(path);
        let root = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let lease = WriterLease::try_acquire(root)?;
        let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(backend)?;
        conn.busy_timeout(std::time::Duration::from_secs(1))
            .map_err(backend)?;
        let schema: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(backend)?;
        if schema != SCHEMA_VERSION {
            return Err(PortError::SchemaIncompatible(
                "relocation requires the current catalog schema; run index rebuild first".into(),
            ));
        }
        Self::register_scalar_functions(&conn)?;
        let store = SqliteStore {
            conn: RefCell::new(conn),
            read_snapshot_count: Default::default(),
            read_snapshot_failed: Default::default(),
            _lease: Some(lease),
            semantic_model_id: RefCell::new(None),
            repo_slug_resolver: RefCell::new(Box::new(NoopRepoSlugResolver)),
            pending_installations: RefCell::new(BTreeMap::new()),
            relocation_clock: unix_ms,
        };
        store.recover_interrupted()?;
        Ok(store)
    }

    fn require_relocation_writer(&self) -> PortResult<()> {
        let conn = self.conn.borrow();
        let file_backed = conn.path().is_some_and(|path| !path.is_empty());
        if file_backed && self._lease.is_none() {
            return Err(PortError::WriterBusy(
                "relocation apply requires the catalog writer lease".into(),
            ));
        }
        Ok(())
    }

    /// Create a consistent, verified backup at a previously unused destination.
    /// The private temporary file is only published after verification succeeds.
    pub fn backup_to(&self, path: &str) -> PortResult<()> {
        self.require_relocation_writer()?;
        self.backup_to_inner(Path::new(path)).map_err(|_| {
            PortError::Backend(
                "catalog backup could not be created or verified; relocation was not applied"
                    .into(),
            )
        })
    }

    fn backup_to_inner(&self, path: &Path) -> PortResult<()> {
        if path.as_os_str().is_empty()
            || path.file_name().is_none()
            || backup_destination_exists(path)
        {
            return Err(invalid("backup destination must be a new file"));
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let temporary = tempfile::Builder::new()
            .prefix(".asg-backup-")
            .tempfile_in(parent)
            .map_err(backend)?;
        let _sidecars = BackupSidecars(temporary.path().to_path_buf());
        {
            let conn = self.conn.borrow();
            let tx = conn.unchecked_transaction().map_err(backend)?;
            // Pin the catalog snapshot before Backup; a WAL file is never copied
            // on its own and provider databases are never checkpointed.
            let generation: i64 = tx
                .query_row(
                    "SELECT active_generation FROM store_metadata WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )
                .map_err(backend)?;
            tx.backup(rusqlite::MAIN_DB, temporary.path(), None)
                .map_err(backend)?;
            let copied = Connection::open_with_flags(
                temporary.path(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            )
            .map_err(backend)?;
            copied
                .busy_timeout(std::time::Duration::from_secs(1))
                .map_err(backend)?;
            // Only the private backup is switched out of WAL before publication,
            // so it is self-contained and never depends on a renamed sidecar.
            copied
                .execute_batch("PRAGMA journal_mode=DELETE;")
                .map_err(backend)?;
            let check: String = copied
                .query_row("PRAGMA quick_check", [], |r| r.get(0))
                .map_err(backend)?;
            let schema: i64 = copied
                .query_row("PRAGMA user_version", [], |r| r.get(0))
                .map_err(backend)?;
            let copied_generation: i64 = copied
                .query_row(
                    "SELECT active_generation FROM store_metadata WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )
                .map_err(backend)?;
            if check != "ok" || schema != SCHEMA_VERSION || copied_generation != generation {
                return Err(invalid("backup verification failed"));
            }
            tx.commit().map_err(backend)?;
        }
        temporary.as_file().sync_all().map_err(backend)?;
        // No-clobber publication also refuses races, hard links and symlinks at
        // the requested destination, without truncating somebody else's file.
        if backup_destination_exists(path) {
            return Err(invalid("backup destination must be unused"));
        }
        let backup = temporary.persist_noclobber(path).map_err(backend)?;
        backup.sync_all().map_err(backend)?;
        #[cfg(unix)]
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(backend)?;
        Ok(())
    }

    pub fn apply_relocation(
        &self,
        provider_id: &str,
        from_root: &str,
        to_root: &str,
        alias_ttl_days: u32,
        plan_token: &str,
        backup_path: &str,
    ) -> PortResult<RelocationResult> {
        self.require_relocation_writer()?;
        let now = (self.relocation_clock)()?;
        let (_, _, mapping_key) =
            Self::relocation_inputs(provider_id, from_root, to_root, alias_ttl_days)?;
        let claims = decode_plan(plan_token, now)?;
        if let Some(manifest) = self.applied_relocation(&mapping_key, Some(&claims))? {
            return Ok(manifest.result(
                RelocationStatus::Unchanged,
                self.active_generation()?,
                None,
            ));
        }
        let generation = self.active_generation()?;
        if claims.generation != generation {
            return Err(PortError::GenerationMismatch(
                "catalog changed after relocation preview; create a new preview".into(),
            ));
        }
        let manifest =
            self.prepare_relocation(provider_id, from_root, to_root, alias_ttl_days, now)?;
        verify_plan(
            plan_token,
            SCHEMA_VERSION as u32,
            manifest.generation,
            &manifest.mapping_digest,
            alias_ttl_days,
            now,
        )?;
        if manifest.from_key == manifest.to_key {
            return Ok(manifest.result(RelocationStatus::Unchanged, manifest.generation, None));
        }
        self.backup_to(backup_path)?;
        if self.active_generation()? != manifest.generation {
            return Err(PortError::GenerationMismatch(
                "catalog changed while creating the relocation backup".into(),
            ));
        }
        manifest.verify_snapshots()?;
        let relations = RelationManifests {
            relocation: Some(manifest.clone()),
            ..Default::default()
        };
        let pending = self.begin_index_batch_with_relations(&[], &[], &relations)?;
        let batch_manifest = crate::batch_manifest(&[], &[], &relations)?;
        self.commit_index_batch_with_relations(&pending, &[], &[], &relations, &batch_manifest)?;
        self.clear_installation_reservations(
            manifest
                .sources
                .iter()
                .map(|source| source.from_path.as_str()),
        );
        Ok(manifest.result(RelocationStatus::Applied, pending.target_generation, None))
    }

    pub(super) fn apply_relocation_in_tx(
        tx: &rusqlite::Transaction<'_>,
        pending: &PendingIndexBatch,
        manifest: &RelocationManifest,
    ) -> PortResult<()> {
        manifest.validate()?;
        if manifest.generation != pending.base_generation {
            return Err(PortError::GenerationMismatch(
                "relocation generation no longer matches the plan".into(),
            ));
        }
        manifest.verify_snapshots()?;
        for source in &manifest.sources {
            let matches:bool=tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM source_installations si JOIN source_scans ss USING(source_path)
                 WHERE si.source_path=?1 AND si.namespace_id=?2 AND ss.len_bytes=?3 AND ss.fingerprint=?4)",
                rusqlite::params![source.from_path,source.namespace_id,source.len_bytes as i64,source.fingerprint],|r|r.get(0)
            ).map_err(backend)?;
            if !matches {
                return Err(invalid("relocation source ownership changed after preview"));
            }
            let occupied: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM source_installations WHERE source_key=?1)",
                    [normalize_absolute_path(&source.to_path)?],
                    |r| r.get(0),
                )
                .map_err(backend)?;
            if occupied {
                return Err(invalid("relocation target source is already occupied"));
            }
        }
        // Roots are disjoint and source keys unique, so no temporary keys or
        // overwrite/upsert behavior is needed for a source rename.
        for table in LIVE_SOURCE_TABLES {
            let sql = format!("UPDATE {table} SET source_path=?2 WHERE source_path=?1");
            let mut statement = tx.prepare(&sql).map_err(backend)?;
            for source in &manifest.sources {
                statement
                    .execute(rusqlite::params![source.from_path, source.to_path])
                    .map_err(backend)?;
            }
        }
        for source in &manifest.sources {
            tx.execute(
                "UPDATE source_installations SET source_key=?2 WHERE source_path=?1",
                rusqlite::params![source.to_path, normalize_absolute_path(&source.to_path)?],
            )
            .map_err(backend)?;
        }
        for mapped in &manifest.locations {
            let changed = tx
                .execute(
                    "UPDATE installation_locations SET state='retired',retired_until_ms=?4
                 WHERE provider_id=?1 AND root_key=?2 AND namespace_id=?3 AND state='current'",
                    rusqlite::params![
                        manifest.provider_id,
                        mapped.from_key,
                        mapped.namespace_id,
                        manifest.retired_until_ms
                    ],
                )
                .map_err(backend)?;
            if changed != 1 {
                return Err(invalid(
                    "relocation installation ownership changed after preview",
                ));
            }
            let existing:Option<(String,String,Option<i64>)>=tx.query_row("SELECT namespace_id,state,retired_until_ms FROM installation_locations WHERE provider_id=?1 AND root_key=?2",rusqlite::params![manifest.provider_id,mapped.to_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(backend)?;
            if existing.as_ref().is_some_and(|(id, state, expiry)| {
                location_blocks_relocation(
                    state,
                    *expiry,
                    id == &mapped.namespace_id,
                    manifest.operation_at_ms,
                )
            }) {
                return Err(invalid("relocation target location became occupied"));
            }
            tx.execute(
                "INSERT INTO installation_locations(provider_id,root_key,root_locator,namespace_id,state,retired_until_ms)
                 VALUES(?1,?2,?3,?4,'current',NULL)
                 ON CONFLICT(provider_id,root_key) DO UPDATE SET root_locator=excluded.root_locator,namespace_id=excluded.namespace_id,state='current',retired_until_ms=NULL",
                rusqlite::params![manifest.provider_id,mapped.to_key,mapped.to_locator,mapped.namespace_id]
            ).map_err(backend)?;
        }
        let manifest_json = serde_json::to_string(&manifest.canonical_value()).map_err(backend)?;
        tx.execute(
            "INSERT INTO installation_relocations(operation_id,mapping_key,mapping_digest,provider_id,from_key,to_key,base_generation,target_generation,alias_ttl_days,source_count,session_count,namespace_count,manifest_json)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            rusqlite::params![pending.operation_id,manifest.mapping_key,manifest.mapping_digest,manifest.provider_id,manifest.from_key,manifest.to_key,pending.base_generation as i64,pending.target_generation as i64,manifest.alias_ttl_days,manifest.sources.len() as i64,manifest.session_count as i64,manifest.locations.len() as i64,manifest_json]
        ).map_err(backend)?;
        Ok(())
    }
}

impl SqliteStore {
    pub(super) fn clear_installation_reservations<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a str>,
    ) {
        let mut pending = self.pending_installations.borrow_mut();
        for path in paths {
            if let Ok(key) = normalize_absolute_path(path) {
                pending.remove(&key);
            }
        }
    }

    pub(super) fn canonicalize_source_batches<'a>(
        &self,
        sources: &'a [SourceBatch],
    ) -> PortResult<Vec<std::borrow::Cow<'a, SourceBatch>>> {
        let conn = self.conn.borrow();
        let mut query = conn
            .prepare("SELECT source_path FROM source_installations WHERE source_key=?1")
            .map_err(backend)?;
        let mut canonical = Vec::with_capacity(sources.len());
        for source in sources {
            let stored = match normalize_absolute_path(&source.source_path) {
                Ok(key) => query
                    .query_row([key], |r| r.get::<_, String>(0))
                    .optional()
                    .map_err(backend)?,
                Err(_) => None,
            };
            if let Some(path) = stored.filter(|path| path != &source.source_path) {
                let mut source = source.clone();
                source.source_path = path;
                canonical.push(std::borrow::Cow::Owned(source));
            } else {
                canonical.push(std::borrow::Cow::Borrowed(source));
            }
        }
        Ok(canonical)
    }

    fn unbound_legacy_root(
        conn: &Connection,
        provider: &str,
        root_key: &str,
    ) -> PortResult<Option<InstallationAssignment>> {
        let mut stmt=conn.prepare(
            "SELECT ss.source_path,ss.provider_id FROM source_scans ss
             LEFT JOIN source_installations si USING(source_path)
             WHERE si.source_path IS NULL AND (ss.provider_id=?1 OR EXISTS(
                SELECT 1 FROM source_session_resume_claims rc WHERE rc.source_path=ss.source_path AND rc.provider_id=?1))
             AND (ss.len_bytes IS NOT NULL OR ss.fingerprint IS NOT NULL OR EXISTS(
                SELECT 1 FROM source_membership sm WHERE sm.source_path=ss.source_path))"
        ).map_err(backend)?;
        let mut candidate: Option<InstallationAssignment> = None;
        for row in stmt
            .query_map([provider], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
            })
            .map_err(backend)?
        {
            let (path, scan_provider) = row.map_err(backend)?;
            let Ok(root) =
                installation_root(&path, provider).and_then(|root| normalize_absolute_path(&root))
            else {
                continue;
            };
            if root != root_key {
                continue;
            }
            let Some(assignment) = Self::legacy_assignment(conn, &path, scan_provider.as_deref())?
            else {
                // A source without proof is kept readable but cannot establish
                // or conflict with a registry namespace until an explicit scan
                // supplies its frozen legacy seed and matching claims.
                continue;
            };
            if candidate
                .as_ref()
                .is_some_and(|candidate| candidate.namespace_input != assignment.namespace_input)
            {
                return Err(invalid(
                    "normalized location would merge distinct legacy installation identities",
                ));
            }
            candidate = Some(assignment);
        }
        Ok(candidate)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod empty_placeholder_review_tests {
    use super::*;

    fn placeholder_source(path: &str) -> SourceBatch {
        let fingerprint = blake3::hash(b"").to_hex().to_string();
        let document = StableId::derive(
            IdKind::Document,
            Stability::Reconstructed,
            &[b"empty", b"empty", fingerprint.as_bytes()],
        );
        let session = StableId::derive(
            IdKind::Session,
            Stability::Reconstructed,
            &[document.as_str().as_bytes()],
        );
        assert_eq!(document.as_str(), "doc_v1_9d688f0b845e2b6f7adb7a9eda35c150");
        assert_eq!(session.as_str(), "ses_v1_bf6e9effacfd4419e7a57dd0d881f0c6");
        SourceBatch {
            source_path: path.into(),
            entries: vec![
                (
                    session,
                    serde_json::to_vec(&serde_json::json!({
                        "document": document.as_str(),
                        "documents": [document.as_str()],
                        "messages": [],
                    }))
                    .unwrap(),
                    String::new(),
                ),
                (
                    document,
                    serde_json::to_vec(&serde_json::json!({
                        "provider": "empty", "variant": "empty",
                        "fingerprint": fingerprint, "len": 0,
                    }))
                    .unwrap(),
                    String::new(),
                ),
            ],
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(0),
            fingerprint: Some(fingerprint),
            provider_id: None,
            resume_claims: Vec::new(),
        }
    }

    fn seed_placeholder(store: &SqliteStore, source: &SourceBatch) -> String {
        store
            .resolve_or_allocate_installation_namespace(
                "empty",
                &source.source_path,
                &legacy_installation_namespace(&source.source_path, "empty"),
            )
            .unwrap();
        store
            .commit_source_batches_if_changed(std::slice::from_ref(source))
            .unwrap();
        let conn = store.conn.borrow();
        let assignment = SqliteStore::assignment_for_source(&conn, &source.source_path)
            .unwrap()
            .unwrap();
        assert!(
            SqliteStore::repairable_empty_placeholder(
                &conn,
                &source.source_path,
                &assignment.namespace_id,
            )
            .unwrap()
        );
        assignment.namespace_id
    }

    fn replacement_source(store: &SqliteStore, path: &str) -> SourceBatch {
        let namespace = store
            .resolve_or_allocate_installation_namespace(
                "claude-code",
                path,
                &legacy_installation_namespace(path, "claude-code"),
            )
            .unwrap();
        let mut source = placeholder_source(path);
        let document = StableId::derive(
            IdKind::Document,
            Stability::Reconstructed,
            &[b"new document"],
        );
        let session = StableId::native_session_scoped(
            &SessionIdentityNamespace {
                provider_id: "claude-code",
                installation_namespace: &namespace,
            },
            "real-session",
        );
        let message = StableId::native_checked(IdKind::Message, "real-message").unwrap();
        source.entries = vec![
            (
                document.clone(),
                serde_json::to_vec(&serde_json::json!({
                    "provider": "claude-code", "variant": "claude-code/jsonl-v1",
                    "fingerprint": "nonempty", "len": 8,
                }))
                .unwrap(),
                String::new(),
            ),
            (
                session.clone(),
                serde_json::to_vec(&serde_json::json!({
                    "document": document.as_str(), "documents": [document.as_str()],
                    "messages": [message.as_str()],
                }))
                .unwrap(),
                String::new(),
            ),
            (
                message.clone(),
                serde_json::to_vec(&serde_json::json!({
                    "role": "user", "text": "real body", "timestamp": null,
                }))
                .unwrap(),
                "real body".into(),
            ),
        ];
        source.placements = vec![MessagePlacement::new(
            session.clone(),
            document,
            message,
            0,
            false,
            None,
        )];
        source.resume_claims = vec![SourceResumeClaim::from_observation(
            "claude-code",
            session.as_str(),
            &agent_session_grep_ports::ProviderSessionObservation {
                provider_session_id: agent_session_grep_ports::MetadataResolution::Resolved(
                    "real-session".into(),
                ),
                original_working_directory: agent_session_grep_ports::MetadataResolution::Missing,
                pair_observed: false,
                multi_session: false,
            },
        )];
        source.len_bytes = Some(8);
        source.fingerprint = Some("nonempty".into());
        source
    }

    fn live_state(store: &SqliteStore) -> BTreeMap<String, Vec<Vec<rusqlite::types::Value>>> {
        let conn = store.conn.borrow();
        let tables: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'
             AND name<>'index_batches' ORDER BY name",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        tables
            .into_iter()
            .map(|table| {
                let mut statement = conn
                    .prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))
                    .unwrap();
                let width = statement.column_count();
                let mut rows: Vec<Vec<rusqlite::types::Value>> = statement
                    .query_map([], |row| (0..width).map(|column| row.get(column)).collect())
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
                rows.sort_by_cached_key(|row| format!("{row:?}"));
                (table, rows)
            })
            .collect()
    }

    #[test]
    fn empty_repair_preserves_shared_placeholders_until_the_last_source_replaces_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("first.jsonl")
            .to_string_lossy()
            .into_owned();
        let other_path = dir
            .path()
            .join("second.jsonl")
            .to_string_lossy()
            .into_owned();
        let store = SqliteStore::open_in_memory().unwrap();
        let source = placeholder_source(&path);
        let namespace = seed_placeholder(&store, &source);
        assert_eq!(
            seed_placeholder(&store, &placeholder_source(&other_path)),
            namespace
        );
        let replacement = replacement_source(&store, &path);
        assert!(
            store
                .commit_source_batches_if_changed(&[replacement])
                .unwrap()
        );
        for (id, payload, _) in &source.entries {
            assert_eq!(
                store.get(id).unwrap(),
                Some(payload.clone()),
                "shared placeholder must survive"
            );
        }
        let conn = store.conn.borrow();
        let other = SqliteStore::assignment_for_source(&conn, &other_path)
            .unwrap()
            .unwrap();
        assert_eq!(other.namespace_id, namespace);
        assert_eq!(other.provider_id, "empty");
        let repaired = SqliteStore::assignment_for_source(&conn, &path)
            .unwrap()
            .unwrap();
        assert_eq!(repaired.provider_id, "claude-code");
        assert_ne!(repaired.namespace_id, namespace);
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM source_membership WHERE source_path=?1",
                [&other_path],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            2
        );
        assert_eq!(
            conn.query_row(
                "SELECT provider_id FROM source_scans WHERE source_path=?1",
                [&path],
                |row| row.get::<_, Option<String>>(0)
            )
            .unwrap(),
            None
        );
        drop(conn);
        let replacement = replacement_source(&store, &other_path);
        assert!(
            store
                .commit_source_batches_if_changed(&[replacement])
                .unwrap()
        );
        for (id, _, _) in &source.entries {
            assert_eq!(
                store.get(id).unwrap(),
                None,
                "last replacement retires only the empty placeholder"
            );
        }
        assert_eq!(
            store
                .conn
                .borrow()
                .query_row(
                    "SELECT COUNT(*) FROM installation_namespaces WHERE namespace_id=?1",
                    [&namespace],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1,
            "repair is not namespace garbage collection"
        );
    }

    #[test]
    fn empty_repair_revalidates_reservations_and_rolls_back_failed_activation() {
        for failure in ["changed-proof", "activation"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir
                .path()
                .join("source.jsonl")
                .to_string_lossy()
                .into_owned();
            let store = SqliteStore::open_in_memory().unwrap();
            seed_placeholder(&store, &placeholder_source(&path));
            let replacement = replacement_source(&store, &path);
            let conn = store.conn.borrow();
            if failure == "changed-proof" {
                conn.execute(
                    "UPDATE source_scans SET provider_id='claude-code' WHERE source_path=?1",
                    [&path],
                )
                .unwrap();
            } else {
                conn.execute_batch(
                    "CREATE TRIGGER reject_placeholder_rebind BEFORE UPDATE ON source_installations
                     BEGIN SELECT RAISE(ABORT, 'injected placeholder activation failure'); END;",
                )
                .unwrap();
            }
            drop(conn);
            let before = live_state(&store);
            let error = store
                .commit_source_batches_if_changed(&[replacement])
                .unwrap_err();
            if failure == "changed-proof" {
                assert!(matches!(error, PortError::InvalidRequest(_)), "{error:?}");
            }
            assert_eq!(
                live_state(&store),
                before,
                "{failure}: no live table or generation may change"
            );
        }
    }

    #[test]
    fn empty_repair_keeps_real_provider_and_retired_alias_boundaries_closed() {
        for real_provider in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir
                .path()
                .join("source.jsonl")
                .to_string_lossy()
                .into_owned();
            let store = SqliteStore::open_in_memory().unwrap();
            let namespace = seed_placeholder(&store, &placeholder_source(&path));
            let conn = store.conn.borrow();
            if real_provider {
                conn.execute("UPDATE installation_namespaces SET provider_id='claude-code' WHERE namespace_id=?1", [&namespace]).unwrap();
            } else {
                conn.execute("UPDATE installation_locations SET state='retired', retired_until_ms=?1 WHERE namespace_id=?2", rusqlite::params![i64::MAX, namespace]).unwrap();
            }
            assert!(!SqliteStore::repairable_empty_placeholder(&conn, &path, &namespace).unwrap());
        }
    }
    #[test]
    fn empty_repair_rejects_nonplaceholder_catalog_and_identity_evidence() {
        for defect in [
            "document",
            "session",
            "sidecar",
            "sidecar-kind",
            "sidecar-value",
            "sidecar-native",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir
                .path()
                .join("source.jsonl")
                .to_string_lossy()
                .into_owned();
            let store = SqliteStore::open_in_memory().unwrap();
            let source = placeholder_source(&path);
            let namespace = seed_placeholder(&store, &source);
            let session = &source.entries[0].0;
            let document = &source.entries[1].0;
            let conn = store.conn.borrow();
            if defect == "document" || defect == "session" {
                let (id, payload) = if defect == "document" {
                    (
                        document,
                        serde_json::json!({
                            "provider": "claude-code", "variant": "claude-code/jsonl",
                            "fingerprint": "real-content", "len": 42,
                        }),
                    )
                } else {
                    (
                        session,
                        serde_json::json!({
                            "document": document.as_str(), "documents": [document.as_str()],
                            "messages": ["msg_v1_real-history"],
                        }),
                    )
                };
                conn.execute(
                    "UPDATE catalog SET payload=?1 WHERE id=?2",
                    rusqlite::params![serde_json::to_vec(&payload).unwrap(), id.as_str()],
                )
                .unwrap();
            } else {
                let mut identity = serde_json::to_value(document).unwrap();
                match defect {
                    "sidecar" => identity = serde_json::json!({"stability": "Reconstructed"}),
                    "sidecar-kind" => identity["kind"] = serde_json::json!("Message"),
                    "sidecar-native" => identity["stability"] = serde_json::json!("Native"),
                    "sidecar-value" => identity["value"] = serde_json::json!("doc_v1_other"),
                    _ => unreachable!(),
                }
                conn.execute(
                    "UPDATE fts_ids SET id_json=?1 WHERE wire_id=?2",
                    rusqlite::params![identity.to_string(), document.as_str()],
                )
                .unwrap();
            }
            assert!(
                !SqliteStore::repairable_empty_placeholder(&conn, &path, &namespace).unwrap(),
                "{defect} is not proof of an empty placeholder"
            );
        }
    }

    #[test]
    fn empty_repair_allows_only_missing_metadata_for_the_actual_placeholder() {
        for defect in [
            "none",
            "cwd",
            "ambiguous-cwd",
            "pair",
            "provider",
            "session",
            "unknown-state",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir
                .path()
                .join("source.jsonl")
                .to_string_lossy()
                .into_owned();
            let store = SqliteStore::open_in_memory().unwrap();
            let source = placeholder_source(&path);
            let namespace = seed_placeholder(&store, &source);
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO source_session_resume_claims(
                    source_path, session_id, provider_id, provider_session_id,
                    provider_session_id_state, original_working_directory,
                    original_working_directory_state, pair_observed)
                 VALUES(?1, ?2, ?3, NULL, 'missing', ?4, ?5, ?6)",
                rusqlite::params![
                    path,
                    if defect == "session" {
                        "ses_v1_other"
                    } else {
                        source.entries[0].0.as_str()
                    },
                    if defect == "provider" {
                        "claude-code"
                    } else {
                        "empty"
                    },
                    (defect == "cwd").then_some("synthetic-working-directory"),
                    match defect {
                        "cwd" => "resolved",
                        "ambiguous-cwd" => "ambiguous",
                        "unknown-state" => "unknown",
                        _ => "missing",
                    },
                    i64::from(defect == "pair"),
                ],
            )
            .unwrap();
            assert_eq!(
                SqliteStore::repairable_empty_placeholder(&conn, &path, &namespace).unwrap(),
                defect == "none",
                "{defect}: only a fully missing claim for the proven placeholder is repairable"
            );
        }
    }
}
