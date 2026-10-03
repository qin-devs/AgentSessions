//! Optional local Candle embedding backend (`semantic-candle` feature).
//!
//! Loads a pinned multilingual-e5-small bundle from a local directory — never
//! downloads, never opens a socket. The bundle must contain:
//!
//! - `config.json`
//! - `tokenizer.json`
//! - `model.safetensors`
//! - `MODEL-MANIFEST.json` (id / dimension / license / per-file sha256)
//!
//! Architecture adapted from Recall (MIT) `src/embedding.rs`: BertModel + mean
//! pooling + L2 + E5 `query:`/`passage:` prefixes. The download path is
//! intentionally absent so the zero-egress audit stays green.

#![cfg(feature = "semantic-candle")]

use crate::embedding::BIGRAM_HASH_DIMENSION;
use agent_session_grep_ports::{EmbeddingManifest, EmbeddingModel, PortError, PortResult};
use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config, DTYPE};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

/// Stable model id recorded on every stored vector. Changing pooling, prefix
/// policy, precision, or truncation MUST change this id so old vectors stay inert.
pub const E5_SMALL_MODEL_ID: &str =
    "intfloat-multilingual-e5-small@614241f6-candle-f32-meanpool-l2-qpass-v1";

/// Files that must be present in an imported bundle.
pub const REQUIRED_BUNDLE_FILES: &[&str] = &[
    "config.json",
    "tokenizer.json",
    "model.safetensors",
    "MODEL-MANIFEST.json",
];

/// On-disk bundle manifest. Written by `asg model import` after hash verification.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
pub struct ModelBundleManifest {
    pub model_id: String,
    pub dimension: usize,
    pub license: String,
    pub files: Vec<ModelBundleFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
pub struct ModelBundleFile {
    pub name: String,
    pub sha256: String,
    pub size_bytes: u64,
}

/// Local Candle multilingual-e5-small encoder.
pub struct CandleE5Model {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
    manifest: EmbeddingManifest,
}

impl CandleE5Model {
    /// Load a verified local bundle once per process and cache it.
    ///
    /// The first semantic search in a long-lived process (MCP / TUI / serve)
    /// pays the ~seconds model-load cost; every later query reuses the cached
    /// encoder. One-shot CLI invocations still pay the load once per process —
    /// that cost is reported honestly by the benchmark, not hidden.
    ///
    /// The first load failure is also cached for this process. After importing
    /// or repairing a bundle, restart a long-lived caller before retrying. This
    /// does not persist failures across CLI invocations or process restarts.
    pub fn load_cached(dir: impl AsRef<Path>) -> PortResult<&'static CandleE5Model> {
        static CACHE: std::sync::OnceLock<Result<CandleE5Model, String>> =
            std::sync::OnceLock::new();
        let cached = CACHE
            .get_or_init(|| CandleE5Model::load_from_dir(dir).map_err(|error| error.to_string()));
        cached
            .as_ref()
            .map_err(|error| PortError::Backend(error.clone()))
    }

    /// Load a verified local bundle. Fails closed on any missing/mismatched file.
    pub fn load_from_dir(dir: impl AsRef<Path>) -> PortResult<Self> {
        let dir = dir.as_ref();
        let bundle = read_and_verify_bundle(dir)?;
        if bundle.model_id != E5_SMALL_MODEL_ID {
            return Err(PortError::Backend(format!(
                "unsupported model_id `{}` (expected `{E5_SMALL_MODEL_ID}`)",
                bundle.model_id
            )));
        }
        if bundle.dimension != BIGRAM_HASH_DIMENSION {
            return Err(PortError::Backend(format!(
                "model dimension {} does not match storage layout {}",
                bundle.dimension, BIGRAM_HASH_DIMENSION
            )));
        }

        let device = Device::Cpu;
        let config_path = dir.join("config.json");
        let tokenizer_path = dir.join("tokenizer.json");
        let weights_path = dir.join("model.safetensors");

        let config: Config = serde_json::from_str(
            &fs::read_to_string(&config_path)
                .map_err(|e| PortError::Backend(format!("read config.json: {e}")))?,
        )
        .map_err(|e| PortError::Backend(format!("parse config.json: {e}")))?;

        // SAFETY: weights_path is a verified local file under the model cache;
        // the mmap is read-only for the lifetime of the model.
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&[weights_path], DTYPE, &device)
                .map_err(|e| PortError::Backend(format!("load safetensors: {e}")))?
        };
        let model = BertModel::load(vb, &config)
            .map_err(|e| PortError::Backend(format!("load BertModel: {e}")))?;

        let mut tokenizer = Tokenizer::from_file(&tokenizer_path)
            .map_err(|e| PortError::Backend(format!("load tokenizer: {e}")))?;
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..Default::default()
        }));
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: 512,
                ..Default::default()
            }))
            .map_err(|e| PortError::Backend(format!("tokenizer truncation: {e}")))?;

        let weights_hash = bundle
            .files
            .iter()
            .find(|f| f.name == "model.safetensors")
            .map(|f| f.sha256.clone())
            .unwrap_or_else(|| "missing".into());

        Ok(Self {
            model,
            tokenizer,
            device,
            manifest: EmbeddingManifest {
                model_id: E5_SMALL_MODEL_ID.to_string(),
                file_hash: format!("sha256:{weights_hash}"),
                dimension: BIGRAM_HASH_DIMENSION,
                license: bundle.license,
            },
        })
    }

    fn embed_batch(&self, texts: &[&str]) -> PortResult<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let encodings = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|e| PortError::Backend(format!("tokenize: {e}")))?;

        let token_ids = encodings
            .iter()
            .map(|e| {
                Tensor::new(e.get_ids(), &self.device)
                    .map_err(|err| PortError::Backend(format!("token tensor: {err}")))
            })
            .collect::<PortResult<Vec<_>>>()?;
        let token_ids = Tensor::stack(&token_ids, 0)
            .map_err(|e| PortError::Backend(format!("stack tokens: {e}")))?;

        let attention_masks = encodings
            .iter()
            .map(|e| {
                Tensor::new(e.get_attention_mask(), &self.device)
                    .map_err(|err| PortError::Backend(format!("mask tensor: {err}")))
            })
            .collect::<PortResult<Vec<_>>>()?;
        let attention_mask = Tensor::stack(&attention_masks, 0)
            .map_err(|e| PortError::Backend(format!("stack masks: {e}")))?;
        let token_type_ids = token_ids
            .zeros_like()
            .map_err(|e| PortError::Backend(format!("token type ids: {e}")))?;

        let hidden = self
            .model
            .forward(&token_ids, &token_type_ids, Some(&attention_mask))
            .map_err(|e| PortError::Backend(format!("bert forward: {e}")))?;

        let mask = attention_mask
            .to_dtype(DTYPE)
            .map_err(|e| PortError::Backend(format!("mask dtype: {e}")))?
            .unsqueeze(2)
            .map_err(|e| PortError::Backend(format!("mask unsqueeze: {e}")))?;
        let sum_mask = mask
            .sum(1)
            .map_err(|e| PortError::Backend(format!("mask sum: {e}")))?;
        let pooled = hidden
            .broadcast_mul(&mask)
            .map_err(|e| PortError::Backend(format!("broadcast mul: {e}")))?
            .sum(1)
            .map_err(|e| PortError::Backend(format!("hidden sum: {e}")))?;
        let pooled = pooled
            .broadcast_div(&sum_mask)
            .map_err(|e| PortError::Backend(format!("mean pool: {e}")))?;
        let norm = pooled
            .sqr()
            .map_err(|e| PortError::Backend(format!("sqr: {e}")))?
            .sum_keepdim(1)
            .map_err(|e| PortError::Backend(format!("norm sum: {e}")))?
            .sqrt()
            .map_err(|e| PortError::Backend(format!("sqrt: {e}")))?;
        let normalized = pooled
            .broadcast_div(&norm)
            .map_err(|e| PortError::Backend(format!("l2 normalize: {e}")))?;
        normalized
            .to_vec2::<f32>()
            .map_err(|e| PortError::Backend(format!("to_vec2: {e}")))
    }
}

impl EmbeddingModel for CandleE5Model {
    fn embed(&self, text: &str, is_query: bool) -> PortResult<Vec<f32>> {
        let prefixed = if is_query {
            format!("query: {text}")
        } else {
            format!("passage: {text}")
        };
        let mut batch = self.embed_batch(&[prefixed.as_str()])?;
        batch
            .pop()
            .ok_or_else(|| PortError::Backend("empty embedding batch".into()))
    }

    fn dimension(&self) -> usize {
        BIGRAM_HASH_DIMENSION
    }

    fn manifest(&self) -> &EmbeddingManifest {
        &self.manifest
    }
}

/// Verify a local model bundle directory and return its manifest.
///
/// Does not load weights — only checks presence + SHA-256 of each declared file.
pub fn read_and_verify_bundle(dir: &Path) -> PortResult<ModelBundleManifest> {
    if !dir.is_dir() {
        return Err(PortError::Backend(format!(
            "model bundle is not a directory: {}",
            dir.display()
        )));
    }
    for required in REQUIRED_BUNDLE_FILES {
        let path = dir.join(required);
        if !path.is_file() {
            return Err(PortError::Backend(format!(
                "model bundle missing required file `{required}` under {}",
                dir.display()
            )));
        }
    }
    let raw = fs::read_to_string(dir.join("MODEL-MANIFEST.json"))
        .map_err(|e| PortError::Backend(format!("read MODEL-MANIFEST.json: {e}")))?;
    let manifest: ModelBundleManifest = serde_json::from_str(&raw)
        .map_err(|e| PortError::Backend(format!("parse MODEL-MANIFEST.json: {e}")))?;
    let mut declared = std::collections::BTreeSet::new();
    for file in &manifest.files {
        let mut components = Path::new(&file.name).components();
        if !matches!(components.next(), Some(std::path::Component::Normal(_)))
            || components.next().is_some()
            || file.name.contains(['\\', ':'])
        {
            return Err(PortError::Backend(
                "model bundle manifest contains an unsafe file name".into(),
            ));
        }
        if !declared.insert(file.name.as_str()) {
            return Err(PortError::Backend(
                "model bundle manifest contains duplicate file entries".into(),
            ));
        }
    }
    for required in REQUIRED_BUNDLE_FILES {
        if *required != "MODEL-MANIFEST.json" && !declared.contains(required) {
            return Err(PortError::Backend(format!(
                "model bundle manifest missing required content entry `{required}`"
            )));
        }
    }
    for file in &manifest.files {
        let path = dir.join(&file.name);
        let data =
            fs::read(&path).map_err(|e| PortError::Backend(format!("read {}: {e}", file.name)))?;
        if data.len() as u64 != file.size_bytes {
            return Err(PortError::Backend(format!(
                "{} size mismatch: got {} expected {}",
                file.name,
                data.len(),
                file.size_bytes
            )));
        }
        let digest = blake3_sha256_hex(&data);
        if !digest.eq_ignore_ascii_case(&file.sha256) {
            return Err(PortError::Backend(format!("{} sha256 mismatch", file.name)));
        }
    }
    Ok(manifest)
}

/// Import a verified local bundle into the model cache (atomic publish).
///
/// `source_dir` is the user-provided verified bundle; `cache_root` is the
/// platform cache directory (e.g. from `config paths`). The published path is
/// `{cache_root}/models/{model_id}/`.
pub fn import_bundle(source_dir: &Path, cache_root: &Path) -> PortResult<PathBuf> {
    let manifest = read_and_verify_bundle(source_dir)?;
    let target = cache_root
        .join("models")
        .join(sanitize_model_id(&manifest.model_id));
    if target.exists() {
        // Re-verify existing publish; if good, return it. Never silently overwrite.
        let existing = read_and_verify_bundle(&target)?;
        if existing == manifest {
            return Ok(target);
        }
        return Err(PortError::Backend(format!(
            "model cache already has a different bundle at {}",
            target.display()
        )));
    }
    let staging = cache_root.join("models").join(format!(
        ".staging-{}-{}",
        sanitize_model_id(&manifest.model_id),
        std::process::id()
    ));
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .map_err(|e| PortError::Backend(format!("clear staging: {e}")))?;
    }
    fs::create_dir_all(&staging).map_err(|e| PortError::Backend(format!("create staging: {e}")))?;
    for required in REQUIRED_BUNDLE_FILES {
        fs::copy(source_dir.join(required), staging.join(required))
            .map_err(|e| PortError::Backend(format!("copy {required}: {e}")))?;
    }
    // Re-verify the staged copy before publish.
    let _ = read_and_verify_bundle(&staging)?;
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| PortError::Backend(format!("create model cache parent: {e}")))?;
    }
    fs::rename(&staging, &target)
        .map_err(|e| PortError::Backend(format!("publish bundle: {e}")))?;
    Ok(target)
}

/// Default model cache path under the platform cache root.
pub fn default_model_dir(cache_root: &Path) -> PathBuf {
    cache_root
        .join("models")
        .join(sanitize_model_id(E5_SMALL_MODEL_ID))
}

fn sanitize_model_id(model_id: &str) -> String {
    model_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '@' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// SHA-256 hex digest of `data`. Implemented with a pure-Rust portable hash so
/// we do not pull in an extra crypto crate; blake3 is already a workspace dep
/// but the published contract records SHA-256 (HF/LFS convention). We therefore
/// implement a minimal SHA-256 here.
fn blake3_sha256_hex(data: &[u8]) -> String {
    // Prefer real SHA-256 when available via a tiny pure implementation.
    sha256_hex(data)
}

// Minimal SHA-256 (public domain style) so the import path stays dependency-free.
fn sha256_hex(data: &[u8]) -> String {
    let hash = sha256(data);
    let mut out = String::with_capacity(64);
    for byte in hash {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn sha256(msg: &[u8]) -> [u8; 32] {
    // Based on FIPS 180-4; compact single-file implementation for offline verify.
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let bit_len = (msg.len() as u64).saturating_mul(8);
    let mut buf = msg.to_vec();
    buf.push(0x80);
    while (buf.len() % 64) != 56 {
        buf.push(0);
    }
    buf.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in buf.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn sha256_empty_and_abc() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn sha256_known_vectors_long_inputs() {
        // FIPS 180-4 multi-block vectors; exercise the padding + chunk loop
        // beyond the single-block cases above.
        assert_eq!(
            sha256_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        let million_a = vec![b'a'; 1_000_000];
        assert_eq!(
            sha256_hex(&million_a),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn verify_bundle_rejects_missing_files() {
        let dir = tempfile_dir();
        let err = read_and_verify_bundle(&dir).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("missing required file"), "{msg}");
    }

    #[test]
    fn verify_bundle_requires_hash_coverage_for_every_content_file() {
        for omitted in [
            None,
            Some("config.json"),
            Some("tokenizer.json"),
            Some("model.safetensors"),
        ] {
            let dir = tempfile_dir();
            write_minimal_bundle(&dir);
            let mut manifest = read_manifest(&dir);
            match omitted {
                None => manifest.files.clear(),
                Some(name) => manifest.files.retain(|file| file.name != name),
            }
            write_manifest(&dir, &manifest);
            assert!(
                read_and_verify_bundle(&dir).is_err(),
                "accepted missing coverage: {omitted:?}"
            );
        }
    }

    #[test]
    fn verify_bundle_rejects_duplicate_manifest_entries() {
        let dir = tempfile_dir();
        write_minimal_bundle(&dir);
        let mut manifest = read_manifest(&dir);
        manifest.files.push(manifest.files[0].clone());
        write_manifest(&dir, &manifest);
        assert!(read_and_verify_bundle(&dir).is_err());
    }

    #[test]
    fn verify_bundle_rejects_size_mismatch() {
        let dir = tempfile_dir();
        write_minimal_bundle(&dir);
        let mut manifest = read_manifest(&dir);
        manifest.files[0].size_bytes += 1;
        write_manifest(&dir, &manifest);
        let err = read_and_verify_bundle(&dir).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("size mismatch"), "{msg}");
    }

    #[test]
    fn verify_bundle_rejects_sha256_mismatch() {
        let dir = tempfile_dir();
        write_minimal_bundle(&dir);
        // Flip a byte in place after the manifest was written: the declared
        // size still matches, but the digest no longer does.
        flip_first_byte(&dir.join("model.safetensors"), b'X');
        let err = read_and_verify_bundle(&dir).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("sha256 mismatch"), "{msg}");
    }

    #[test]
    fn verify_bundle_rejects_declared_file_missing() {
        let dir = tempfile_dir();
        write_minimal_bundle(&dir);
        // Manifest declares a file that is absent from the directory.
        let mut manifest = read_manifest(&dir);
        manifest.files.push(ModelBundleFile {
            name: "extra.bin".to_string(),
            sha256: "ab".repeat(32),
            size_bytes: 1,
        });
        write_manifest(&dir, &manifest);
        let err = read_and_verify_bundle(&dir).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("read extra.bin"), "{msg}");
    }

    #[test]
    fn verify_bundle_rejects_malformed_manifest() {
        let dir = tempfile_dir();
        // Model files present, manifest not JSON: parsing must fail closed.
        for name in REQUIRED_BUNDLE_FILES {
            if *name != "MODEL-MANIFEST.json" {
                fs::write(dir.join(name), b"x").unwrap();
            }
        }
        fs::write(dir.join("MODEL-MANIFEST.json"), b"{not json").unwrap();
        let err = read_and_verify_bundle(&dir).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("parse MODEL-MANIFEST.json"), "{msg}");
    }

    #[test]
    fn import_bundle_round_trip() {
        let source = tempfile_dir();
        let cache = tempfile_dir();
        write_minimal_bundle(&source);
        let published = import_bundle(&source, &cache).expect("import");
        assert!(published.join("MODEL-MANIFEST.json").is_file());
        // Second import of identical bundle is idempotent.
        let again = import_bundle(&source, &cache).expect("re-import");
        assert_eq!(published, again);
    }

    #[test]
    fn import_bundle_publishes_to_sanitized_model_id_dir() {
        let source = tempfile_dir();
        let cache = tempfile_dir();
        write_minimal_bundle(&source);
        let published = import_bundle(&source, &cache).expect("import");
        assert_eq!(
            published,
            cache
                .join("models")
                .join(sanitize_model_id(E5_SMALL_MODEL_ID))
        );
        // Only the published model dir remains — no staging leftovers.
        let names: Vec<String> = fs::read_dir(cache.join("models"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec![sanitize_model_id(E5_SMALL_MODEL_ID)]);
    }

    #[test]
    fn import_bundle_tampered_source_fails_without_touching_cache() {
        let source = tempfile_dir();
        let cache = tempfile_dir();
        write_minimal_bundle(&source);
        flip_first_byte(&source.join("tokenizer.json"), b'X');
        let err = import_bundle(&source, &cache).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("sha256 mismatch"), "{msg}");
        let models = cache.join("models");
        if models.exists() {
            assert_eq!(fs::read_dir(&models).unwrap().count(), 0);
        }
    }

    #[test]
    fn import_bundle_reverifies_published_copy_on_reimport() {
        let source = tempfile_dir();
        let cache = tempfile_dir();
        write_minimal_bundle(&source);
        let published = import_bundle(&source, &cache).expect("import");
        // Tamper the published copy: a re-import must detect it and never
        // silently "repair" or overwrite.
        flip_first_byte(&published.join("model.safetensors"), b'X');
        let err = import_bundle(&source, &cache).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("sha256 mismatch"), "{msg}");
        let tampered = fs::read(published.join("model.safetensors")).unwrap();
        assert_eq!(tampered[0], b'X');
    }

    #[test]
    fn import_bundle_rejects_conflicting_existing_publish() {
        let source = tempfile_dir();
        let cache = tempfile_dir();
        write_minimal_bundle(&source);
        let published = import_bundle(&source, &cache).expect("import");
        // Replace the published manifest with a different (but internally
        // consistent) one: same model id, different weights bytes.
        let other = tempfile_dir();
        write_minimal_bundle_with_bytes(&other, b"different-weight-bytes");
        fs::copy(
            other.join("model.safetensors"),
            published.join("model.safetensors"),
        )
        .unwrap();
        fs::copy(
            other.join("MODEL-MANIFEST.json"),
            published.join("MODEL-MANIFEST.json"),
        )
        .unwrap();
        let err = import_bundle(&source, &cache).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("already has a different bundle"), "{msg}");
        // The conflicting publish was not overwritten.
        let stored = fs::read(published.join("model.safetensors")).unwrap();
        assert!(stored.ends_with(b"different-weight-bytes"));
    }

    #[test]
    fn load_from_dir_rejects_wrong_model_id_before_loading_weights() {
        let dir = tempfile_dir();
        write_minimal_bundle(&dir);
        let mut manifest = read_manifest(&dir);
        manifest.model_id = "someone-elses-model@v9".to_string();
        write_manifest(&dir, &manifest);
        let err = match CandleE5Model::load_from_dir(&dir) {
            Err(err) => err,
            Ok(_) => panic!("load must fail for a mismatched model_id"),
        };
        let msg = format!("{err}");
        assert!(msg.contains("unsupported model_id"), "{msg}");
    }

    #[test]
    fn load_from_dir_rejects_dimension_mismatch_before_loading_weights() {
        let dir = tempfile_dir();
        write_minimal_bundle(&dir);
        let mut manifest = read_manifest(&dir);
        manifest.dimension = 768;
        write_manifest(&dir, &manifest);
        let err = match CandleE5Model::load_from_dir(&dir) {
            Err(err) => err,
            Ok(_) => panic!("load must fail for a dimension mismatch"),
        };
        let msg = format!("{err}");
        assert!(msg.contains("does not match storage layout"), "{msg}");
    }

    #[test]
    fn sanitize_model_id_replaces_unsafe_characters() {
        assert_eq!(sanitize_model_id("a/b\\c d"), "a_b_c_d");
        // The pinned id is already sanitized-safe.
        assert_eq!(sanitize_model_id(E5_SMALL_MODEL_ID), E5_SMALL_MODEL_ID);
    }

    #[test]
    fn manifest_serde_round_trip() {
        let dir = tempfile_dir();
        write_minimal_bundle(&dir);
        let manifest = read_and_verify_bundle(&dir).expect("verify");
        assert_eq!(manifest.model_id, E5_SMALL_MODEL_ID);
        assert_eq!(manifest.dimension, BIGRAM_HASH_DIMENSION);
        assert_eq!(manifest.files.len(), 3);
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let decoded: ModelBundleManifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(manifest, decoded);
    }

    #[test]
    fn tempfile_dirs_are_isolated_for_parallel_callers_at_one_clock_tick() {
        let tick = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dirs = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..16)
                .map(|_| scope.spawn(|| tempfile_dir_at(tick)))
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        let distinct: std::collections::BTreeSet<_> = dirs.iter().collect();
        assert_eq!(distinct.len(), dirs.len());
        for dir in dirs {
            fs::remove_dir(dir).unwrap();
        }
    }

    fn tempfile_dir() -> PathBuf {
        tempfile_dir_at(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        )
    }

    fn tempfile_dir_at(tick: u128) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // Wall-clock nanoseconds can repeat across parallel callers (e.g. macOS).
        let serial = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "asg-model-test-{}-{tick}-{serial}",
            std::process::id()
        ));
        // Never silently reuse another fixture if a path unexpectedly exists.
        fs::create_dir(&dir).unwrap();
        dir
    }

    /// Overwrite the first byte of a file in place (length unchanged) so the
    /// digest check — not the size check — is what rejects it.
    fn flip_first_byte(path: &Path, byte: u8) {
        let mut f = fs::OpenOptions::new().write(true).open(path).unwrap();
        f.write_all(&[byte]).unwrap();
        f.flush().unwrap();
    }

    fn read_manifest(dir: &Path) -> ModelBundleManifest {
        let raw = fs::read_to_string(dir.join("MODEL-MANIFEST.json")).unwrap();
        serde_json::from_str(&raw).unwrap()
    }

    fn write_manifest(dir: &Path, manifest: &ModelBundleManifest) {
        fs::write(
            dir.join("MODEL-MANIFEST.json"),
            serde_json::to_string_pretty(manifest).unwrap(),
        )
        .unwrap();
    }

    fn write_minimal_bundle(dir: &Path) {
        write_minimal_bundle_with_bytes(dir, b"not-a-real-weight");
    }

    fn write_minimal_bundle_with_bytes(dir: &Path, weights: &[u8]) {
        // Minimal valid-looking files for hash/size verification only — not a
        // loadable model. Load tests require a real e5-small bundle.
        let files = [
            (
                "config.json",
                b"{\"architectures\":[\"BertModel\"]}" as &[u8],
            ),
            ("tokenizer.json", b"{\"version\":\"1.0\"}"),
            ("model.safetensors", weights),
        ];
        let mut manifest_files = Vec::new();
        for (name, bytes) in files {
            let mut f = fs::File::create(dir.join(name)).unwrap();
            f.write_all(bytes).unwrap();
            manifest_files.push(ModelBundleFile {
                name: name.to_string(),
                sha256: sha256_hex(bytes),
                size_bytes: bytes.len() as u64,
            });
        }
        let manifest = ModelBundleManifest {
            model_id: E5_SMALL_MODEL_ID.to_string(),
            dimension: BIGRAM_HASH_DIMENSION,
            license: "MIT (intfloat/multilingual-e5-small model weights)".into(),
            files: manifest_files,
        };
        write_manifest(dir, &manifest);
    }
}
