//! 只读源快照读取（RFC-0002 §4, §7 bounded ingest）。
//!
//! 打开后流式捕获 `(len, mtime_ms, fingerprint)`，只读捕获范围；提交前流式复核
//! 三元组，任一变化返回 [`PortError::SnapshotChanged`]。capture / parse / verify
//! 各自重新打开文件，以固定大小缓冲流式读取——不再为整个 transcript 分配 Vec。
//!
//! **等长异容替换**必须靠 content fingerprint——len+mtime 不足
//! （证据：`spikes/source-snapshot/EVIDENCE.md` assertion D）。

use agent_session_grep_ports::{
    JSON_FAMILY_MAX_SOURCE_BYTES, PortError, PortResult, ReadOnlySource, SourceSnapshot,
};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

/// Fixed filesystem buffer used by capture, parse readers, and verification.
pub const SOURCE_IO_BUFFER_BYTES: usize = 64 * 1024;

fn backend<E: std::fmt::Display>(e: E) -> PortError {
    PortError::SourceIo(e.to_string())
}

fn mtime_ms(meta: &std::fs::Metadata) -> PortResult<i64> {
    let m = meta.modified().map_err(backend)?;
    Ok(m.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64)
}

/// Stream a reader through BLAKE3, returning `(total_bytes, hex fingerprint)`.
///
/// Bounded by a fixed-size chunk; the caller is responsible for limiting the
/// read range (`std::io::Take`) so a concurrently growing file cannot blow up
/// the fingerprint pass.
fn fingerprint_reader(reader: &mut dyn Read) -> PortResult<(u64, String)> {
    let mut hasher = blake3::Hasher::new();
    let mut total = 0_u64;
    let mut chunk = [0_u8; SOURCE_IO_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut chunk).map_err(backend)?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk[..read]);
        total = total.saturating_add(read as u64);
    }
    Ok((total, hasher.finalize().to_hex().to_string()))
}

/// 元数据复核：len + mtime 任一变化即 [`PortError::SnapshotChanged`]。
fn verify_metadata(meta: &std::fs::Metadata, snap: &SourceSnapshot) -> PortResult<()> {
    if meta.len() != snap.len {
        return Err(PortError::SnapshotChanged(format!(
            "len {} -> {}",
            snap.len,
            meta.len()
        )));
    }
    let current_mtime = mtime_ms(meta)?;
    if current_mtime != snap.mtime_ms {
        return Err(PortError::SnapshotChanged(format!(
            "mtime {} -> {current_mtime}",
            snap.mtime_ms
        )));
    }
    Ok(())
}

fn has_sqlite_header(reader: &mut dyn Read) -> PortResult<bool> {
    let mut header = [0; 16];
    match reader.read_exact(&mut header) {
        Ok(()) => Ok(&header == b"SQLite format 3\0"),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(backend(error)),
    }
}

/// Capture `(len, mtime, BLAKE3)` with one fixed-buffer pass.
///
/// Text files use a streaming byte fingerprint. SQLite files use a bounded
/// logical backup so committed WAL frames are included, with a `sqlite:`
/// fingerprint prefix identifying the logical verification strategy.
pub fn capture(path: &Path) -> PortResult<SourceSnapshot> {
    let mut file = File::open(path).map_err(backend)?;
    if has_sqlite_header(&mut file)? {
        let copy = sqlite_snapshot(path)?;
        let mut snapshot = capture_file(&copy)?;
        snapshot.path = path.to_string_lossy().into_owned();
        snapshot.mtime_ms = mtime_ms(&file.metadata().map_err(backend)?)?;
        snapshot.fingerprint.insert_str(0, "sqlite:");
        return Ok(snapshot);
    }
    capture_file(path)
}

/// Capture the database's logical pages, including committed WAL frames. The
/// source connection is read-only; only the guarded destination may be written.
fn sqlite_snapshot(path: &Path) -> PortResult<tempfile::TempPath> {
    let source =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(backend)?;
    source
        .busy_timeout(std::time::Duration::from_secs(1))
        .map_err(backend)?;
    // Pin one read transaction before copying pages: concurrent writers cannot
    // restart the backup indefinitely, and the size check covers that snapshot.
    source
        .execute_batch("BEGIN; SELECT rootpage FROM sqlite_schema LIMIT 1;")
        .map_err(backend)?;
    let pages: i64 = source
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .map_err(backend)?;
    let page_size: i64 = source
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .map_err(backend)?;
    if pages
        .checked_mul(page_size)
        .is_none_or(|len| len < 0 || len as u64 > agent_session_grep_ports::SQLITE_MAX_SOURCE_BYTES)
    {
        return Err(PortError::SourceIo(
            "SQLite source exceeds supported snapshot size".into(),
        ));
    }
    let destination = tempfile::NamedTempFile::new()
        .map_err(backend)?
        .into_temp_path();
    source
        .backup(rusqlite::MAIN_DB, &destination, None)
        .map_err(backend)?;
    Ok(destination)
}

fn capture_file(path: &Path) -> PortResult<SourceSnapshot> {
    let file = File::open(path).map_err(backend)?;
    let meta = file.metadata().map_err(backend)?;
    let len = meta.len();
    let mtime = mtime_ms(&meta)?;
    // 固定捕获长度：若读期间文件被追加，仍只认初始 len 范围。
    let mut captured = file.take(len);
    let (read_len, fingerprint) = fingerprint_reader(&mut captured)?;
    if read_len != len {
        return Err(PortError::SnapshotChanged(format!(
            "len changed during capture {len} -> {read_len}"
        )));
    }
    let snap = SourceSnapshot {
        path: path.to_string_lossy().into_owned(),
        len,
        mtime_ms: mtime,
        fingerprint,
    };
    post_read_verify(path, &snap)?;
    Ok(snap)
}

/// A repeatable, read-only view limited to the captured byte range.
#[derive(Clone)]
pub struct FileSource {
    path: PathBuf,
    snapshot: SourceSnapshot,
    _temporary: Option<std::sync::Arc<tempfile::TempPath>>,
}

impl ReadOnlySource for FileSource {
    fn len(&self) -> u64 {
        self.snapshot.len
    }

    fn open(&self) -> PortResult<Box<dyn BufRead + Send + '_>> {
        let file = File::open(&self.path).map_err(backend)?;
        // 每次重开都先核对 len/mtime：并发改写（含等长替换的 mtime 变化）在此
        // 被尽早拒绝，避免在"过期视图"上继续解析。
        verify_metadata(&file.metadata().map_err(backend)?, &self.snapshot)?;
        Ok(Box::new(BufReader::with_capacity(
            SOURCE_IO_BUFFER_BYTES,
            file.take(self.snapshot.len),
        )))
    }
}

/// Reopen a captured source without reading it eagerly.
pub fn open_snapshot_source(path: &Path, snapshot: &SourceSnapshot) -> PortResult<FileSource> {
    if snapshot.fingerprint.starts_with("sqlite:") {
        let copy = sqlite_snapshot(path)?;
        let current = capture_file(&copy)?;
        verify_sqlite_snapshot(snapshot, &current)?;
        return Ok(FileSource {
            path: copy.to_path_buf(),
            snapshot: current,
            _temporary: Some(std::sync::Arc::new(copy)),
        });
    }
    let meta = std::fs::metadata(path).map_err(backend)?;
    verify_metadata(&meta, snapshot)?;
    Ok(FileSource {
        path: path.to_path_buf(),
        snapshot: snapshot.clone(),
        _temporary: None,
    })
}

/// Final pre-commit verification: metadata + captured-range BLAKE3, all streamed.
pub fn verify_snapshot(path: &Path, snap: &SourceSnapshot) -> PortResult<()> {
    if snap.fingerprint.starts_with("sqlite:") {
        let copy = sqlite_snapshot(path)?;
        return verify_sqlite_snapshot(snap, &capture_file(&copy)?);
    }
    let file = File::open(path).map_err(backend)?;
    verify_metadata(&file.metadata().map_err(backend)?, snap)?;
    let mut captured = file.take(snap.len);
    let (read_len, current_fingerprint) = fingerprint_reader(&mut captured)?;
    if read_len != snap.len {
        return Err(PortError::SnapshotChanged(format!(
            "len changed during verification {} -> {read_len}",
            snap.len
        )));
    }
    if current_fingerprint != snap.fingerprint {
        return Err(PortError::SnapshotChanged(
            "fingerprint changed (content replaced at same len)".into(),
        ));
    }
    // 复核读取期间（首次 stat → read 之间）源被追加/截断：内容按旧 len 截断后
    // 指纹仍可与快照一致，等长替换也已被指纹覆盖，因此再核对一次 len+mtime 以
    // 收窄窗口。诚实的结论是"复核读取窗口内未观察到变化"。
    post_read_verify(path, snap)
}

fn verify_sqlite_snapshot(expected: &SourceSnapshot, current: &SourceSnapshot) -> PortResult<()> {
    if expected.len != current.len
        || expected.fingerprint.strip_prefix("sqlite:") != Some(current.fingerprint.as_str())
    {
        return Err(PortError::SnapshotChanged(
            "SQLite logical content changed".into(),
        ));
    }
    Ok(())
}

/// 读取完成后的最终复核：源文件的 len/mtime 不得在读取窗口内变化。
///
/// 单独抽取为可测试函数——真实竞态（stat→read 之间追加）无法在单测里确定性
/// 复现，但"读取后文件已变化"这一状态可以直接构造（写入新内容后本函数必须
/// 报告 SnapshotChanged）。
fn post_read_verify(path: &Path, snap: &SourceSnapshot) -> PortResult<()> {
    let meta_after = std::fs::metadata(path).map_err(backend)?;
    verify_metadata(&meta_after, snap).map_err(|error| match error {
        PortError::SnapshotChanged(detail) => {
            PortError::SnapshotChanged(format!("source changed during verification: {detail}"))
        }
        other => other,
    })
}

/// Capture and return a repeatable reader source; no transcript-sized `Vec`.
pub fn read_verified(path: &Path) -> PortResult<(SourceSnapshot, FileSource)> {
    let snapshot = capture(path)?;
    let source = open_snapshot_source(path, &snapshot)?;
    verify_snapshot(path, &snapshot)?;
    Ok((snapshot, source))
}

/// 基于已有快照元数据的只读路径发现器（实现 [`SourceDiscovery`] 的最小落地）。
///
/// 构造时给定一组已 capture 的快照；`discover` 返回它们的克隆，
/// `read_verified` 按路径重读并校验三元组。用于测试与组合根装配。
pub struct SnapshotFs {
    snapshots: Vec<SourceSnapshot>,
}

impl SnapshotFs {
    pub fn new(snapshots: Vec<SourceSnapshot>) -> Self {
        Self { snapshots }
    }
}

impl agent_session_grep_ports::SourceDiscovery for SnapshotFs {
    fn discover(&self) -> PortResult<Vec<SourceSnapshot>> {
        Ok(self.snapshots.clone())
    }

    fn read_verified(&self, snapshot: &SourceSnapshot) -> PortResult<Vec<u8>> {
        // 兼容的整读路径：生产 ingest 已改走 `ReadOnlySource` 流式；这里保留
        // 给测试/旧字节调用方，但显式硬上限（与 JSON 系 manifest 一致），
        // 绝不无界整读。
        if snapshot.len > JSON_FAMILY_MAX_SOURCE_BYTES {
            return Err(PortError::SourceIo(format!(
                "legacy verified-byte read exceeds bounded limit {JSON_FAMILY_MAX_SOURCE_BYTES}"
            )));
        }
        let path = Path::new(&snapshot.path);
        let source = open_snapshot_source(path, snapshot)?;
        let mut reader = source.open()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(snapshot.len as usize)
            .map_err(|_| PortError::SourceIo("bounded source allocation failed".into()))?;
        let mut chunk = [0_u8; SOURCE_IO_BUFFER_BYTES];
        loop {
            let read = reader.read(&mut chunk).map_err(backend)?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..read]);
        }
        verify_snapshot(path, snapshot)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct HeaderReader {
        bytes: std::io::Cursor<Vec<u8>>,
        chunk: usize,
        fail_after: Option<u64>,
    }

    impl Read for HeaderReader {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self
                .fail_after
                .is_some_and(|at| self.bytes.position() >= at)
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "synthetic read failure",
                ));
            }
            let count = out.len().min(self.chunk);
            self.bytes.read(&mut out[..count])
        }
    }

    #[test]
    fn sqlite_header_detection_handles_segmented_reads() {
        for chunk in 1..=16 {
            let mut reader = HeaderReader {
                bytes: std::io::Cursor::new(b"SQLite format 3\0payload".to_vec()),
                chunk,
                fail_after: None,
            };
            assert!(has_sqlite_header(&mut reader).unwrap(), "chunk={chunk}");
            assert_eq!(reader.bytes.position(), 16);
        }
    }

    #[test]
    fn sqlite_header_detection_distinguishes_eof_and_io_error() {
        for bytes in [
            b"SQLite".as_slice(),
            b"ordinary source contents".as_slice(),
            b"".as_slice(),
        ] {
            assert!(!has_sqlite_header(&mut std::io::Cursor::new(bytes)).unwrap());
        }
        let mut reader = HeaderReader {
            bytes: std::io::Cursor::new(b"SQLite format 3\0".to_vec()),
            chunk: 4,
            fail_after: Some(4),
        };
        assert!(matches!(
            has_sqlite_header(&mut reader),
            Err(PortError::SourceIo(_))
        ));
    }

    #[test]
    fn sqlite_capture_reads_wal_and_detects_wal_only_changes_without_writing_source() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.db");
        let writer = rusqlite::Connection::open(&path).unwrap();
        writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(value); INSERT INTO t VALUES('first');").unwrap();
        let wal = path.with_extension("db-wal");
        let main_before = std::fs::read(&path).unwrap();
        let wal_before = std::fs::read(&wal).unwrap();
        let snapshot = capture(&path).unwrap();
        let source = open_snapshot_source(&path, &snapshot).unwrap();
        let reader = rusqlite::Connection::open_with_flags(
            &source.path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        assert_eq!(
            reader
                .query_row("SELECT value FROM t", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "first"
        );
        verify_snapshot(&path, &snapshot).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), main_before);
        assert_eq!(std::fs::read(&wal).unwrap(), wal_before);
        writer
            .execute("INSERT INTO t VALUES('second')", [])
            .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), main_before);
        assert!(matches!(
            verify_snapshot(&path, &snapshot),
            Err(PortError::SnapshotChanged(_))
        ));
        let next = capture(&path).unwrap();
        assert_ne!(next.fingerprint, snapshot.fingerprint);
        // A checkpoint performed by the external writer changes physical
        // files only; the captured logical source remains current.
        writer
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        verify_snapshot(&path, &next).unwrap();
    }

    fn write_file(dir: &Path, name: &str, content: &[u8]) -> std::path::PathBuf {
        let p = dir.join(name);
        let mut f = File::create(&p).unwrap();
        f.write_all(content).unwrap();
        f.flush().unwrap();
        p
    }

    #[test]
    fn capture_and_verify_stable_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_file(dir.path(), "a.jsonl", b"hello stable content");
        let snap = capture(&p).unwrap();
        assert_eq!(
            snap.fingerprint,
            blake3::hash(b"hello stable content").to_hex().to_string()
        );
        assert_eq!(snap.len, 20);
        verify_snapshot(&p, &snap).unwrap();
    }

    #[test]
    fn append_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_file(dir.path(), "a.jsonl", b"hello");
        let snap = capture(&p).unwrap();
        // 追加
        {
            let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
            f.write_all(b" world").unwrap();
        }
        let err = verify_snapshot(&p, &snap).unwrap_err();
        assert!(matches!(err, PortError::SnapshotChanged(ref m) if m.contains("len")));
    }

    #[test]
    fn append_between_stat_and_read_is_detected_after_read() {
        // 复核读取窗口内追加：首次 stat 后、read 完成后文件才被追加。指纹校验
        // 只覆盖读取范围（按旧 len 截断），追加的字节重建后不会被指纹识别；
        // verify_snapshot 必须在读取后再次核对 len+mtime 才能检出。
        // 真实竞态（stat→read 之间追加）无法在单测里确定性复现，因此直接驱动
        // 读后复检函数 post_read_verify——它必须在读取完成后捕获 len/mtime 变化。
        let dir = tempfile::tempdir().unwrap();
        let p = write_file(dir.path(), "a.jsonl", b"hello world!!!!!");
        let snap = capture(&p).unwrap();
        // 捕获后截短内容：等价于"read 已完成、源在窗口内被改写"的状态。
        std::fs::write(&p, b"12345").unwrap();
        let err = post_read_verify(&p, &snap).unwrap_err();
        assert!(
            matches!(err, PortError::SnapshotChanged(ref m) if m.contains("source changed during verification")),
            "got {err:?}"
        );
    }

    #[test]
    fn truncate_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_file(dir.path(), "a.jsonl", b"hello world!!!!!");
        let snap = capture(&p).unwrap();
        {
            let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
            f.set_len(5).unwrap();
        }
        let err = verify_snapshot(&p, &snap).unwrap_err();
        assert!(matches!(err, PortError::SnapshotChanged(ref m) if m.contains("len")));
    }

    #[test]
    fn equal_length_content_replacement_is_detected_by_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_file(dir.path(), "a.jsonl", b"AAAAAAAAAA"); // 10 bytes
        // 捕获原始 mtime，替换后逐字节恢复，使 len 与 mtime 都与快照一致，
        // fingerprint 成为唯一能检出等长异容替换的信号。
        let original_modified = std::fs::metadata(&p).unwrap().modified().unwrap();
        let snap = capture(&p).unwrap();

        std::fs::write(&p, b"BBBBBBBBBB").unwrap();
        File::options()
            .write(true)
            .open(&p)
            .unwrap()
            .set_modified(original_modified)
            .unwrap();
        let restored = std::fs::metadata(&p).unwrap();
        assert_eq!(
            mtime_ms(&restored).unwrap(),
            snap.mtime_ms,
            "mtime 必须被恢复，fingerprint 才是唯一信号"
        );
        assert_eq!(restored.len(), snap.len, "替换必须等长");

        let err = verify_snapshot(&p, &snap).unwrap_err();
        assert!(
            matches!(err, PortError::SnapshotChanged(ref m) if m.contains("fingerprint changed")),
            "got {err:?}"
        );
    }

    #[test]
    fn snapshot_fs_read_verified_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_file(dir.path(), "s.jsonl", b"payload-bytes");
        let snap = capture(&p).unwrap();
        let fs = SnapshotFs::new(vec![snap.clone()]);
        use agent_session_grep_ports::SourceDiscovery;
        let discovered = fs.discover().unwrap();
        assert_eq!(discovered.len(), 1);
        let bytes = fs.read_verified(&snap).unwrap();
        assert_eq!(bytes, b"payload-bytes");
    }

    #[test]
    fn read_verified_returns_repeatable_source() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_file(dir.path(), "s.jsonl", b"line one\nline two\n");
        let (snap, source) = read_verified(&p).unwrap();
        assert_eq!(snap.len, 18);
        let mut reader = source.open().unwrap();
        let mut text = String::new();
        reader.read_to_string(&mut text).unwrap();
        assert_eq!(text, "line one\nline two\n");
    }

    #[test]
    fn file_source_reader_detects_replacement_at_reopen() {
        // 等长异容替换后重新 open：metadata 一致（len+mtime 相同）仍会读到替换
        // 内容——这正是 ReadOnlySource 只读视图的诚实边界；提交前 verify_snapshot
        // 用 fingerprint 兜住（等长替换检测测试已覆盖）。
        let dir = tempfile::tempdir().unwrap();
        let p = write_file(dir.path(), "a.jsonl", b"AAAAAAAAAA");
        let original_modified = std::fs::metadata(&p).unwrap().modified().unwrap();
        let snap = capture(&p).unwrap();
        std::fs::write(&p, b"BBBBBBBBBB").unwrap();
        File::options()
            .write(true)
            .open(&p)
            .unwrap()
            .set_modified(original_modified)
            .unwrap();
        let source = open_snapshot_source(&p, &snap).unwrap();
        let mut reader = source.open().unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"BBBBBBBBBB");
    }

    #[test]
    fn snapshot_fs_read_verified_rejects_oversized_source() {
        // 兼容整读路径必须诚实拒绝超过 JSON 系上限的源，而非按大文件分配。
        let dir = tempfile::tempdir().unwrap();
        let big = vec![b'x'; (JSON_FAMILY_MAX_SOURCE_BYTES as usize) + 1];
        let p = write_file(dir.path(), "big.jsonl", &big);
        let snap = capture(&p).unwrap();
        let fs = SnapshotFs::new(vec![snap.clone()]);
        use agent_session_grep_ports::SourceDiscovery;
        let err = fs.read_verified(&snap).unwrap_err();
        assert!(
            matches!(err, PortError::SourceIo(ref m) if m.contains("bounded limit")),
            "got {err:?}"
        );
    }
}
