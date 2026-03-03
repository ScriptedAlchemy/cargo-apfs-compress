use anyhow::{Context, Result, anyhow};
use applesauce::FileCompressor;
use applesauce::compressor::Kind;
use applesauce::progress::Progress as _;
use clap::{ArgAction, Parser, ValueEnum};
use indicatif::HumanBytes;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

mod flock;
mod progress;

use crate::flock::Filesystem;
use crate::progress::{ProgressBars, Verbosity};

const CARGO_LOCK_NAME: &str = ".cargo-lock";

const ROOT_SKIP_DIRS: &[&str] = &["tmp"];
const PROFILE_SKIP_DIRS: &[&str] = &[".fingerprint", "build", "deps", "examples", "incremental"];

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum CompressionArg {
    Lzfse,
    Zlib,
    Lzvn,
}

impl CompressionArg {
    fn to_kind(self) -> Kind {
        match self {
            Self::Lzfse => Kind::Lzfse,
            Self::Zlib => Kind::Zlib,
            Self::Lzvn => Kind::Lzvn,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, ValueEnum)]
pub enum CacheArg {
    Cargo,
    NodeModules,
    Go,
}

#[derive(Debug, Parser)]
#[command(name = "cargo-apfs-compress")]
pub struct Cli {
    /// Finds and compresses all profiles by default. Use this to restrict which profiles are
    /// compressed.
    #[arg(long = "profile")]
    pub profiles: Vec<String>,

    /// Finds all platform targets by default. Use this to restrict which target platforms are
    /// compressed.
    #[arg(long = "target")]
    pub targets: Vec<String>,

    #[arg(long = "compression", value_enum, default_value = "lzfse")]
    pub compression: CompressionArg,

    #[arg(short = 'v', long = "verbose", action = ArgAction::Count, conflicts_with = "quiet")]
    pub verbose: u8,

    #[arg(short = 'q', long = "quiet", action = ArgAction::Count, conflicts_with = "verbose")]
    pub quiet: u8,

    /// Cache sources to include. Defaults to cargo when no cache sources or cache-dir are set.
    #[arg(long = "cache", value_enum)]
    pub caches: Vec<CacheArg>,

    /// Additional cache directories to compress.
    #[arg(long = "cache-dir")]
    pub cache_dirs: Vec<PathBuf>,

    /// Show what would be compressed without writing changes.
    #[arg(long = "dry-run", alias = "preview")]
    pub dry_run: bool,
}

impl Cli {
    fn verbosity(&self) -> Verbosity {
        if self.quiet > 0 {
            Verbosity::Quiet
        } else if self.verbose > 0 {
            Verbosity::Verbose
        } else {
            Verbosity::Normal
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LockPolicy {
    CargoLock,
    None,
}

#[derive(Clone, Debug)]
struct WorkDirSpec {
    path: PathBuf,
    lock_policy: LockPolicy,
    sources: BTreeSet<String>,
}

impl WorkDirSpec {
    fn source_summary(&self) -> String {
        self.sources.iter().cloned().collect::<Vec<_>>().join(",")
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DirSummary {
    files: u64,
    bytes: u64,
}

#[derive(Deserialize)]
struct GoEnvOutput {
    #[serde(rename = "GOCACHE")]
    gocache: Option<PathBuf>,
    #[serde(rename = "GOMODCACHE")]
    gomodcache: Option<PathBuf>,
}

pub fn resolve_cargo_exe() -> String {
    match std::env::var("CARGO") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => "cargo".to_owned(),
    }
}

#[derive(Deserialize)]
struct MetadataOutput {
    target_directory: PathBuf,
}

pub fn run_cargo_metadata(cargo_exe: &str, cwd: &Path) -> Result<PathBuf> {
    let output = Command::new(cargo_exe)
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to execute `{cargo_exe} metadata`"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!(
            "`{cargo_exe} metadata` failed with status {}: {stderr}",
            output.status
        ));
    }

    let metadata: MetadataOutput = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("failed to parse `{cargo_exe} metadata` output"))?;
    Ok(metadata.target_directory)
}

pub fn load_profile_dir_name_overrides(cwd: &Path) -> Result<HashMap<String, String>> {
    let mut roots = Vec::new();
    let mut current = Some(cwd);
    while let Some(path) = current {
        roots.push(path.to_path_buf());
        current = path.parent();
    }
    roots.reverse();

    let mut overrides = HashMap::new();
    for root in roots {
        for candidate in [
            root.join(".cargo").join("config"),
            root.join(".cargo").join("config.toml"),
        ] {
            if !candidate.is_file() {
                continue;
            }
            let content = fs::read_to_string(&candidate)
                .with_context(|| format!("failed reading {}", candidate.display()))?;
            let value: toml::Value = toml::from_str(&content)
                .with_context(|| format!("failed parsing {}", candidate.display()))?;
            if let Some(profile_table) = value.get("profile").and_then(toml::Value::as_table) {
                for (name, profile_value) in profile_table {
                    let dir_name = profile_value
                        .get("dir-name")
                        .and_then(toml::Value::as_str)
                        .map(ToOwned::to_owned);
                    if let Some(dir_name) = dir_name {
                        overrides.insert(name.to_owned(), dir_name);
                    }
                }
            }
        }
    }

    Ok(overrides)
}

pub fn resolve_profile_dir_name(profile: &str, overrides: &HashMap<String, String>) -> String {
    if let Some(override_dir) = overrides.get(profile) {
        return override_dir.clone();
    }

    match profile {
        "dev" | "test" => "debug".to_owned(),
        "bench" | "release" => "release".to_owned(),
        custom => custom.to_owned(),
    }
}

pub fn resolve_work_dirs(
    target_dir: &Path,
    profiles: &[String],
    targets: &[String],
    overrides: &HashMap<String, String>,
) -> Vec<PathBuf> {
    let mut out = BTreeSet::new();

    for profile in profiles {
        let profile_dir = resolve_profile_dir_name(profile, overrides);
        if targets.is_empty() {
            out.insert(target_dir.join(&profile_dir));
        } else {
            for target in targets {
                out.insert(target_dir.join(target).join(&profile_dir));
            }
        }
    }

    out.into_iter().collect()
}

fn selected_cache_sources(caches: &[CacheArg], cache_dirs: &[PathBuf]) -> BTreeSet<CacheArg> {
    if caches.is_empty() {
        if cache_dirs.is_empty() {
            [CacheArg::Cargo].into_iter().collect()
        } else {
            BTreeSet::new()
        }
    } else {
        caches.iter().copied().collect()
    }
}

fn validate_cli(cli: &Cli, selected_caches: &BTreeSet<CacheArg>) -> Result<()> {
    if !selected_caches.contains(&CacheArg::Cargo)
        && (!cli.profiles.is_empty() || !cli.targets.is_empty())
    {
        return Err(anyhow!(
            "--profile/--target are cargo-specific and require --cache cargo"
        ));
    }
    Ok(())
}

fn parse_go_env_output(stdout: &[u8]) -> Result<Vec<PathBuf>> {
    let parsed: GoEnvOutput =
        serde_json::from_slice(stdout).context("failed to parse `go env` JSON output")?;

    let mut dirs = BTreeSet::new();
    if let Some(path) = parsed.gocache {
        if !path.as_os_str().is_empty() {
            dirs.insert(path);
        }
    }
    if let Some(path) = parsed.gomodcache {
        if !path.as_os_str().is_empty() {
            dirs.insert(path);
        }
    }

    Ok(dirs.into_iter().collect())
}

fn resolve_go_cache_dirs(cwd: &Path) -> Result<Vec<PathBuf>> {
    let output = Command::new("go")
        .args(["env", "-json", "GOCACHE", "GOMODCACHE"])
        .current_dir(cwd)
        .output()
        .context("failed to execute `go env` for cache discovery")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!(
            "`go env` failed with status {}: {stderr}",
            output.status
        ));
    }
    parse_go_env_output(&output.stdout)
}

fn resolve_custom_cache_dirs(cwd: &Path, cache_dirs: &[PathBuf]) -> Vec<PathBuf> {
    cache_dirs
        .iter()
        .map(|dir| {
            if dir.is_absolute() {
                dir.clone()
            } else {
                cwd.join(dir)
            }
        })
        .collect()
}

fn add_work_dir_spec(
    specs: &mut BTreeMap<PathBuf, WorkDirSpec>,
    path: PathBuf,
    lock_policy: LockPolicy,
    source: &str,
) {
    match specs.get_mut(&path) {
        Some(existing) => {
            if lock_policy == LockPolicy::CargoLock {
                existing.lock_policy = LockPolicy::CargoLock;
            }
            existing.sources.insert(source.to_owned());
        }
        None => {
            let mut sources = BTreeSet::new();
            sources.insert(source.to_owned());
            specs.insert(
                path.clone(),
                WorkDirSpec {
                    path,
                    lock_policy,
                    sources,
                },
            );
        }
    }
}

fn resolve_work_dir_specs(cli: &Cli, cwd: &Path) -> Result<Vec<WorkDirSpec>> {
    let selected_caches = selected_cache_sources(&cli.caches, &cli.cache_dirs);
    validate_cli(cli, &selected_caches)?;

    let mut specs = BTreeMap::new();

    if selected_caches.contains(&CacheArg::Cargo) {
        let cargo_exe = resolve_cargo_exe();
        let target_dir = run_cargo_metadata(&cargo_exe, cwd)?;
        let dirs = if cli.profiles.is_empty() {
            discover_default_work_dirs(&target_dir, &cli.targets)?
        } else {
            let overrides = load_profile_dir_name_overrides(cwd)?;
            resolve_work_dirs(&target_dir, &cli.profiles, &cli.targets, &overrides)
        };
        for dir in dirs {
            add_work_dir_spec(&mut specs, dir, LockPolicy::CargoLock, "cargo");
        }
    }

    if selected_caches.contains(&CacheArg::NodeModules) {
        add_work_dir_spec(
            &mut specs,
            cwd.join("node_modules"),
            LockPolicy::None,
            "node-modules",
        );
    }

    if selected_caches.contains(&CacheArg::Go) {
        for dir in resolve_go_cache_dirs(cwd)? {
            add_work_dir_spec(&mut specs, dir, LockPolicy::None, "go");
        }
    }

    for dir in resolve_custom_cache_dirs(cwd, &cli.cache_dirs) {
        add_work_dir_spec(&mut specs, dir, LockPolicy::None, "custom");
    }

    Ok(specs.into_values().collect())
}

fn is_hidden(name: &str) -> bool {
    name.starts_with('.')
}

fn looks_like_target_triple(name: &str) -> bool {
    name.matches('-').count() >= 2
}

fn should_skip_root_dir(name: &str) -> bool {
    is_hidden(name) || ROOT_SKIP_DIRS.contains(&name)
}

fn should_skip_profile_dir(name: &str) -> bool {
    is_hidden(name) || PROFILE_SKIP_DIRS.contains(&name)
}

pub fn discover_default_work_dirs(target_dir: &Path, targets: &[String]) -> Result<Vec<PathBuf>> {
    let mut out = BTreeSet::new();
    let target_filters: BTreeSet<&str> = targets.iter().map(String::as_str).collect();

    for entry in fs::read_dir(target_dir)
        .with_context(|| format!("failed reading {}", target_dir.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed reading entry in {}", target_dir.display()))?;
        if !entry.file_type()?.is_dir() {
            continue;
        }

        let root_name = entry.file_name().to_string_lossy().to_string();
        if should_skip_root_dir(&root_name) {
            continue;
        }

        if !target_filters.is_empty() {
            if !target_filters.contains(root_name.as_str()) {
                continue;
            }
            for child in fs::read_dir(entry.path())
                .with_context(|| format!("failed reading {}", entry.path().display()))?
            {
                let child = child.with_context(|| {
                    format!("failed reading entry in {}", entry.path().display())
                })?;
                if !child.file_type()?.is_dir() {
                    continue;
                }
                let child_name = child.file_name().to_string_lossy().to_string();
                if should_skip_profile_dir(&child_name) {
                    continue;
                }
                out.insert(child.path());
            }
            continue;
        }

        if looks_like_target_triple(&root_name) {
            for child in fs::read_dir(entry.path())
                .with_context(|| format!("failed reading {}", entry.path().display()))?
            {
                let child = child.with_context(|| {
                    format!("failed reading entry in {}", entry.path().display())
                })?;
                if !child.file_type()?.is_dir() {
                    continue;
                }
                let child_name = child.file_name().to_string_lossy().to_string();
                if should_skip_profile_dir(&child_name) {
                    continue;
                }
                out.insert(child.path());
            }
        } else {
            out.insert(entry.path());
        }
    }

    Ok(out.into_iter().collect())
}

pub trait Compressor: Send + Sync {
    fn compress_paths(
        &self,
        paths: &[PathBuf],
        compression: Kind,
        progress: &ProgressBars,
    ) -> Result<()>;
}

#[derive(Default)]
pub struct ApplesauceCompressor;

impl Compressor for ApplesauceCompressor {
    fn compress_paths(
        &self,
        paths: &[PathBuf],
        compression: Kind,
        progress: &ProgressBars,
    ) -> Result<()> {
        let mut compressor = FileCompressor::new();
        let refs: Vec<&Path> = paths.iter().map(PathBuf::as_path).collect();
        compressor.recursive_compress(refs, compression, 1.0, 2, &progress, false);
        Ok(())
    }
}

fn collect_input_paths(
    dir: &Path,
    exclude_name: Option<&OsStr>,
    progress: &ProgressBars,
) -> Result<Vec<PathBuf>> {
    let mut inputs = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("failed reading {}", dir.display()))? {
        let entry = entry.with_context(|| format!("failed reading entry in {}", dir.display()))?;
        if exclude_name.is_some_and(|excluded| entry.file_name() == excluded) {
            let excluded = exclude_name
                .and_then(OsStr::to_str)
                .unwrap_or("<unknown exclusion>");
            progress.println_verbose(|| format!("exclude {excluded} from {}", dir.display()));
            continue;
        }
        inputs.push(entry.path());
    }
    Ok(inputs)
}

fn with_work_dir_inputs<T>(
    spec: &WorkDirSpec,
    progress: &ProgressBars,
    mut op: impl FnMut(&[PathBuf]) -> Result<T>,
) -> Result<Option<T>> {
    if !spec.path.exists() {
        progress.println_normal(|| format!("skip {} (missing)", spec.path.display()));
        return Ok(None);
    }
    if !spec.path.is_dir() {
        return Err(anyhow!("{} is not a directory", spec.path.display()));
    }

    let out = match spec.lock_policy {
        LockPolicy::CargoLock => {
            let fs = Filesystem::new(spec.path.clone());
            let _lock = fs
                .open_rw_exclusive_create(CARGO_LOCK_NAME, "build directory", progress)
                .with_context(|| format!("failed to lock {}", spec.path.display()))?;
            let inputs =
                collect_input_paths(&spec.path, Some(OsStr::new(CARGO_LOCK_NAME)), progress)?;
            op(&inputs)?
        }
        LockPolicy::None => {
            let inputs = collect_input_paths(&spec.path, None, progress)?;
            op(&inputs)?
        }
    };

    Ok(Some(out))
}

fn summarize_paths(paths: &[PathBuf]) -> Result<DirSummary> {
    let mut summary = DirSummary::default();
    let mut stack = paths.to_vec();
    while let Some(path) = stack.pop() {
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("failed to read metadata for {}", path.display()))?;
        if metadata.is_file() {
            summary.files += 1;
            summary.bytes += metadata.len();
            continue;
        }
        if metadata.is_dir() {
            for entry in
                fs::read_dir(&path).with_context(|| format!("failed reading {}", path.display()))?
            {
                let entry =
                    entry.with_context(|| format!("failed reading entry in {}", path.display()))?;
                stack.push(entry.path());
            }
        }
    }
    Ok(summary)
}

fn preview_work_dir(spec: &WorkDirSpec, progress: &ProgressBars) -> Result<()> {
    let summary = with_work_dir_inputs(spec, progress, summarize_paths)?;
    if let Some(summary) = summary {
        progress.println_normal(|| {
            format!(
                "Would compress {} [{}]: {} files, {}",
                spec.path.display(),
                spec.source_summary(),
                summary.files,
                HumanBytes(summary.bytes)
            )
        });
    }
    Ok(())
}

fn process_work_dir_spec(
    spec: &WorkDirSpec,
    compression: Kind,
    progress: &ProgressBars,
    compressor: &dyn Compressor,
) -> Result<()> {
    with_work_dir_inputs(spec, progress, |inputs| {
        compressor.compress_paths(inputs, compression, progress)
    })
    .with_context(|| format!("compression failed for {}", spec.path.display()))?;
    Ok(())
}

pub fn process_work_dir(
    dir: &Path,
    compression: Kind,
    progress: &ProgressBars,
    compressor: &dyn Compressor,
) -> Result<()> {
    let mut sources = BTreeSet::new();
    sources.insert("cargo".to_owned());
    let spec = WorkDirSpec {
        path: dir.to_path_buf(),
        lock_policy: LockPolicy::CargoLock,
        sources,
    };
    process_work_dir_spec(&spec, compression, progress, compressor)
}

pub fn run(cli: Cli) -> Result<()> {
    run_with_compressor(cli, &ApplesauceCompressor)
}

pub fn run_with_compressor(cli: Cli, compressor: &dyn Compressor) -> Result<()> {
    let verbosity = cli.verbosity();
    let progress = ProgressBars::new(verbosity);
    let cwd = std::env::current_dir().context("failed to get current directory")?;
    let old_cwd = cwd.clone();
    let specs = resolve_work_dir_specs(&cli, &cwd)?;

    let restore_cwd = |progress: &ProgressBars| {
        if let Err(error) = std::env::set_current_dir(&old_cwd) {
            progress.println_normal(|| {
                format!("warning: failed to restore cwd {}: {error}", old_cwd.display())
            });
        }
    };

    let mut had_error = false;

    if cli.dry_run {
        progress.println_normal(|| "Dry-run mode: previewing directories only".to_owned());
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            let progress_ref = &progress;
            for spec in specs {
                handles.push(scope.spawn(move || {
                    let path = spec.path.clone();
                    let result = preview_work_dir(&spec, progress_ref);
                    (path, result)
                }));
            }

            for handle in handles {
                let (path, result) = handle.join().expect("worker thread panicked");
                if let Err(error) = result {
                    had_error = true;
                    progress.error(&path, &format!("{error:#}"));
                }
            }
        });
    } else {
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            let progress_ref = &progress;
            for spec in specs {
                handles.push(scope.spawn(move || {
                    let path = spec.path.clone();
                    let result = process_work_dir_spec(
                        &spec,
                        cli.compression.to_kind(),
                        progress_ref,
                        compressor,
                    );
                    (path, result)
                }));
            }

            for handle in handles {
                let (path, result) = handle.join().expect("worker thread panicked");
                match result {
                    Ok(()) => progress.println_normal(|| format!("Compressed {}", path.display())),
                    Err(error) => {
                        had_error = true;
                        progress.error(&path, &format!("{error:#}"));
                    }
                }
            }
        });
    }
    progress.finish();
    restore_cwd(&progress);

    if had_error {
        Err(anyhow!("one or more directories failed"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    #[test]
    fn maps_builtin_profiles_to_dirs() {
        let overrides = HashMap::new();
        assert_eq!(resolve_profile_dir_name("dev", &overrides), "debug");
        assert_eq!(resolve_profile_dir_name("test", &overrides), "debug");
        assert_eq!(resolve_profile_dir_name("bench", &overrides), "release");
        assert_eq!(resolve_profile_dir_name("release", &overrides), "release");
        assert_eq!(resolve_profile_dir_name("custom", &overrides), "custom");
    }

    #[test]
    fn applies_profile_dir_name_override_from_config() {
        let temp = tempdir().unwrap();
        let cargo_dir = temp.path().join(".cargo");
        fs::create_dir(&cargo_dir).unwrap();
        fs::write(
            cargo_dir.join("config.toml"),
            "[profile.dev]\ndir-name = \"my-debug\"\nunknown = 1\n",
        )
        .unwrap();

        let overrides = load_profile_dir_name_overrides(temp.path()).unwrap();
        assert_eq!(overrides.get("dev"), Some(&"my-debug".to_owned()));
    }

    #[test]
    fn resolves_dirs_without_target() {
        let overrides = HashMap::new();
        let dirs = resolve_work_dirs(
            Path::new("/tmp/target"),
            &["dev".to_owned(), "release".to_owned()],
            &[],
            &overrides,
        );
        assert_eq!(
            dirs,
            vec![
                PathBuf::from("/tmp/target/debug"),
                PathBuf::from("/tmp/target/release")
            ]
        );
    }

    #[test]
    fn resolves_dirs_with_target() {
        let overrides = HashMap::new();
        let dirs = resolve_work_dirs(
            Path::new("/tmp/target"),
            &["dev".to_owned()],
            &[
                "aarch64-apple-darwin".to_owned(),
                "x86_64-apple-darwin".to_owned(),
            ],
            &overrides,
        );
        assert_eq!(
            dirs,
            vec![
                PathBuf::from("/tmp/target/aarch64-apple-darwin/debug"),
                PathBuf::from("/tmp/target/x86_64-apple-darwin/debug"),
            ]
        );
    }

    #[test]
    fn dedups_same_output_dir() {
        let overrides = HashMap::new();
        let dirs = resolve_work_dirs(
            Path::new("/tmp/target"),
            &["dev".to_owned(), "test".to_owned()],
            &[],
            &overrides,
        );
        assert_eq!(dirs, vec![PathBuf::from("/tmp/target/debug")]);
    }

    #[test]
    fn defaults_to_lzfse() {
        let cli = Cli::try_parse_from(["cargo-apfs-compress"]).unwrap();
        assert_eq!(cli.compression, CompressionArg::Lzfse);
        assert!(cli.profiles.is_empty());
        assert!(cli.caches.is_empty());
        assert!(cli.cache_dirs.is_empty());
        assert!(!cli.dry_run);
        assert_eq!(cli.verbose, 0);
        assert_eq!(cli.quiet, 0);
    }

    #[test]
    fn parses_verbose_flag() {
        let cli = Cli::try_parse_from(["cargo-apfs-compress", "-v"]).unwrap();
        assert_eq!(cli.verbose, 1);
        assert_eq!(cli.quiet, 0);
        assert_eq!(cli.verbosity(), Verbosity::Verbose);
    }

    #[test]
    fn parses_quiet_flag() {
        let cli = Cli::try_parse_from(["cargo-apfs-compress", "-q"]).unwrap();
        assert_eq!(cli.quiet, 1);
        assert_eq!(cli.verbose, 0);
        assert_eq!(cli.verbosity(), Verbosity::Quiet);
    }

    #[test]
    fn parses_cache_flags_and_dry_run() {
        let cli = Cli::try_parse_from([
            "cargo-apfs-compress",
            "--cache",
            "node-modules",
            "--cache",
            "go",
            "--cache-dir",
            "node_modules",
            "--dry-run",
        ])
        .unwrap();

        assert_eq!(cli.caches, vec![CacheArg::NodeModules, CacheArg::Go]);
        assert_eq!(cli.cache_dirs, vec![PathBuf::from("node_modules")]);
        assert!(cli.dry_run);
    }

    #[test]
    fn defaults_cache_selection_to_cargo() {
        let selected = selected_cache_sources(&[], &[]);
        assert_eq!(selected, [CacheArg::Cargo].into_iter().collect());
    }

    #[test]
    fn cache_dir_only_does_not_select_cargo() {
        let selected = selected_cache_sources(&[], &[PathBuf::from("/tmp/custom")]);
        assert!(selected.is_empty());
    }

    #[test]
    fn rejects_profile_or_target_without_cargo_cache() {
        let cli = Cli {
            profiles: vec!["dev".to_owned()],
            targets: vec![],
            compression: CompressionArg::Lzfse,
            verbose: 0,
            quiet: 0,
            caches: vec![CacheArg::NodeModules],
            cache_dirs: vec![],
            dry_run: false,
        };
        let selected = selected_cache_sources(&cli.caches, &cli.cache_dirs);
        let error = validate_cli(&cli, &selected).unwrap_err().to_string();
        assert!(error.contains("--profile/--target"));
    }

    #[test]
    fn parses_go_env_json_output() {
        let output = br#"{"GOCACHE":"/tmp/go-build","GOMODCACHE":"/tmp/go/pkg/mod"}"#;
        let mut dirs = parse_go_env_output(output).unwrap();
        dirs.sort();
        let mut expected = vec![
            PathBuf::from("/tmp/go-build"),
            PathBuf::from("/tmp/go/pkg/mod"),
        ];
        expected.sort();
        assert_eq!(dirs, expected);
    }

    #[test]
    fn resolves_non_cargo_specs_without_metadata() {
        let root = tempdir().unwrap();
        let cli = Cli {
            profiles: vec![],
            targets: vec![],
            compression: CompressionArg::Lzfse,
            verbose: 0,
            quiet: 0,
            caches: vec![CacheArg::NodeModules],
            cache_dirs: vec![
                PathBuf::from("node_modules"),
                PathBuf::from("/tmp/custom-cache"),
            ],
            dry_run: true,
        };

        let specs = resolve_work_dir_specs(&cli, root.path()).unwrap();
        let paths: Vec<PathBuf> = specs.iter().map(|spec| spec.path.clone()).collect();
        assert!(paths.contains(&root.path().join("node_modules")));
        assert!(paths.contains(&PathBuf::from("/tmp/custom-cache")));
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn discovers_default_target_roots() {
        let root = tempdir().unwrap();
        let target = root.path().join("target");
        fs::create_dir_all(target.join("debug")).unwrap();
        fs::create_dir_all(target.join("release")).unwrap();
        fs::create_dir_all(target.join("x86_64-apple-darwin").join("debug")).unwrap();
        fs::create_dir_all(target.join("x86_64-apple-darwin").join("release")).unwrap();
        fs::create_dir_all(target.join("doc")).unwrap();
        fs::create_dir_all(target.join("package")).unwrap();
        fs::create_dir_all(target.join("tmp")).unwrap();

        let dirs = discover_default_work_dirs(&target, &[]).unwrap();

        assert!(dirs.contains(&target.join("debug")));
        assert!(dirs.contains(&target.join("release")));
        assert!(dirs.contains(&target.join("x86_64-apple-darwin").join("debug")));
        assert!(dirs.contains(&target.join("x86_64-apple-darwin").join("release")));
        assert!(dirs.contains(&target.join("doc")));
        assert!(dirs.contains(&target.join("package")));
        assert!(!dirs.contains(&target.join("tmp")));
    }

    #[test]
    fn discovers_only_requested_targets_when_filtered() {
        let root = tempdir().unwrap();
        let target = root.path().join("target");
        fs::create_dir_all(target.join("x86_64-apple-darwin").join("debug")).unwrap();
        fs::create_dir_all(target.join("aarch64-apple-darwin").join("debug")).unwrap();

        let dirs =
            discover_default_work_dirs(&target, &["x86_64-apple-darwin".to_owned()]).unwrap();

        assert_eq!(dirs, vec![target.join("x86_64-apple-darwin").join("debug")]);
    }

    #[derive(Default)]
    struct RecordingCompressor {
        calls: Mutex<Vec<Vec<PathBuf>>>,
        delay: Duration,
        fail_on: Option<String>,
        starts: Mutex<Vec<Instant>>,
        ends: Mutex<Vec<Instant>>,
    }

    impl Compressor for RecordingCompressor {
        fn compress_paths(
            &self,
            paths: &[PathBuf],
            _compression: Kind,
            _progress: &ProgressBars,
        ) -> Result<()> {
            self.starts.lock().unwrap().push(Instant::now());
            self.calls.lock().unwrap().push(paths.to_vec());
            if self.delay > Duration::ZERO {
                thread::sleep(self.delay);
            }
            self.ends.lock().unwrap().push(Instant::now());

            if let Some(fail_on) = &self.fail_on {
                if paths
                    .iter()
                    .any(|path| path.to_string_lossy().contains(fail_on))
                {
                    return Err(anyhow!("intentional failure"));
                }
            }
            Ok(())
        }
    }

    #[test]
    fn excludes_cargo_lock_from_inputs() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("artifact.bin"), b"abc").unwrap();
        fs::write(temp.path().join(CARGO_LOCK_NAME), b"").unwrap();

        let compressor = RecordingCompressor::default();
        let progress = ProgressBars::new(Verbosity::Normal);
        process_work_dir(temp.path(), Kind::Lzfse, &progress, &compressor).unwrap();

        let calls = compressor.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].iter().any(|p| p.ends_with("artifact.bin")));
        assert!(!calls[0].iter().any(|p| p.ends_with(CARGO_LOCK_NAME)));
    }

    #[test]
    fn lock_contention_blocks_second_worker() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("artifact.bin"), b"abc").unwrap();

        let compressor = Arc::new(RecordingCompressor {
            delay: Duration::from_millis(200),
            ..RecordingCompressor::default()
        });

        let d1 = temp.path().to_path_buf();
        let d2 = temp.path().to_path_buf();
        let c1 = Arc::clone(&compressor);
        let c2 = Arc::clone(&compressor);
        let t1 = thread::spawn(move || {
            let progress = ProgressBars::new(Verbosity::Normal);
            process_work_dir(&d1, Kind::Lzfse, &progress, &*c1)
        });
        thread::sleep(Duration::from_millis(20));
        let t2 = thread::spawn(move || {
            let progress = ProgressBars::new(Verbosity::Normal);
            process_work_dir(&d2, Kind::Lzfse, &progress, &*c2)
        });
        t1.join().unwrap().unwrap();
        t2.join().unwrap().unwrap();

        let starts = compressor.starts.lock().unwrap();
        let ends = compressor.ends.lock().unwrap();
        assert_eq!(starts.len(), 2);
        assert_eq!(ends.len(), 2);
        assert!(starts[1] >= ends[0]);
    }

    #[test]
    fn parallelizes_distinct_dirs() {
        let root = tempdir().unwrap();
        let d1 = root.path().join("one");
        let d2 = root.path().join("two");
        fs::create_dir_all(&d1).unwrap();
        fs::create_dir_all(&d2).unwrap();
        fs::write(d1.join("a.bin"), b"a").unwrap();
        fs::write(d2.join("b.bin"), b"b").unwrap();

        let compressor = Arc::new(RecordingCompressor {
            delay: Duration::from_millis(200),
            ..RecordingCompressor::default()
        });

        let c1 = Arc::clone(&compressor);
        let c2 = Arc::clone(&compressor);
        let d1c = d1.clone();
        let d2c = d2.clone();

        let t1 = thread::spawn(move || {
            let progress = ProgressBars::new(Verbosity::Normal);
            process_work_dir(&d1c, Kind::Lzfse, &progress, &*c1)
        });
        let t2 = thread::spawn(move || {
            let progress = ProgressBars::new(Verbosity::Normal);
            process_work_dir(&d2c, Kind::Lzfse, &progress, &*c2)
        });
        t1.join().unwrap().unwrap();
        t2.join().unwrap().unwrap();

        let starts = compressor.starts.lock().unwrap();
        assert_eq!(starts.len(), 2);
        let delta = if starts[0] > starts[1] {
            starts[0] - starts[1]
        } else {
            starts[1] - starts[0]
        };
        assert!(delta < Duration::from_millis(150));
    }

    #[test]
    fn returns_error_if_any_worker_fails() {
        let root = tempdir().unwrap();
        let target = root.path().join("target").join("debug");
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("will-fail.bin"), b"f").unwrap();

        let old = std::env::current_dir().unwrap();
        std::env::set_current_dir(root.path()).unwrap();

        let cli = Cli {
            profiles: vec!["dev".to_owned()],
            targets: vec![],
            compression: CompressionArg::Lzfse,
            verbose: 0,
            quiet: 0,
            caches: vec![],
            cache_dirs: vec![],
            dry_run: false,
        };

        let compressor = RecordingCompressor {
            fail_on: Some("will-fail".to_owned()),
            ..RecordingCompressor::default()
        };

    let result = run_with_compressor(cli, &compressor);
    if std::env::set_current_dir(old).is_err() {
        return;
    }
    assert!(result.is_err());
}

    #[test]
    fn dry_run_does_not_invoke_compressor() {
        let root = tempdir().unwrap();
        let node_modules = root.path().join("node_modules");
        fs::create_dir_all(&node_modules).unwrap();
        fs::write(node_modules.join("pkg.json"), br#"{"name":"pkg"}"#).unwrap();

        let old = std::env::current_dir().unwrap();
        std::env::set_current_dir(root.path()).unwrap();

        let cli = Cli {
            profiles: vec![],
            targets: vec![],
            compression: CompressionArg::Lzfse,
            verbose: 0,
            quiet: 0,
            caches: vec![CacheArg::NodeModules],
            cache_dirs: vec![],
            dry_run: true,
        };

        let compressor = RecordingCompressor::default();
    let result = run_with_compressor(cli, &compressor);

    if std::env::set_current_dir(old).is_err() {
        return;
    }
    assert!(result.is_ok());
        assert!(compressor.calls.lock().unwrap().is_empty());
    }
}
