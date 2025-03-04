use sha1::{Digest, Sha1};
use std::{
    ffi::{OsStr, OsString},
    io,
    path::{Path, PathBuf},
};

use crate::{gel::context_trace, FileAccess};

use super::{BuildContext, InstanceName};

/// The ordered list of project filenames supported.
pub const PROJECT_FILES: &[&str] = &["gel.toml", "edgedb.toml"];

#[derive(Debug)]
pub struct ProjectSearchResult {
    #[allow(unused)]
    pub project_path: PathBuf,
    pub project: Option<Project>,
}

pub enum ProjectDir {
    SearchCwd,
    Search(PathBuf),
    NoSearch(PathBuf),
}

impl ProjectDir {
    pub fn search_parents(&self) -> bool {
        match self {
            ProjectDir::Search(_) => true,
            ProjectDir::NoSearch(_) => false,
            ProjectDir::SearchCwd => true,
        }
    }
}

/// Searches for a project file either from the current directory or from a
pub fn find_project_file(
    context: &mut impl BuildContext,
    start_path: ProjectDir,
) -> io::Result<Option<ProjectSearchResult>> {
    let search_parents = start_path.search_parents();
    let dir = match start_path {
        ProjectDir::SearchCwd => {
            let Some(cwd) = context.cwd() else {
                context_trace!(context, "No current directory, skipping project search");
                return Ok(None);
            };
            cwd.to_path_buf()
        }
        ProjectDir::Search(path) => path,
        ProjectDir::NoSearch(path) => path,
    };
    let Some(project_path) = search_directory(context, &dir, search_parents)? else {
        context_trace!(context, "No project file found");
        return Ok(None);
    };
    context_trace!(context, "Project path: {:?}", project_path);
    let stash = get_stash_path(context, project_path.parent().unwrap_or(&project_path))?;
    context_trace!(context, "Stash path: {:?}", stash);
    let project = stash.and_then(|path| Project::load(&path, context));
    context_trace!(context, "Project: {:?}", project);
    Ok(Some(ProjectSearchResult {
        project_path,
        project,
    }))
}

/// Computes the SHA-1 hash of a path's canonical representation.
fn hash_path(path: &Path) -> String {
    let mut hasher = Sha1::new();
    hasher.update(path.as_os_str().as_encoded_bytes());
    format!("{:x}", hasher.finalize())
}

/// Generates a stash name for a project directory.
fn stash_name(path: &Path) -> OsString {
    let hash = hash_path(path);
    let base = path.file_name().unwrap_or(OsStr::new(""));
    let mut name = base.to_os_string();
    name.push("-");
    name.push(hash);
    name
}

/// Searches for project files in the given directory and optionally its parents.
fn search_directory(
    context: &mut impl BuildContext,
    base: &Path,
    search_parents: bool,
) -> io::Result<Option<PathBuf>> {
    let mut path = base.to_path_buf();
    loop {
        let mut found = Vec::new();
        for name in PROJECT_FILES {
            let file = path.join(name);
            if context.files().exists(&file)? {
                context_trace!(context, "Found project file: {:?}", file);
                found.push(file);
            }
        }

        if found.len() > 1 {
            let (first, rest) = found.split_at(1);
            let first_content = context.files().read(&first[0])?;
            for file in rest {
                if context.files().read(file)? != first_content {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{:?} and {:?} found in {:?} but the contents are different",
                            first[0].file_name(),
                            file.file_name(),
                            path
                        ),
                    ));
                }
            }
            return Ok(Some(first[0].clone()));
        } else if let Some(file) = found.pop() {
            return Ok(Some(file));
        }

        if !search_parents {
            break;
        }
        if let Some(parent) = path.parent() {
            path = parent.to_path_buf();
        } else {
            break;
        }
    }
    Ok(None)
}

/// Computes the path to the project's stash file based on the canonical path.
fn get_stash_path(
    context: &mut impl BuildContext,
    project_dir: &Path,
) -> io::Result<Option<PathBuf>> {
    let canonical = context
        .files()
        .canonicalize(project_dir)
        .unwrap_or(project_dir.to_path_buf());
    let stash_name = stash_name(&canonical);
    let path = Path::new("projects").join(stash_name);
    Ok(Some(path))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Project {
    pub cloud_profile: Option<String>,
    pub instance_name: InstanceName,
    pub project_path: Option<PathBuf>,
    pub branch: Option<String>,
    pub database: Option<String>,
}

impl Project {
    #[cfg(test)]
    pub fn new(instance_name: InstanceName) -> Self {
        Self {
            cloud_profile: None,
            instance_name,
            project_path: None,
            branch: None,
            database: None,
        }
    }

    pub fn load(path: &Path, context: &mut impl BuildContext) -> Option<Self> {
        let cloud_profile = context
            .read_config_file::<String>(&path.join("cloud-profile"))
            .unwrap_or_default();
        let instance_name = context
            .read_config_file::<InstanceName>(&path.join("instance-name"))
            .unwrap_or_default();
        let project_path = context
            .read_config_file::<PathBuf>(&path.join("project-path"))
            .unwrap_or_default();
        let branch = context
            .read_config_file::<String>(&path.join("branch"))
            .unwrap_or_default();
        let database = context
            .read_config_file::<String>(&path.join("database"))
            .unwrap_or_default();
        let instance_name = instance_name?;
        Some(Self {
            cloud_profile,
            instance_name,
            project_path,
            branch,
            database,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{file::SystemFileAccess, gel::BuildContextImpl};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use super::*;

    #[test]
    fn test_stash_examples() {
        let files = HashMap::from_iter([
            (Path::new("/home/edgedb/test/gel.toml"),
            ""),
            (Path::new("/home/edgedb/.config/edgedb/projects/test-cf3c86df8fc33fbb73a47671ac5762eda8219158/instance-name"),
            "instance-name"),
        ]);

        let traces = Arc::new(Mutex::new(Vec::new()));
        let traces_clone = traces.clone();

        let mut context = BuildContextImpl::new_with((), files);
        context.tracing = Some(Box::new(move |s| {
            traces_clone.lock().unwrap().push(s.to_string())
        }));
        context.config_dir = Some(vec![PathBuf::from("/home/edgedb/.config/edgedb")]);
        let res = find_project_file(
            &mut context,
            ProjectDir::Search(PathBuf::from("/home/edgedb/test")),
        );

        for trace in traces.lock().unwrap().iter() {
            eprintln!("{}", trace);
        }
        let res = res.unwrap().unwrap();
        assert_eq!(
            res.project_path,
            PathBuf::from("/home/edgedb/test/gel.toml")
        );
        assert_eq!(
            res.project,
            Some(Project::new(InstanceName::Local(
                "instance-name".to_string()
            )))
        );
    }

    #[test]
    fn test_project_file_priority() {
        use std::fs;

        let temp = tempfile::tempdir().unwrap();
        let base = temp.path();

        let gel_path = base.join("gel.toml");
        let edgedb_path = base.join("edgedb.toml");

        let mut context = BuildContextImpl::new_with((), SystemFileAccess);

        // Test gel.toml only
        fs::write(&gel_path, "test1").unwrap();
        let found = find_project_file(&mut context, ProjectDir::Search(base.to_path_buf()))
            .unwrap()
            .unwrap();
        assert_eq!(found.project_path, gel_path);

        // Test edgedb.toml only
        fs::remove_file(&gel_path).unwrap();
        fs::write(&edgedb_path, "test2").unwrap();
        let found = find_project_file(&mut context, ProjectDir::Search(base.to_path_buf()))
            .unwrap()
            .unwrap();
        assert_eq!(found.project_path, edgedb_path);

        // Test both files with same content
        fs::write(&gel_path, "test3").unwrap();
        fs::write(&edgedb_path, "test3").unwrap();
        let found = find_project_file(&mut context, ProjectDir::Search(base.to_path_buf()))
            .unwrap()
            .unwrap();
        assert_eq!(found.project_path, gel_path);

        // Test both files with different content
        fs::write(&gel_path, "test4").unwrap();
        fs::write(&edgedb_path, "test5").unwrap();
        let err =
            find_project_file(&mut context, ProjectDir::Search(base.to_path_buf())).unwrap_err();
        assert!(err.to_string().contains("but the contents are different"));
    }
}
