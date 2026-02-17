use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, Receiver, RecvTimeoutError},
        Arc,
    },
    thread,
    time::Duration,
};

use notify::Watcher as _;
use rustc_hash::FxHashMap;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub struct Watcher {
    path: PathBuf,
    size: usize,
    stop: Arc<AtomicBool>,
}

impl Watcher {
    pub fn new(path: PathBuf, size: usize) -> Self {
        Self {
            path,
            size,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn stop_signal(mut self, value: Arc<AtomicBool>) -> Self {
        self.stop = value;
        self
    }

    pub fn watch(&self) -> Result<Receiver<Event>, Error> {
        tracing::debug!("Start watching {}", self.path.display());
        let (itx, irx) = channel();
        let (etx, erx) = channel();

        let base = self.path.clone();
        let base = std::fs::canonicalize(&base)
            .map_err(|e| Error::Unexpected(format!("Canonicalize path {}: {e}", base.display())))?;
        let chunk_size = self.size;
        let mut watcher = notify::recommended_watcher(itx)?;
        watcher.watch(&base, notify::RecursiveMode::Recursive)?;

        let path = self.path.clone();
        let stop = self.stop.clone();
        thread::spawn(move || {
            let mut cursors = FxHashMap::default();
            let _watcher = watcher; // Move watcher to live in this scope

            loop {
                match irx.recv_timeout(Duration::from_millis(100)) {
                    Ok(event) => {
                        tracing::debug!("Watch on {event:?}");
                        match event {
                            Ok(event) => {
                                if let Err(error) =
                                    process(&etx, &base, chunk_size, &mut cursors, event)
                                {
                                    tracing::debug!(
                                        "Error while process watching {}: {}",
                                        path.display(),
                                        error
                                    );
                                };
                            }
                            Err(error) => {
                                tracing::debug!(
                                    "Error while watching {}: {}",
                                    path.display(),
                                    error
                                );
                            }
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        Ok(erx)
    }
}

fn process(
    etx: &std::sync::mpsc::Sender<Event>,
    base: &PathBuf,
    chunk_size: usize,
    cursors: &mut std::collections::HashMap<PathBuf, u64, rustc_hash::FxBuildHasher>,
    event: notify::Event,
) -> Result<(), Box<dyn std::error::Error>> {
    match event.kind {
        notify::EventKind::Modify(_) => {
            for path in event.paths {
                if !path.is_file() {
                    continue;
                }

                let mut file = File::open(&path)?;
                let last_pos = *cursors.entry(path.clone()).or_insert(0);
                let new_pos = file.seek(SeekFrom::End(0))?;

                if new_pos > last_pos {
                    let mut buffer = vec![0; (new_pos - last_pos) as usize];
                    file.seek(SeekFrom::Start(last_pos))?;
                    file.read_exact(&mut buffer)?;

                    let buffer = buffer.to_vec();
                    let relative_path = path
                        .strip_prefix(base)
                        .map_err(|e| {
                            format!(
                                "Strip prefix '{}' from '{}': {}",
                                base.display(),
                                path.display(),
                                e
                            )
                        })?
                        .to_path_buf();
                    for chunk in buffer.chunks(chunk_size) {
                        let bytes = chunk.to_vec();
                        let event = Event::Append(relative_path.clone(), bytes);
                        if etx.send(event).is_err() {
                            break;
                        }
                    }
                    cursors.insert(path, new_pos);
                }
            }
        }
        notify::EventKind::Create(_)
        | notify::EventKind::Remove(_)
        | notify::EventKind::Access(_)
        | notify::EventKind::Any
        | notify::EventKind::Other => {}
    };

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Event {
    Append(PathBuf, Vec<u8>),
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Watch error: {0}")]
    Watch(#[from] notify::Error),
    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Write, time::Duration};

    use super::*;

    #[test]
    fn test_new_file() {
        // Given
        let t = std::time::Duration::from_millis(1000);
        let c = "toto".as_bytes();
        let file_path = std::path::PathBuf::from("toto.txt");
        let tmp = tempdir::TempDir::new("rcsync").unwrap();
        let tmp = tmp.path().to_path_buf();
        let watcher = Watcher::new(tmp.clone(), 4096).watch().unwrap();

        // When
        thread::sleep(Duration::from_millis(100));
        std::fs::write(tmp.join(&file_path), c).unwrap();

        // Then
        let event = watcher.recv_timeout(t).unwrap();
        assert_eq!(event, Event::Append(file_path.clone(), c.to_vec()));
    }

    #[test]
    fn test_ignore_new_folder() {
        // Given
        let t = std::time::Duration::from_millis(100);
        let folder_path = std::path::PathBuf::from("toto");
        let tmp = tempdir::TempDir::new("rcsync").unwrap();
        let tmp = tmp.path().to_path_buf();
        let watcher = Watcher::new(tmp.clone(), 4096).watch().unwrap();

        // When
        thread::sleep(Duration::from_millis(100));
        std::fs::create_dir(tmp.join(&folder_path)).unwrap();

        // Then
        let event = watcher.recv_timeout(t);
        assert_eq!(event, Err(std::sync::mpsc::RecvTimeoutError::Timeout));
    }

    #[test]
    fn test_append_file() {
        // Given
        let t = std::time::Duration::from_millis(1000);
        let c = "toto".as_bytes();
        let file_path = std::path::PathBuf::from("toto.txt");
        let tmp = tempdir::TempDir::new("rcsync").unwrap();
        let tmp = tmp.path().to_path_buf();
        // NOTE: 1 octet by 1 octet
        let watcher = Watcher::new(tmp.clone(), 1).watch().unwrap();

        // When
        thread::sleep(Duration::from_millis(100));
        std::fs::write(tmp.join(&file_path), c).unwrap();

        // Then
        let event = watcher.recv_timeout(t).unwrap();
        assert_eq!(event, Event::Append(file_path.clone(), vec!['t' as u8]));
        let event = watcher.recv_timeout(t).unwrap();
        assert_eq!(event, Event::Append(file_path.clone(), vec!['o' as u8]));
        let event = watcher.recv_timeout(t).unwrap();
        assert_eq!(event, Event::Append(file_path.clone(), vec!['t' as u8]));
        let event = watcher.recv_timeout(t).unwrap();
        assert_eq!(event, Event::Append(file_path.clone(), vec!['o' as u8]));
    }

    #[test]
    fn test_multiple_append_file() {
        // Given
        let t = std::time::Duration::from_millis(1000);
        let c = "toto".as_bytes();
        let c2 = "zaza".as_bytes();
        let file_path = std::path::PathBuf::from("toto.txt");
        let tmp = tempdir::TempDir::new("rcsync").unwrap();
        let tmp = tmp.path().to_path_buf();
        // NOTE: 1 octet by 1 octet
        let watcher = Watcher::new(tmp.clone(), 1024).watch().unwrap();

        // When
        thread::sleep(Duration::from_millis(100));
        std::fs::write(tmp.join(&file_path), c).unwrap();
        thread::sleep(Duration::from_millis(100));
        OpenOptions::new()
            .create(false)
            .append(true)
            .open(tmp.join(&file_path))
            .unwrap()
            .write_all(c2)
            .unwrap();

        // Then
        let event = watcher.recv_timeout(t).unwrap();
        assert_eq!(event, Event::Append(file_path.clone(), c.to_vec()));
        let event = watcher.recv_timeout(t).unwrap();
        assert_eq!(event, Event::Append(file_path.clone(), c2.to_vec()));
    }
}
