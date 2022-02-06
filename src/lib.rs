pub mod archive_manager;
pub mod wc_config;

use archive_manager::ArchiveManager;
use lazy_static::lazy_static;
use std::fs;
use walkdir::WalkDir;
use wc_config::WCConfig;

lazy_static! {
    pub static ref ARCH_EXT: Vec<&'static str> = vec![".zip", ".tar", ".gz", ".tar.gz", "7z"];
}
const MAX_FILESIZE: u64 = 10_000_000;

#[derive(Clone)]
enum MyFile {
    Archive(String, Vec<u8>),
    Regular(String, Vec<u8>),
    Poisoned,
}

// type IndexQueue = lockfree::queue::Queue<String>;
type IndexQueue = crossbeam::queue::SegQueue<MyFile>;

fn parse_files_recur(path: &String, queue: &mut IndexQueue) {
    for entry in WalkDir::new(path) {
        let entry = entry.unwrap();
        let entry = entry.path();
        if !entry.is_file() || entry.extension().is_none() {
            println!("file: is_dir: {} {:?}", entry.is_dir(), entry.display());
            if entry.extension().is_some() {
                println!(
                    "filename: {}.{}",
                    entry.file_name().unwrap().to_str().unwrap(),
                    entry.extension().unwrap().to_str().unwrap()
                );
            }
            continue;
        }
        let ext = entry.extension().unwrap().to_str().unwrap_or_default();
        if ext == ".txt" {
            if fs::metadata(entry).unwrap().len() > MAX_FILESIZE {
                continue;
            }
            match fs::read(entry) {
                Ok(content) => queue.push(MyFile::Regular(entry.display().to_string(), content)),
                Err(err) => eprintln!("Error while reading file: {:?}", err),
            };
        } else if ARCH_EXT.contains(&ext) {
            match fs::read(entry) {
                Ok(content) => queue.push(MyFile::Archive(entry.display().to_string(), content)),
                Err(err) => eprintln!("Error while reading archive: {:?}", err),
            };
        }
    }
    queue.push(MyFile::Poisoned);
}

// fn count_words_in_file(counter: &mut HashMap<String, usize>, content: &Vec<u8>) {
//     todo!()
// }

fn one_thread_count(queue: &mut IndexQueue) {
    let mut archive_manager = ArchiveManager::new();
    loop {
        let file = match queue.pop() {
            Some(file) => file,
            None => continue,
        };
        // println!("Filename: {}", file);
        match file {
            MyFile::Archive(filename, mut content) => {
                println!("Archive file: {}", filename);
                if let Err(_) = archive_manager.set_archive(&mut content) {
                    eprintln!("ArchiveManager: error in new");
                    continue;
                }

                loop {
                    match archive_manager.prepare_next() {
                        Ok(status) => {
                            if !status {
                                break;
                            }
                            // count words
                        }
                        Err(err) => {
                            eprintln!("ArchiveManager: error in prepare_next: {}", err);
                            continue;
                        }
                    }
                }
            }
            MyFile::Regular(filename, _) => {
                println!("Archive file: {}", filename)
                // count words
            }
            MyFile::Poisoned => {
                queue.push(MyFile::Poisoned);
                return;
            }
        }
    }
}

pub fn count_words(config: &WCConfig) {
    let mut read_queue = IndexQueue::new();
    parse_files_recur(&config.indir, &mut read_queue);
    println!("Size of queue: {}", read_queue.len());
    one_thread_count(&mut read_queue);
}
