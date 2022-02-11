pub mod archive_manager;
pub mod wc_config;

use archive_manager::ArchiveManager;
use lazy_static::lazy_static;
use std::{collections::HashMap, fs, sync::Arc};
use walkdir::WalkDir;
use wc_config::WCConfig;
use std::thread;

lazy_static! {
    pub static ref ARCH_EXT: Vec<&'static str> = vec!["zip", "tar", "gz", "tar.gz", "7z"];
}
const MAX_FILESIZE: u64 = 10_000_000;

#[derive(Clone)]
enum MyFile {
    Archive(String, Vec<u8>),
    Regular(String, Vec<u8>),
    Poisoned,
}

// enum WordCounter {
//     Counter(HashMap<String, usize>),
//     Poisoned,
// }

// type IndexQueue = lockfree::queue::Queue<String>;
type IndexQueue = crossbeam::queue::SegQueue<MyFile>;
// type IndexQueue = crossbeam::queue::ArrayQueue<MyFile>;
type MergeQueue = crossbeam::queue::SegQueue<HashMap<String, usize>>;

fn parse_files_recur(path: &String, queue: &IndexQueue) {
    for entry in WalkDir::new(path) {
        let entry = entry.unwrap();
        let entry = entry.path();

        if !entry.is_file() || entry.extension().is_none() {
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

fn count_words_in_file(counter: &mut HashMap<String, usize>, content: Vec<u8>) {
    let str_content: String;
    match String::from_utf8(content) {
        Ok(s) => str_content = s,
        Err(_) => return
    }
    // let str_content = unsafe {String::from_utf8_unchecked(content)};
    for word in str_content.split_whitespace() {
        match counter.get_mut(word) {
            Some(counter) => *counter += 1,
            None => {
                counter.insert(String::from(word), 1);
            }
        }
    }
}

fn one_thread_count(index_queue: &IndexQueue, merge_queue: &MergeQueue) {
    let mut word_counter: HashMap<String, usize> = HashMap::new();

    let mut archive_manager = ArchiveManager::new();
    loop {
        let file = match index_queue.pop() {
            Some(file) => file,
            None => continue,
        };
        match file {
            MyFile::Archive(_, mut content) => {
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
                            match archive_manager.get_next() {
                                Ok(content) => count_words_in_file(&mut word_counter, content),
                                Err(err) => eprintln!("ArchiveManager: error in get_next: {}", err),
                            }
                        }
                        Err(err) => {
                            eprintln!("ArchiveManager: error in prepare_next: {}", err);
                            continue;
                        }
                    }
                }
            }
            MyFile::Regular(_, content) => {
                // println!("Regular file: {}", filename);
                count_words_in_file(&mut word_counter, content);
            }
            MyFile::Poisoned => {
                index_queue.push(MyFile::Poisoned);
                merge_queue.push(word_counter);
                return;
            }
        }
    }
}

fn merge_counters(merge_queue: &MergeQueue, n_threads: u32) -> HashMap<String, usize> {
    let mut result: HashMap<String, usize> = HashMap::new();
    let mut processed_counters = 0;
    while processed_counters != n_threads {
        let counter = match merge_queue.pop() {
            Some(counter) => counter,
            None => continue,
        };
        processed_counters += 1;
        
        for (word, n) in counter {
            let count = result.entry(word).or_insert(0);
            *count += n;
        }
    }
    result
}

pub fn count_words(config: &WCConfig) -> HashMap<String, usize> {
    let index_queue = Arc::new(IndexQueue::new());
    let merge_queue = Arc::new(MergeQueue::new());

    let reader_queue = Arc::clone(&index_queue);
    let indir = config.indir.clone();
    let reader_handle = thread::spawn(move || {
        parse_files_recur(&indir, &reader_queue);
    });


    let mut index_handlers = Vec::new();
    for _ in 0..config.index_threads {    
        let cur_index_queue = Arc::clone(&index_queue);
        let cur_merge_queue = Arc::clone(&merge_queue);
        let index_handler = thread::spawn(move || {
            one_thread_count(&cur_index_queue, &cur_merge_queue);
        });
        index_handlers.push(index_handler);
    }

    let counted_words = merge_counters(&merge_queue, config.index_threads);
    reader_handle.join().unwrap();
    while let Some(handler) = index_handlers.pop() {
        handler.join().unwrap();
    }
    counted_words
}
