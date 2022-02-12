pub mod archive_manager;
pub mod wc_config;
pub mod casefold;

use archive_manager::ArchiveManager;
use lazy_static::lazy_static;
use std::sync::mpsc::{self, Receiver};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    sync::{mpsc::Sender, Arc, Mutex},
    thread, time,
};
use walkdir::WalkDir;
use wc_config::WCConfig;
use casefold::default_case_fold_str;
// use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;


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

enum WordCounter {
    Counter(HashMap<String, usize>),
    Poisoned,
}

pub struct CountResult {
    pub reading_time_ms: u128,
    pub indexing_time_ms: u128,
    pub total_time: u128,
    pub counted_words: HashMap<String, usize>,
}

type IndexQueue = crossbeam::queue::SegQueue<MyFile>;
// type IndexQueue = crossbeam::queue::ArrayQueue<MyFile>;
// type MergeQueue = crossbeam::queue::SegQueue<HashMap<String, usize>>;

fn read_files_recur(path: &String, queue: &IndexQueue, read_time_mut: Arc<Mutex<u128>>) {
    let start_time = time::Instant::now();
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
    let mut read_time = read_time_mut.lock().unwrap();
    *read_time += start_time.elapsed().as_millis();
}

fn _count_words_in_file(content: Vec<u8>) -> HashMap<String, usize> {
    let mut counter: HashMap<String, usize> = HashMap::new();
    let str_content: String;
    match String::from_utf8(content) {
        Ok(s) => str_content = s,
        Err(_) => return counter,
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
    counter
}

fn count_words_in_file_unicode(content: Vec<u8>) -> HashMap<String, usize> {
    let mut counter: HashMap<String, usize> = HashMap::new();
    let str_content: String;
    match String::from_utf8(content) {
        Ok(s) => str_content = s,
        Err(err) => {
            eprintln!("Unicode error: {}", err);
            return counter;
        },
    }
    // normalizing - no need since default_case_fold_str normalizes
    // let str_content = str_content.nfc().collect::<String>();

    // case fold
    let str_content = default_case_fold_str(&str_content);

    // segment by words
    for word in str_content.unicode_words() {
        match counter.get_mut(word) {
            Some(counter) => *counter += 1,
            None => {
                counter.insert(String::from(word), 1);
            }
        }
    }

    counter
}

fn one_thread_count(
    index_queue: &IndexQueue,
    merge_channel: Sender<WordCounter>,
    index_time_mut: Arc<Mutex<u128>>,
) {
    let start_time = time::Instant::now();
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
                                Ok(content) => merge_channel
                                    .send(WordCounter::Counter(count_words_in_file_unicode(content)))
                                    .unwrap(),
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
            MyFile::Regular(_, content) => merge_channel
                .send(WordCounter::Counter(count_words_in_file_unicode(content)))
                .unwrap(),
            MyFile::Poisoned => {
                index_queue.push(MyFile::Poisoned);
                merge_channel.send(WordCounter::Poisoned).unwrap();
                break;
            }
        }
    }
    let mut index_time = index_time_mut.lock().unwrap();
    *index_time += start_time.elapsed().as_millis();
}

fn merge_counters(rx_merge: Receiver<WordCounter>, n_threads: u32) -> HashMap<String, usize> {
    let mut result: HashMap<String, usize> = HashMap::new();
    let mut processed_counters = 0;

    while processed_counters != n_threads {
        match rx_merge.recv().unwrap() {
            WordCounter::Counter(w_counter) => {
                for (word, n) in w_counter {
                    let count = result.entry(word).or_insert(0);
                    *count += n;
                }
            }
            WordCounter::Poisoned => processed_counters += 1,
        }
    }
    result
}

pub fn count_words(config: &WCConfig) -> CountResult {
    let index_queue = Arc::new(IndexQueue::new());
    // let merge_queue = Arc::new(MergeQueue::new());
    let (tx_merge, rx_merge) = mpsc::channel();

    let reading_time_ms: Arc<Mutex<u128>> = Arc::new(Mutex::new(0));
    let indexing_time_ms: Arc<Mutex<u128>> = Arc::new(Mutex::new(0));

    let start_time = time::Instant::now();

    let reader_queue = Arc::clone(&index_queue);
    let indir = config.indir.clone();
    let cloned_reading_time_ms = Arc::clone(&reading_time_ms);
    let reader_handle = thread::spawn(move || {
        read_files_recur(&indir, &reader_queue, cloned_reading_time_ms);
    });

    let mut index_handlers = Vec::new();
    for _ in 0..config.index_threads {
        let cur_index_queue = Arc::clone(&index_queue);
        let tx_merge_clone = tx_merge.clone();
        let cloned_indexing_time_ms = Arc::clone(&indexing_time_ms);
        let index_handler = thread::spawn(move || {
            one_thread_count(&cur_index_queue, tx_merge_clone, cloned_indexing_time_ms);
        });
        index_handlers.push(index_handler);
    }

    let counted_words = merge_counters(rx_merge, config.index_threads);
    reader_handle.join().unwrap();
    while let Some(handler) = index_handlers.pop() {
        handler.join().unwrap();
    }

    let total_time = start_time.elapsed().as_millis();
    let reading_time_ms = *reading_time_ms.lock().unwrap();
    let indexing_time_ms = (*indexing_time_ms.lock().unwrap() as f64 / config.index_threads as f64) as u128;

    CountResult {
        reading_time_ms,
        indexing_time_ms,
        total_time,
        counted_words,
    }
}

pub fn dump_res(config: &WCConfig, count_result: CountResult) {
    // let mut counted_words = Vec::from_iter(count_result.counted_words.iter());
    let mut counted_words: Vec<(String, usize)> = count_result.counted_words.into_iter().collect();
    let mut by_n_file = File::create(&config.out_by_n).unwrap();

    counted_words.sort_by(|a, b| b.1.cmp(&a.1));
    for (word, n) in &counted_words {
        write!(&mut by_n_file, "{} : {}\n", word, n).unwrap();
    }

    let mut by_a_file = File::create(&config.out_by_a).unwrap();
    counted_words.sort_by(|a, b| a.0.cmp(&b.0));
    for (word, n) in &counted_words {
        write!(&mut by_a_file, "{} : {}\n", word, n).unwrap();
    }
}
